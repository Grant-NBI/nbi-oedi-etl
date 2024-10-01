"""
ETL Script Workflow (Optimized)

This ETL script extracts, transforms, and loads energy data from an S3 data lake to a target S3 bucket using async I/O and multiprocessing.

Subsystems:
1. **Fetcher/Extractor**:
   - Lists files from S3 partitions and enqueues file keys to the async fetch queue.
   - Fetches file content from S3 and places it in the async process queue.
   - Injects a poison pill into the queue once fetching completes.
   - Operations yield periodically to ensure other tasks proceed concurrently.

2. **Transformer**:
   - Transfers file content from the async process queue to the multiprocessing input queue.
   - Worker processes apply transformations (e.g., 15-minute data aggregation) and push results to the multiprocessing result queue.
   - Injects poison pills into result queues once transformation is complete.
   - Utilizes ProcessPoolExecutor for parallel CPU-bound processing.

3. **Uploader/Loader**:
   - Transfers transformed data from the multiprocessing result queue to the async results queue.
   - Asynchronously uploads processed data to S3.
   - Terminates on poison pill reception, completing the pipeline.

4. **Monitor**:
    - Periodically checks for idle periods across all queues.
    - Cancels tasks and shuts down the process executor if the idle timeout is reached.

Pointers:
1. **Async S3 Operations**: Listing, fetching, and uploading are done asynchronously to avoid blocking and optimize I/O performance.
2. **Queue-Based Parallelism**: Decouples I/O-bound tasks (async) from CPU-bound tasks (multiprocessing), leveraging efficient parallelism.
3. **Multiprocessing for CPU Tasks**: Data transformations occur in parallel across CPU cores, using ProcessPoolExecutor for scalable performance. #TODO! No serious investigation is done to justify this and if the transformation evolves to be extensive and complex, the assumption that the I/O is the driver for cost + performance may not hold. The cost of such optimization/development should be weighed against the cost of operation and the expected performance gain => only needs consideration when operation cost starts biting. Another consideration is to look into Spark for such operations
4. **Error Handling**: Exception handling is built into each subsystem to log and manage errors during processing.
5. **Partition Handling**: Data is processed concurrently based on configurable partitions (state, upgrades) for better performance across large datasets.
"""

import asyncio
import datetime
import os
import time
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager

from oedi_etl.fetch import fetch_files
from oedi_etl.log import get_logger
from oedi_etl.monitor import monitor_idle_tasks, ETLMonitor
from oedi_etl.transform import worker_process
from oedi_etl.upload import load_files

logger = get_logger()


async def etl_process(etl_name, config):
    """
    Main ETL orchestrator using async I/O for file fetching, transformations, and uploading.
    Utilizes multiprocessing for CPU-bound transformations and async for I/O-bound operations.
    Includes job monitoring to handle idle timeouts and ensure graceful shutdown as well as track job progress
    """
    # Init
    logger.info(f"1000: ETL Started for {etl_name} at {datetime.datetime.now()}")
    num_cpus = os.cpu_count()
    idle_timeout_in_minutes = config["idle_timeout_in_minutes"]

    # Async queues
    objects_to_fetch_async_queue = asyncio.Queue()
    files_to_process_async_queue = asyncio.Queue()
    results_async_queue = asyncio.Queue()

    # Multiprocessing queues and Manager
    with Manager() as manager:
        # Shared state to track file counts across all tasks
        state = manager.dict(
            {
                "bypassed":0,
                "listed": 0,
                "fetched": 0,
                "transferred_to_worker": 0,
                "transformed": 0,
                "transferred_to_uploader": 0,
                "uploaded": 0,
            }
        )

        # Initialize the monitor
        monitor = ETLMonitor(etl_name, time.time(), state)

        process_input_queue = manager.Queue()
        process_result_queue = manager.Queue()

        # Create fetch task
        fetch_task = asyncio.create_task(
            fetch_files(
                config,
                objects_to_fetch_async_queue,
                files_to_process_async_queue,
                process_input_queue,
                num_cpus,
                monitor,  # Pass monitor to update counts
            )
        )

        # Create load task
        load_task = asyncio.create_task(
            load_files(
                config,
                process_result_queue,
                results_async_queue,
                monitor,  # Pass monitor to update counts
            )
        )

        # Create worker processes
        loop = asyncio.get_event_loop()
        process_executor = ProcessPoolExecutor(max_workers=num_cpus)
        worker_futures = []
        for _ in range(num_cpus):
            future = loop.run_in_executor(
                process_executor,
                worker_process,
                process_input_queue,
                process_result_queue,
                state,  # pass shared state, not monitor (not picklable)
            )
            worker_futures.append(future)

        # Create monitoring task
        monitor_idle_task = asyncio.create_task(
            monitor_idle_tasks(
                [
                    objects_to_fetch_async_queue,
                    files_to_process_async_queue,
                    process_input_queue,
                    process_result_queue,
                ],
                idle_timeout_in_minutes,
                [fetch_task, load_task],
                process_executor,
            )
        )

        # Wait for all tasks to complete or be cancelled
        try:
            await asyncio.gather(fetch_task, load_task, monitor_idle_task)
        except asyncio.CancelledError:
            logger.info("1090: Tasks were cancelled due to idle timeout.")
        except Exception as e:
            logger.exception("1091:An exception occurred: %s", e)


        # Print monitor summary before exiting the context and shutdown the process executor
        monitor.print_summary()
        process_executor.shutdown(wait=True)

    # End time after completion or timeout
    logger.info(f"1099: ETL completed for {etl_name}")
