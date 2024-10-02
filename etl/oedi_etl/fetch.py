"""
Fetcher Pipeline: Fetches files from the configured S3 partitions and enqueue them to the process input queue for transformer workers pool distributed one per cpu.

NOTE: This job runs on a 1DPU max (for Python), and the number of files that can reasonably be fetched per second is constrained by network bandwidth (low Gbps range + concurrent connection limits) and the transformer capacity (4 vCPUs and 16GB memory). At best, you can expect to handle files in the 100s per sec. Listing files at a faster rate is not useful and may destabilize downstream tasks => good practice to pace. The page size is configurable, and since the lister pauses for a second after each page, this provides a good pacing mechanism.
"""

import asyncio
import json

import boto3
from oedi_etl.log import get_logger

logger = get_logger()


s3 = boto3.client("s3")


def get_upgrade_str(upgrade):
    """
    Returns a string representation of the upgrade level.

    Parameters:
    upgrade (int): The upgrade level. If 0, returns "baseline". Otherwise, returns "upgrade" followed by the upgrade level formatted as a two-digit number. Using formatters maybe problematic so using logic instead.

    Returns:
    str: A string representing the upgrade level.
    """
    _upgrade = int(upgrade)
    if _upgrade == 0:
        return "baseline"
    elif 1 <= _upgrade <= 9:
        return f"upgrade0{upgrade}"
    elif _upgrade != 0:
        return f"upgrade{upgrade}"
    else:
        raise ValueError(f"Invalid upgrade value: {upgrade}")


async def list_files_in_s3(objects_to_fetch_async_queue, config, monitor):
    """
    Lists S3 files in pages with adjustable page size and introduces pacing.
    After each page (500 files by default), the function sleeps for 1 second to allow other processes to catch up.
    Back pressure control ensures that the queue size is monitored and paused if the queue becomes too full.
    """
    src_bucket = config["src_bucket"]
    base_partition = config["base_partition"]
    release_name = config["release_name"]
    release_year = config["release_year"]
    state = config["state"]
    upgrades = config["upgrades"]
    data_partition_in_release = config["data_partition_in_release"]
    metadata_location = config["metadata_location"]
    metadata_base_partition = f"{metadata_location}/state={state}/parquet"

    # Configurable page size
    page_size = int(config.get("listing_page_size", 500))
    max_queue_size = int(config.get("max_queue_size", 1000))

    # Partitions for a state and upgrade combo + their corresponding metadata
    partitions = []
    for upgrade in upgrades:
        upgrade_str = get_upgrade_str(upgrade)

        # The data partition path (directory prefix)
        partition = f"{base_partition}/{release_year}/{release_name}/{data_partition_in_release}/upgrade={upgrade}/state={state}"
        partitions.append(partition)

        # Metadata filenames
        basic_metadata_filename = (
            f"{state}_{upgrade_str}_basic_metadata_and_annual_results.parquet"
        )
        metadata_filename = f"{state}_{upgrade_str}_metadata_and_annual_results.parquet"

        # Full metadata paths (file keys)
        basic_metadata_partition = f"{metadata_base_partition}/{basic_metadata_filename}"
        metadata_partition = f"{metadata_base_partition}/{metadata_filename}"

        # Append the metadata files (full file keys)
        partitions.append(basic_metadata_partition)
        partitions.append(metadata_partition)

        logger.info(f"1103: Listing files for partitions: {json.dumps(partition)}")

    async def list_files_for_partition(prefix):
        paginator = s3.get_paginator("list_objects_v2")

        # Create an iterator
        page_iterator = paginator.paginate(
            Bucket=src_bucket,
            Prefix=prefix,
            MaxKeys=page_size,  # Page size based on config
        )

        file_count = 0  # Track

        # Loop through pages
        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    file_key = obj["Key"]
                    file_size = obj["Size"]
                    logger.debug(
                        f"1111: Found file {file_key} with size {file_size} bytes"
                    )

                    # Back pressure control
                    if objects_to_fetch_async_queue.qsize() >= max_queue_size:
                        logger.info(
                            f"1112: Queue size ({objects_to_fetch_async_queue.qsize()}) exceeds limit ({max_queue_size}). Pausing..."
                        )
                        await asyncio.sleep(2)  # Sleep to allow catchup

                    # Enqueue
                    await objects_to_fetch_async_queue.put(file_key)
                    file_count += 1
                    monitor.record_listed(file_key)
                    # Yield control every 10 files
                    if file_count % 10 == 0:
                        await asyncio.sleep(0)

            # After each page, yield control and sleep for pacing
            logger.info(
                f"1113: Processed page for prefix {prefix}, sleeping for 1 second..."
            )
            await asyncio.sleep(1)  # Sleep for pacing

    # Loop through the partitions incrementally
    try:
        logger.info("1101: Starting file listing...")

        while partitions:
            current_partition = partitions.pop(0)
            logger.debug(f"1105: Processing partition: {current_partition}")
            # Back pressure control
            if objects_to_fetch_async_queue.qsize() >= max_queue_size:
                logger.info(
                    f"1112: Queue size ({objects_to_fetch_async_queue.qsize()}) exceeds limit ({max_queue_size}). Pausing..."
                )
                await asyncio.sleep(2)  # Sleep to allow catchup

            # Check if current_partition is a metadata file (ends with '.parquet')
            if current_partition.endswith('.parquet'):
                # directly copy metadata file from src to dest bucket without processing
                await bypass_etl(current_partition, config, monitor)
                await asyncio.sleep(0)  # Yield control
            else:
                # It's a data partition (prefix), list files under it
                await list_files_for_partition(current_partition)
                await asyncio.sleep(0)  # Yield control

        # Inject poison pill to signal completion
        await objects_to_fetch_async_queue.put(None)
        logger.info("1114: File listing completed and injected the poison pill")

    except Exception as e:
        logger.error(f"1115: Error listing files: {e}")
        await objects_to_fetch_async_queue.put(None)


async def bypass_etl(partition_key, config, monitor):
    """
    Bypasses the ETL process by copying the file directly from the source bucket to the destination bucket.

    Args:
        partition_key (str): The partition key (path) of the file to be copied. This includes the s3 bucket as well.
        config (dict): Configuration dictionary containing source and destination bucket info.
        monitor (Monitor): An optional monitoring object to track the bypass.

    """
    src_bucket = config["src_bucket"]
    dest_bucket = config["dest_bucket"]
    out_dir = config["output_dir"]
    #partition_key is in the format of bucket_name/file_key
    file_key = "/".join(partition_key.split("/")[1:])
    dest_key =  f"{out_dir}/{file_key}"
    dest = f"{dest_bucket}/{dest_key}"

    try:
        # Perform S3 copy operation
        logger.info(f"1106: Copying file from {partition_key} to {dest}")

        copy_source = {'Bucket': src_bucket, 'Key': file_key}
        s3.copy(copy_source, dest_bucket, dest_key)

        if monitor:
            monitor.record_bypassed(partition_key)

        logger.info(f"1107: Successfully bypassed ETL and copied {partition_key} to {dest}")

    except s3.exceptions.NoSuchKey:
        logger.error(f"1108:File {partition_key} not found in {src_bucket}")

    except Exception as e:
        logger.error(f"1109:Error during bypass ETL for file {partition_key}: {e}")
        raise


def fetch_file(config, file_key):
    """
    Fetches a file from an S3 bucket.

    Args:
        config (dict): Configuration dictionary containing the source bucket name.
        file_key (str): The key (path) of the file to fetch from the S3 bucket.

    Returns:
        file_bytes: The content of the fetched file as bytes.
    """
    response = s3.get_object(Bucket=config["src_bucket"], Key=file_key)
    file_bytes = response["Body"].read()
    # * can implement IO stream but no benefit for aggregation only use case
    return file_bytes


async def fetch_files_and_enqueue(
    objects_to_fetch_async_queue, files_to_process_async_queue, config, monitor
):
    """
    Fetches files from S3 using the keys in objects_to_fetch_async_queue and adds them to files_to_process_async_queue.
    """
    loop = asyncio.get_event_loop()
    while True:
        try:
            logger.info("1120: Fetching files...")
            file_key = await objects_to_fetch_async_queue.get()

            if file_key is None:  # Poison pill received
                logger.info("1121: Received poison pill in fetcher. Stopping fetching.")
                await files_to_process_async_queue.put(None)
                break

            logger.debug(f"1122: Fetching file: {file_key}")
            # non blocking
            file_bytes = await loop.run_in_executor(None, fetch_file, config, file_key)
            logger.debug(f"1123: Fetched file: {file_key}")
            monitor.record_fetched()

            # enqueue
            await files_to_process_async_queue.put((file_key, file_bytes))
            logger.info(
                f"1124: Enqueued file for processing: {file_key} (Queue size after enqueuing: {files_to_process_async_queue.qsize()})"
            )

            # Yield
            await asyncio.sleep(0)

        except Exception as e:
            logger.error(f"1125: Error fetching or enqueuing file: {e}")


async def transfer_input_to_process_queue(
    files_to_process_async_queue, process_input_queue, num_cpus, monitor
):
    """
    Transfers files from the async files_to_process_async_queue to the multiprocessing process_input_queue.
    This task is responsible for reading file data from the async queue and feeding it into the
    process input queue which will be consumed by worker processes.
    """
    while True:
        try:
            logger.info(
                "1126: Waiting to transfer file from files_to_process_async_queue..."
            )

            # Get file data from the asyncio queue (files_to_process_async_queue)
            file_data = await files_to_process_async_queue.get()

            if file_data is None:  # poison pill
                logger.info(
                    "1127: Received poison pill in files_to_process_async_queue. Stopping transfer."
                )
                # Send poison pills to all workers
                for _ in range(num_cpus):
                    process_input_queue.put(None)
                    logger.info("1128: Sent poison pill to worker queue.")
                break

            # Put the file data into the multiprocessing queue (process_input_queue)
            logger.debug("1129: Transferring file to process_input_queue...")
            process_input_queue.put(file_data)
            logger.info("1130: Transferred file to process_input_queue successfully.")
            monitor.record_transferred_to_worker()

        except Exception as e:
            logger.error(
                f"1131: Error transferring file from async queue to process queue: {e}"
            )


### Fetcher pipeline ###
async def fetch_files(
    config,
    objects_to_fetch_async_queue,
    files_to_process_async_queue,
    process_input_queue,
    num_cpus,
    monitor,
):
    """
    Orchestrates the entire pipeline of listing, fetching, enqueuing, and transferring files.
    This will run the listing, fetching, and transferring to the process queue concurrently.
    """

    logger.info("1100: Fetch and transfer pipeline started...")

    # List
    listing_task = asyncio.create_task(
        list_files_in_s3(objects_to_fetch_async_queue, config, monitor)
    )

    # Fetch
    fetching_task = asyncio.create_task(
        fetch_files_and_enqueue(
            objects_to_fetch_async_queue, files_to_process_async_queue, config, monitor
        )
    )

    # Transfer/bridge
    transfer_task = asyncio.create_task(
        transfer_input_to_process_queue(
            files_to_process_async_queue, process_input_queue, num_cpus, monitor
        )
    )

    # Gather
    await asyncio.gather(listing_task, fetching_task, transfer_task)

    logger.info("1199: Fetch and transfer pipeline completed.")
