"""
Loader Pipeline: Orchestrates the transfer of processed results from the multiprocessing queue to the asyncio queue and uploads the results to the S3 destination bucket.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor

import boto3
from oedi_etl.log import get_logger

logger = get_logger()

s3 = boto3.client("s3")


def upload_to_s3(dest_bucket, file_key, transformed_data):
    """
    Helper function to upload a file to S3. This runs in the executor.
    """
    try:
        logger.debug(f"1354: Uploading file: {file_key} ...")
        s3.put_object(Bucket=dest_bucket, Key=file_key, Body=transformed_data)
        logger.info(f"1355: Uploaded file: {file_key}")
    except Exception as e:
        logger.error(f"1356: Error uploading file {file_key}: {e}")


async def upload_files(results_queue, dest_bucket, out_dir, monitor):
    """
    Async function that uploads processed files from the results_queue to the S3 destination bucket.
    The S3 operations are executed in an executor to prevent blocking the event loop.
    """
    logger.info("1350: Uploading files started...")

    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=5) as executor:
        while True:
            try:
                logger.info("1351: Waiting for item in results_queue...")
                result = await results_queue.get()

                if result is None:  # Poison pill received
                    break

                file_key, transformed_data = result
                logger.debug(
                    f"1352: Received fileKey: {file_key}, filesize: {len(transformed_data)} bytes"
                )

                # Run the blocking S3 upload in a threadpool executor
                await loop.run_in_executor(
                    executor,
                    upload_to_s3,
                    dest_bucket,
                    f"{out_dir}/{file_key}",
                    transformed_data,
                )
                logger.info(f"1357: Uploaded file: {file_key}")
                monitor.record_uploaded(file_key)

            except Exception as e:
                logger.error(f"1358: Error uploading file: {e}")


async def transfer_results_to_async_queue(process_result_queue, results_async_queue, monitor):
    """
    Continuously transfers results from the multiprocessing result queue to the result asyncio queue.
    """
    loop = asyncio.get_event_loop()
    logger.info("1341: Transfer task started...")

    while True:
        try:
            result = await loop.run_in_executor(None, process_result_queue.get)
            logger.debug("1342: fetched result")
            if result is None:  # Poison pill
                await results_async_queue.put(None)
                break

            # Transfer
            await results_async_queue.put(result)
            logger.info("1343: Transferred result to async queue")
            monitor.record_transferred_to_uploader()

        except Exception as e:
            logger.error(f"1344: Error transferring result: {e}")


async def load_files(config, process_result_queue, results_async_queue, monitor):
    """
    Orchestrates the transfer of processed results from the multiprocessing queue to the asyncio queue
    and uploads the results to the S3 destination bucket. This runs the result transfer and upload concurrently.
    """
    logger.info("1310: Load operation started...")
    dest_bucket = config["dest_bucket"]
    out_dir = config["output_dir"]
    # Transfer from the process_result_queue to results_async_queue
    transfer_task = asyncio.create_task(
        transfer_results_to_async_queue(
            process_result_queue, results_async_queue, monitor
        )
    )

    # Task for uploading files to S3 from the results_async_queue
    upload_task = asyncio.create_task(
        upload_files(results_async_queue, dest_bucket, out_dir, monitor)
    )

    # Wait for both tasks to complete
    await asyncio.gather(transfer_task, upload_task)

    logger.info("1399: Load operation completed.")
