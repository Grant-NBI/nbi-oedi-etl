"""
ETL Job Handler

This script coordinates multiple ETL jobs by loading configuration from a JSON file and running each ETL process in sequence. Each job defines the release year, release name, state, and upgrades to filter and process the corresponding data from an S3 bucket.

It:
- Loads configuration from a JSON file.
- Iterates over multiple ETL jobs.
- Runs each ETL job independently with the required parameters.
"""

import asyncio
import json
import os
from datetime import datetime

from etl_job import etl_process
from log import get_logger

logger = get_logger()



def load_config(config_file):
    """
    Load the ETL configuration from a JSON file.
    """
    with open(config_file, "r", encoding="utf-8") as file:
        config = json.load(file)
    return config["etl_config"]


async def main():
    """
    Main function to execute the ETL process for multiple jobs defined in a configuration file.

    This function performs the following steps:
    1. Loads the ETL configuration from a JSON file.
    2. Sets the common S3 bucket name for all jobs.
    3. Iterates over each job configuration and extracts relevant parameters.
    4. Executes the ETL process asynchronously for each job using the extracted parameters.

    Parameters:
        None

    Returns:
        None
    """

    # config
    config_path = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "config.json")
    )
    etl_config = load_config(config_path)

    # shared config
    # DL config
    base_partition = etl_config["base_partition"]
    data_partition_in_release = etl_config["data_partition_in_release"]

    # output Config
    src_bucket = etl_config["src_bucket"]
    dest_bucket = os.getenv("TEST_BUCKET_NAME") or os.getenv("OUTPUT_BUCKET_NAME")

    # etl settings
    settings = etl_config["settings"]
    idle_timeout_in_minutes = settings.get("idle_timeout_in_minutes", 5)
    listing_page_size = settings.get("listing_page_size", 500)
    max_listing_queue_size = settings.get("max_listing_queue_size", 1000)

    # per glue job
    output_dir = (
        f"{etl_config["output_dir"]}/{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
    )
    job_specific = etl_config["job_specific"]

    # async tasks
    tasks = []

    # Iterate over the jobs defined in job_specific
    for job_config in job_specific:
        # config for each job
        config = {
            "src_bucket": src_bucket,
            "dest_bucket": dest_bucket,
            "output_dir": output_dir,
            "release_name": job_config["release_name"],
            "release_year": job_config["release_year"],
            "state": job_config["state"],
            "upgrades": job_config["upgrades"],
            "metadata_location": job_config["metadata_location"],
            "base_partition": base_partition,
            "data_partition_in_release": data_partition_in_release,
            # "toggle_date_partition": job_config.get("toggle_date_partition", False),
            "idle_timeout_in_minutes": idle_timeout_in_minutes,
            "listing_page_size": listing_page_size,
            "max_listing_queue_size": max_listing_queue_size,
        }
        etl_name = (
            f"{config['release_name']}_{config['release_year']}_{config['state']}"
        )

        logger.info(
            f"\n1000: Running ETL with the following config: {json.dumps(config, indent=2)}"
        )
        tasks.append(etl_process(etl_name, config))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
