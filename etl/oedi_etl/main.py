"""
ETL Job Handler

This script coordinates multiple ETL jobs by loading configuration from a JSON file and running each ETL process in sequence. Each job defines the release year, release name, state, and upgrades to filter and process the corresponding data from an S3 bucket.

It:
1. Loads configuration from a JSON file.
2. Iterates over multiple ETL jobs.
3. Runs each ETL job independently with the required parameters.
4. Update the s3 target for Glue Crawlers with the the latest partitions to crawl
"""

import asyncio
import json
import os
from datetime import datetime

import boto3
from oedi_etl.etl_job import etl_process
from oedi_etl.log import get_logger

logger = get_logger()
glue = boto3.client("glue")


async def etl_main(etl_config):
    """
    Main function to execute the ETL process for multiple jobs defined in a configuration file.

    This function performs the following steps:
    1. Sets the common S3 bucket name for all jobs.
    2. Iterates over each job configuration and extracts relevant parameters.
    3. Executes the ETL process asynchronously for each job using the extracted parameters.
    4. Collects and stores partitions in SSM for the Glue Crawler to discover.

    Parameters:
        None

    Returns:
        None
    """

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
        f"{etl_config['output_dir']}/{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
    )
    job_specific = etl_config["job_specific"]

    # async tasks
    tasks = []
    data_partitions_to_crawl = []
    metadata_partitions_to_crawl = []

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

        # Construct partition paths
        for upgrade in config["upgrades"]:
            partition = (
                f"{base_partition}/{config['release_year']}/{config['release_name']}/"
                f"{data_partition_in_release}/upgrade={upgrade}/state={config['state']}"
            )
            data_partitions_to_crawl.append(partition)

        # Add metadata location as well (remove src_bucket and construct for the state partition). Partition points to the state then ignore csv and target the parquet. If you point it to parquet, it will output a name that ends with parquet and won't be unique per state and all crawl jobs across states will be pointing to the same location (which probably is fine as they most likely have identical schema but that is too much of an assumption)
        _metadata_location = f"{'/'.join(config['metadata_location'].split('/')[1:])}/state={config['state']}"
        metadata_partitions_to_crawl.append(_metadata_location)

    # Execute ETL tasks
    await asyncio.gather(*tasks)

    # output prefix
    output_prefix = f"s3://{dest_bucket}/{output_dir}"
    # TODO!: you can have advanced glob patterns to exclude all states and identify the state to crawl, but not worth testing now -> just set the root at the state level => creates table per partitions => ideally you figure this out and create one table for all and provision this via with CDK with a placeholder and update it per each run (not the most complex thing to do but requires testing and time

    # Pass collected partitions to the Data Crawler
    if data_partitions_to_crawl:
        glue.update_crawler(
            Name=os.getenv("DATA_CRAWLER_NAME"),
            Targets={
                "S3Targets": [
                    {"Path": f"{output_prefix}/{partition}"}
                    for partition in data_partitions_to_crawl
                ]
            },
            Configuration=json.dumps(
                {
                    "Version": 1.0,
                    "Grouping": {"TableLevelConfiguration": 11},
                }
            ),
        )

    # Pass the metadata partitions to the Metadata Crawler
    if metadata_partitions_to_crawl:
        glue.update_crawler(
            Name=os.getenv("METADATA_CRAWLER_NAME"),
            Targets={
                "S3Targets": [
                    {"Path": f"{output_prefix}/{partition}", "Exclusions": ["*/csv/*"]}
                    for partition in metadata_partitions_to_crawl
                ]
            },
            Configuration=json.dumps(
                {
                    "Version": 1.0,
                    "Grouping": {"TableLevelConfiguration": 11},
                }
            ),
        )
