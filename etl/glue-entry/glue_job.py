# pylint: disable=import-error
# pyright: reportMissingImports=false
import sys
import json
import os
import base64
import asyncio
import importlib
from awsglue.utils import getResolvedOptions  # available in Glue pythonshell

import boto3

s3 = boto3.client("s3")


def load_arguments():
    """
    Decodes the Base64-encoded configuration passed to the job.
    """
    # Get the base64 encoded config and additional args from the arguments
    args = getResolvedOptions(
        sys.argv,
        [
            "etl_config",
            "ETL_EXECUTION_ENV",
            "OUTPUT_BUCKET_NAME",
            "DATA_CRAWLER_NAME",
            "METADATA_CRAWLER_NAME",
        ],
    )
    base64_config = args["etl_config"]

    # Decode the base64 encoded config
    decoded_config = base64.b64decode(base64_config).decode("utf-8")
    config = json.loads(decoded_config)

    return (
        config,
        args["ETL_EXECUTION_ENV"],
        args["OUTPUT_BUCKET_NAME"],
        args["DATA_CRAWLER_NAME"],
        args["METADATA_CRAWLER_NAME"],
    )


if __name__ == "__main__":
    (
        _config,
        job_execution_environment,
        output_bucket_name,
        data_crawler_name,
        metadata_crawler_name,
    ) = load_arguments()
    settings = _config["settings"]

    # log settings
    log_dir = settings["log_dir"]
    log_filename = settings["log_filename"]

    ## Set the environment variables
    os.environ["LOGGING_LEVEL"] = settings["logging_level"]
    os.environ["LOG_DIR"] = log_dir
    os.environ["LOG_FILENAME"] = log_filename
    os.environ["ETL_EXECUTION_ENV"] = job_execution_environment
    os.environ["OUTPUT_BUCKET_NAME"] = output_bucket_name
    os.environ["DATA_CRAWLER_NAME"] = data_crawler_name
    os.environ["METADATA_CRAWLER_NAME"] = metadata_crawler_name

    # Dynamically import etl_main after setting environment variables
    etl_main = importlib.import_module("oedi_etl.main").etl_main

    # Run the etl job
    asyncio.run(etl_main(_config))

    # export logs to S3
    log_files = [
        os.path.join("/tmp/", log_dir, f)
        for f in os.listdir(os.path.join("/tmp/", log_dir))
        if log_filename in f
    ]

    for log_file in log_files:
        s3.upload_file(
            log_file, output_bucket_name, f"logs/{os.path.basename(log_file)}"
        )
