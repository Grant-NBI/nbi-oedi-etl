# pylint: disable=import-error
# pyright: reportMissingImports=false
import asyncio
import base64
import importlib
import json
import os
import sys
from datetime import datetime


import boto3
from awsglue.utils import getResolvedOptions  # available in Glue pythonshell

s3 = boto3.client("s3")
glue_client = boto3.client("glue")


def load_arguments():
    """
    Loads default arguments and decodes the Base64-encoded configuration passed to the job.
    Also fetches 'etl_config' overrides passed via Glue Workflow RunProperties and decodes the configuration.
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
            "WORKFLOW_NAME",
            "WORKFLOW_RUN_ID",
        ],
    )

    # Fetch workflow run props and use override if available, otherwise use the default available during the last deployment
    workflow_params = glue_client.get_workflow_run_properties(
        Name=args["WORKFLOW_NAME"], RunId=args["WORKFLOW_RUN_ID"]
    )["RunProperties"]

    # debug info
    print(f"[INFO] (1000) Default Arguments: {json.dumps(args, indent=2)}")
    print(f"[INFO] (1000) Workflow Parameters: {json.dumps(workflow_params, indent=2)}")

    base64_config = workflow_params.get("--etl_config_override", args["etl_config"])

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
    os.environ["LOG_FILE_PREFIX"] = f"{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"

    # Dynamically import etl_main after setting environment variables
    etl_main = importlib.import_module("oedi_etl.main").etl_main

    # Run the etl job
    asyncio.run(etl_main(_config))
