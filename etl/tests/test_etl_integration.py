"""
    Test the ETL process in a local environment with real data.

    This test runs the full ETL process using real data from an AWS Data Lake and upload
    the result to a real S3 bucket. No mocking or unit testing is used to simulate interactions.
    The script relies on actual AWS credentials, and the output is verified by checking the S3 bucket
    for the uploaded files.

    Requirements:
    - A .env file containing AWS_PROFILE, TEST_BUCKET_NAME, and AWS_REGION.
    - Real-time S3 access for data fetching and output.

    Steps:
    1. Load environment variables from .env file.
    2. Set AWS_PROFILE, TEST_BUCKET_NAME, and AWS_REGION.
    3. Run the ETL process and verify successful file uploads in the test bucket.
    """

import asyncio
import importlib.util
import json
import os
import sys

import boto3


current_dir = os.path.dirname(os.path.abspath(__file__))


def load_etl_config():
    """
    Load the ETL configuration from a JSON file.
    """
    # * This is outside of the pyproject. In glue, this is passed as part of a deployment or base64 encoded etl_config per job argument
    config_path = os.path.join(current_dir, "../../config.json")
    with open(config_path, "r", encoding="utf-8") as file:
        config = json.load(file)
    return config["etl_config"]


def load_test_env_config():
    """
    Load the test configuration.
    """
    config_path = os.path.join(current_dir, ".env.json")
    with open(config_path, "r", encoding="utf-8") as config_file:
        return json.load(config_file)


def load_oedi_etl():
    """
    Loads the OEDI ETL module dynamically.

    This function determines the directory of the current script, constructs the path to the
    'oedi_etl' directory, and dynamically imports the 'main.py' file from that directory.
    If the 'oedi_etl' directory is not already in the system path, it is added.

    Returns:
        module: The dynamically loaded OEDI ETL module.
    """

    etl_dir = os.path.normpath(os.path.join(current_dir, "../", "oedi_etl"))
    main_file = os.path.join(etl_dir, "main.py")

    if etl_dir not in sys.path:
        sys.path.append(etl_dir)

    spec = importlib.util.spec_from_file_location("main", main_file)
    oedi_etl_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(oedi_etl_module)

    return oedi_etl_module


async def test_etl_process():
    """
    Test the ETL process in a local environment using real data from an AWS Data Lake and uploads to a real S3 bucket.
    No mocking or unit testing is used, this test simulates a real environment.
    """

    # test config
    test_env_config = load_test_env_config()

    # etl_config
    etl_config = load_etl_config()

    # set env
    settings = etl_config.get("settings", {})
    os.environ["LOGGING_LEVEL"] = settings.get("logging_level", "INFO")
    os.environ["LOG_DIR"] = settings.get("log_dir", "logs")
    os.environ["LOG_FILENAME"] = settings.get("log_filename", "etl.log")
    os.environ["AWS_PROFILE"] = test_env_config.get("AWS_PROFILE", "default_profile")
    os.environ["AWS_REGION"] = test_env_config.get("AWS_REGION", "us-west-2")

    # check
    session = boto3.Session()
    print("Profile:", session.profile_name)
    print("Region:", session.region_name)
    print("ACCESS_KEY_ID:", session.get_credentials().access_key)
    print("LOGGING_LEVEL:", os.getenv("LOGGING_LEVEL"))
    print("LOG_DIR:", os.getenv("LOG_DIR"))
    print("LOG_FILENAME:", os.getenv("LOG_FILENAME"))

    #*This is supplied as argument in glue job
    os.environ["DATA_CRAWLER_NAME"]= "nbi_building_analytics_dev_oedi_etl_crawler"
    # For isolated testing purposes, we must identify the bucket name (this has to be manually /cli provisioned). In the cdk app, a bucket is provisioned and the bucket name is passed to the glue job as environment variable (OUTPUT_BUCKET_NAME
    os.environ["TEST_BUCKET_NAME"] = test_env_config.get("TEST_BUCKET_NAME")

    # note that oedi_etl must be imported after setting the AWS_PROFILE
    # test ETL
    await load_oedi_etl().etl_main(etl_config)

    # Verify files were uploaded correctly
    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION"))
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=test_env_config.get("TEST_BUCKET_NAME"), Prefix="etl_output/"
    )

    # Get the first 10 uploaded files
    first_ten_uploaded_files = []
    for page in page_iterator:
        contents = page.get("Contents", [])
        first_ten_uploaded_files.extend(contents)
        if len(first_ten_uploaded_files) >= 10:
            break

    # Check if any (files processed should be more than 0)
    assert len(first_ten_uploaded_files) > 0, "ETL did not produce any output files!"

    print(
        "ETL process completed successfully. First 10 files uploaded to S3 for visual sampling:\n"
    )

    for i, file in enumerate(first_ten_uploaded_files, start=1):
        print(f"{i}. {file}")


if __name__ == "__main__":
    asyncio.run(test_etl_process())
