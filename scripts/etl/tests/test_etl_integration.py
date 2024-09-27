"""
    Test the ETL process in a local environment with real data.

    This test runs the full ETL process using real data from an AWS Data Lake and uploadsc
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


def load_oedi_etl():
    """
    Loads the OEDI ETL module dynamically.

    This function determines the directory of the current script, constructs the path to the
    'oedi-etl' directory, and dynamically imports the 'main.py' file from that directory.
    If the 'oedi-etl' directory is not already in the system path, it is added.

    Returns:
        module: The dynamically loaded OEDI ETL module.
    """

    etl_dir = os.path.normpath(os.path.join(current_dir, "../", "oedi-etl"))
    main_file = os.path.join(etl_dir, "main.py")

    if etl_dir not in sys.path:
        sys.path.append(etl_dir)

    spec = importlib.util.spec_from_file_location("main", main_file)
    oedi_etl_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(oedi_etl_module)

    return oedi_etl_module


def load_test_config():
    """
    Load the test configuration.
    """
    config_path = os.path.join(current_dir, ".env.json")
    with open(config_path, "r", encoding="utf-8") as config_file:
        return json.load(config_file)


async def test_etl_process():
    """
    Test the ETL process in a local environment using real data from an AWS Data Lake and uploads to a real S3 bucket.
    No mocking or unit testing is used, this test simulates a real environment.
    """

    # test config
    config = load_test_config()

    # AWS ENV
    os.environ["AWS_PROFILE"] = config.get("AWS_PROFILE", "default_profile")
    os.environ["AWS_REGION"] = config.get("AWS_REGION", "us-west-2")

    # check
    session = boto3.Session()
    print("Profile:", session.profile_name)
    print("Region:", session.region_name)
    print("Credentials:", session.get_credentials())

    # For isolated testing purposes, we must identify the bucket name (this has to be manually /cli provisioned). In the cdk app, a bucket is provisioned and the bucket name is passed to the glue job as environment variable (OUTPUT_BUCKET_NAME
    os.environ["TEST_BUCKET_NAME"] = config.get("TEST_BUCKET_NAME")

    # test ETL
    etl = load_oedi_etl()
    await etl.main()

    # Verify files were uploaded correctly
    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION"))
    output_files = s3.list_objects_v2(
        Bucket=config.get("TEST_BUCKET_NAME"), Prefix="etl_output/"
    )

    # Check if any
    assert "Contents" in output_files, "No output files found!"
    assert len(output_files["Contents"]) > 0, "ETL did not produce any output files!"

    # List the first 25 uploaded files (sample check only -> do manual check)
    uploaded_files = [obj["Key"] for obj in output_files.get("Contents", [])][:25]
    print(
        f"ETL process completed successfully. First {len(uploaded_files)} files:",
        uploaded_files,
    )


if __name__ == "__main__":
    asyncio.run(test_etl_process())
