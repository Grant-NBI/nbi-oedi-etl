# NBI Open Energy Data Initiative (OEDI) - ETL and Querying infrastructure

## Quick Guide

The deployment process is automated using AWS CDK.

1. [Edit config](/config.json). Ensure the account, region, profile, and the ETL input parameters are correctly set.
2. [Edit SQL file](/lib/saved-queries.sql)
3. [Edit ETL file](/scripts/etl/oedi-etl/main.py)
4. Run `npm run deploy` from the project root to deploy the stack using the `config.json`.

## config.json Configuration Example

* Functions as the configurator for both the oedi-etl and the cdk/deployment config. This is a bit unconventional => done for simplicity. There is a script that copies this file to the  `oedi-etl` dir during the deployment and will be part of the packaged asset. For manual copy, use `npm run copy-config`.
* *Note that this file is ignored by git ignore*
* Copy the following content to `config.json` at the root of the project - sibling to this file - and update the values as needed.

```json
{
  "monorepoRoot": ".",
  //deploys the stack in the AWS environment
  "deploymentConfig": [
    {
      "appName": "NbiBuildingAnalytics",
      "account": "xxx",
      "deploymentEnv": "dev",
      "profile": "profile_name",
      "regions": [
        "us-west-2" //you can deploy to multiple regions
      ],
      "requireApproval": "never", //never (never ask) | broadening (default and only required when security privilege is expanded) | anyChange (ask for all changes)
      "glueJobTimeoutMinutes": 240, //optional -default is 240 minutes or 4 hrs. This is the maximum time the Glue job can run. The Glue job is terminated after this time. This is a fallback.
    }
  ],
  "etl_config": {
    //output config
    "output_dir": "etl_output", //this is where the output of the ETL is stored within the S3 bucket.
    "src_bucket": "oedi-data-lake",

    //DL Config
    "data_partition_in_release": "timeseries_individual_buildings/by_state",
    "base_partition": "oedi-data-lake/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock",

    //per each glue job
    "job_specific": [
      {
        "metadata_location": "nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/comstock_amy2018_release_1/metadata_and_annual_results/by_state/parquet",
        "release_name": "comstock_amy2018_release_1",
        "release_year": "2024",
        "state": "CA", //TIP: for testing use a state with less data like "AK"
        //"toggle_date_partition": true, //! originally implemented as optional and removed. This can be implemented in the transform.py
        "upgrades": [
          "0",
          "1"
        ]
        //TODO: you can add more configuration here. For instance, if you support column based filtering in the transform, you can add columns to filter out here.
      }
    ],

    //etl settings controlling how the ETL is executed
    "settings":{
      "log_dir": "logs", //relative to project root of the oedi-etl,(scripts/etl), default = logs
      "log_filename": "etl.log", //default - tagged with timestamp as {timestamp}-{logging_filename}.log
      "logging_level": "INFO", //default
      "idle_timeout_in_minutes":5, //optional - default  is 5 minutes and the system shut down after 5 minutes of inactivity. This is a fallback.
      "listing_page_size": 500, //optional - default is 500. This is the number of files to list in a single page and the lister in the fetcher sleeps for 1 second after each page fetch pacing the workflow. Python Glue Jobs have max 1DPU capacity (4vCpu and 16GB) and limited bandwidth. The processing rate is significantly slower than this.
      "max_listing_queue_size": 1000, //optional - default is 1000. This a back pressure threshold -> control mechanism to prevent the lister from listing too many files at once to allow downstream tasks to catch up.

    }
  }
}
```

### Deployment Configuration

* **appName**: `"NbiBuildingAnalytics"` – Application name.
* **account**: `"xxx"` – AWS account for deployment.
* **deploymentEnv**: `"dev"` – Environment (e.g., dev).
* **profile**: `"profile_name"` – AWS CLI profile.
* **regions**: `["us-west-2"]` – Deployment regions. You can deploy to multiple regions
* **requireApproval**: `"never"` – Disables manual approval. Options are never (never ask) | broadening (default and only required when security privilege is expanded) | anyChange (ask for all changes).

### ETL Configuration

* **output_dir**: `"etl_output"` – ETL output directory.
* **data_partition_in_release**: `"timeseries_individual_buildings/by_state"` – Partition path for building data.
* **base_partition**: `"oedi-data-lake/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock"` – S3 path to main dataset.
* **job_specific**: Job-specific parameters:
  * **metadata_location**: Metadata path for the job.
  * **release_name**: Name of the dataset release (e.g., `"comstock_amy2018_release_1"`).
  * **release_year**: Year of the release (e.g., `"2024"`).
  * **state**: State data being processed (e.g., `"CA"`).
  * **upgrades**: List of upgrade codes (e.g., `["0", "1"]`).

### Key Considerations

* **Data Structure Assumptions**: ETL assumes the base partition and data structure follow specific conventions. Other schemas may not be compatible without modification.
* **Flexible Jobs**: `job_specific` settings allow processing different releases, but the overall data structure is fixed.
* **Deployment**: Deployment settings ensure the app is deployed in the correct AWS environment with no manual approval needed.

## Design Guide

This document outlines the architecture and process for setting up an ETL pipeline and querying infrastructure for energy analytics, leveraging AWS Glue for ETL, AWS S3 for storage, and AWS Athena for querying. The workflow is designed to handle configurable data extraction, transformation, and load (ETL) operations and provide an efficient querying mechanism that interfaces with PowerBI.

By separating the ETL process from post-ETL querying, the system ensures optimal performance and cost-efficiency while maintaining flexibility for future modifications. The JSON-based configuration allows easy adaptation to changing data needs, while the infrastructure built on CDK ensures that the deployment is scalable and manageable.

### ETL

NOTE: *You can directly use Athena with the DL and pretty much achieve the same thing. The ETL is used for query performance and cost effectiveness given the project setup. Also, running the join during the ETL is inefficient for storage purposes. This is done to optimize the querying performance and deemed acceptable given the project use case*

The ETL script is written in Python and is manually triggered with configurable input parameters, such as `release_year`, `release_name`, `state`, and `upgrades`. There are two parts to the script. One handles each configuration set corresponding to a single task and lists and filters S3 objects based on these parameters, fetching only the relevant data. As the script processes the filtered objects, it performs transformations such as aggregating from 15-minute intervals to 1-hour intervals, compressing the data using Snappy, and joining relevant metadata. The transformed data is then loaded into the staging area in S3. This process is optimized for parallel execution, utilizing in-memory transformations and uploading data efficiently to minimize latency and maximize resource usage. Then there is another part of the script that runs multiple tasks in parallel.

The ETL is executed once per project setup for a given time period, while the Athena querying is used for ongoing data exploration and feeding the BI platform.

#### Data Lake Structure

The root data for this ETL process is stored in the following bucket:
`https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=nrel-pds-building-stock%2Fend-use-load-profiles-for-us-building-stock%2F`

Bucket structure:
`oedi-data-lake/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/`

#### Configurable Parameters

1. **Bucket and Prefix**:
   * These parameters should be configurable to handle various data sources.

2. **Release Year**:
   * Example directories: `2024/`, `2023/`, `2022/`, etc.

3. **Release Name**:
   * Example directories:
     * `comstock_amy2018_release_1/`
     * `resstock_amy2018_release_2/`
     * `resstock_dataset_2024.1/`
     * ...

4. **Data Directory (renamed to `data_block`)**:
   * This represents different sections within each release.
   * Examples of directories:
     * `building_energy_models/`
     * `geographic_information/`
     * `metadata/`
     * `timeseries_aggregates/`
     * `timeseries_individual_buildings/`
     * `weather/`
   * The structures within these directories may vary, and the ETL is designed to handle `timeseries_individual_buildings` data. Making it more configurable is possible but requires more development time and is outside the scope of this project.

5. **Partitioning by Region/State**:
   * In the `timeseries_individual_buildings` directory, the data is further divided into:
     * `by_puma_midwest/`
     * `by_puma_na/`
     * `by_puma_northeast/`
     * `by_puma_south/`
     * `by_puma_west/`
     * `by_state/`

   * For this project, the focus is on the `by_state` partition. Again, the ETL can be extended to handle other partitions if needed. However, this requires additional development effort and is not covered in this scope.
   * The `by_state` directory is partitioned as:
     * `upgrade=0`, `upgrade=1`, `upgrade=2`, etc.
     * `state=AK`, `state=AL`, `state=CA`, etc.
   * The ETL should extract specific sets of `upgrades` and `states`, and these values must be configurable.

6. **Metadata**:
   * The metadata, located in the `metadata` directory, provides building-specific information.
   * Example structure for the `by_state` partition:

     ```
     CA_baseline_metadata_and_annual_results.parquet
     CA_upgrade01_metadata_and_annual_results.parquet
     CA_upgrade02_metadata_and_annual_results.parquet
     ```

*Note: you can have multiple configurations for different releases, states, and upgrades.*

### Key Operations in ETL

1. **Extraction**:
    * The ETL begins by reading from the S3 bucket using filters based on parameters such as release year and state.
    * Metadata is stored alongside the main table in the output directory. Joining the metadata during this phase may help for the project specific query performance, however it comes at huge storage cost and is avoided.

2. **Transformation**:
    * Data is aggregated from 15-minute intervals to 1-hour intervals.
    * The ETL has a configurable flag to enable/disable date partitioning, improving performance for time-series queries.

3. **Load**:
    * Transformed data is written to an S3 bucket shared with Athena.
    * Output is preserved in Parquet format and compressed using Snappy for speed and performance.

### Key Design Considerations

Once Glue resources are provisioned, costs are incurred by the second and the design focuses on balancing cost and performance as follows:

* **Async I/O** for S3 file listing, fetching, and uploading, ensuring these operations do not block other tasks.
* **Separate worker pools** handle input (fetching) and output (uploading) I/O tasks independently for better parallelization.
* **Multiprocessing for CPU-bound tasks** such as data transformation and aggregation to fully utilize available CPU cores.
* **Queue-based task management** to decouple I/O and processing operations, allowing for efficient task distribution across workers.
* **Parallel partition handling** where different state-upgrade combinations are processed concurrently for faster overall performance.

### ETL Flow Overview

1. **Main Process**: The main function loads configurations and initializes multiple job configurations in parallel. Each configuration specifies a combination of state and upgrade, and jobs run concurrently.

2. **Listing and File Fetching**: Files are listed asynchronously based on the state-upgrade partition and added to a processing queue in chunks (pages). This ensures high-volume listings are handled efficiently.

3. **Async I/O**: Files are fetched from S3 using asynchronous methods to avoid blocking operations. The fetched data is then pushed to a processing queue.

4. **File Processing**: Using multiprocessing, files are transformed and aggregated by converting 15-minute intervals to 1-hour data. Each CPU worker processes files from the queue concurrently, optimizing CPU utilization.

5. **Async Uploading**: After processing, files are asynchronously uploaded to S3. This ensures that upload operations do not block further processing.

6. **Partition Parallelism**: The ETL processes multiple partitions (state-upgrade combinations) in parallel, significantly improving efficiency for larger datasets.

### Output Configuration

* The ETL output is written to a configurable S3 directory under the staging bucket.
* The directory structure will be environment-based, ensuring that different environments (e.g., dev, prod) are kept separate.
* All ETL output is written to a single table, ensuring that the querying process is streamlined. The table is automatically updated by the Glue Crawler after each ETL run.

### Crawler Job: Schema Discovery

* After the ETL job completes, a Glue Crawler is triggered to automatically discover the schema of the output data.
* The Glue Crawler updates the Athena table schema with new partitions or newly added data.

## Athena Integration

The ETL pipeline outputs data to an S3 bucket, which is directly queried by Athena.

### Querying Process

* **Workgroup Setup**: A dedicated Athena Workgroup is configured for the project. This includes saved queries for frequent data operations.
* **Saved Queries**: Common queries (e.g., aggregating data, filtering by building type) are stored as saved queries, allowing for quick access to frequently run operations.
* **Efficient Querying**: The use of Parquet format and the aggregation of data in the ETL step ensures minimal data scanning, lowering query costs and improving performance.
* **Shared Bucket**: A single S3 bucket is used for both **ETL outputs** and **query results**. This bucket is divided into two directories: `etl-outputs` and `query-results`.

## Key Infrastructure Components

The following components are implemented using AWS CDK:

1. **ETL Job**:
    * Manually triggered Glue job, driven by an editable Python script.
    * Responsible for extraction, transformation, and loading of data.

2. **Athena Workgroup**:
    * A workgroup for managing and executing queries.
    * Includes the ability to run pre-saved queries and write results to S3.

3. **Shared S3 Bucket**:
    * A single bucket used for both ETL output and Athena query results.
    * The bucket is configured to handle different environments.

4. **Glue Crawler**:
    * A crawler is set up to automatically discover schema changes after each ETL job and update the Athena tables.

### Athena Query Integration with PowerBI

* Athena provides the querying interface to PowerBI. The saved queries are used to expose clean data views to the BI platform, allowing for visualization without extensive manual querying.
