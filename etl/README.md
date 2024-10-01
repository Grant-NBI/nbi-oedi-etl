# NBI OEDI ETL

The **NBI OEDI ETL** package is a data pipeline designed to extract, transform, and load (ETL) energy usage data from the OEDI data lake to an S3 bucket. It processes time-series building energy data by converting 15-minute intervals into hourly data, with optional partitioning by date. The pipeline handles specific configurations for states, release years, and upgrades, allowing for flexible and efficient data selection.

## Usage

The package can be used to run ETL jobs by specifying configuration parameters such as the S3 bucket, state, and upgrades.

## Installation

Ensure that the project is installed with Poetry: `poetry install`
