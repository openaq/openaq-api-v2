# OPENAQ Version 2 API

The OpenAQ Version 2 API, located in the [openaq_fastapi](openaq_fastapi/), is built using Python 3 and [FastAPI](https://fastapi.tiangolo.com/). It takes advantage of [Pydantic](https://pydantic-docs.helpmanual.io/) for data valdation.

## Installation
It is recommended to install in a python virtual environment.

`
pip install -e .[dev]
`

A convenience executable is included to run a local development version of the API. You must have your environment set up ([Setting up your Environment](../README.md)) prior to running.

`
openaqapi
`



# OPENAQ Version 2 Ingest

The ETL process for the Version 2 Database is split into two parts. The [ingest](openaq_fastapi/ingest/) code is designed to load data that has been fetched and normalized by either [OpenAQ Fetch](https://github.com/openaq/openaq-fetch) or [OpenAQ LCS Fetch](https://github.com/openaq/openaq-lcs-fetch) into an s3 bucket.

After installing the Ingest Lambda, it is required to add a cloudwatch cron trigger (every 5 minutes recommended) and an s3 event trigger on the S3 Fetch Bucket. These can both be done from the AWS Lambda web console.
