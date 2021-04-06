# OpenAQ API Version 2 
[![Slack Chat](https://img.shields.io/badge/Chat-Slack-ff69b4.svg "Join us. Anyone is welcome!")](https://openaq-slackin.herokuapp.com/)

## Overview
This is the main API for the [OpenAQ](https://openaq.org) project. It is a web-accessible API that provides endpoints to query the real-time and historical air quality measurements on the platform.

The API is accessible at [api.openaq.org](https://api.openaq.org) and documentation can be found at [docs.openaq.org](https://docs.openaq.org/).

### Platform Overview
The OpenAQ Data format is explained in [openaq-data-format](https://github.com/openaq/openaq-data-format).


[openaq-fetch](https://github.com/openaq/openaq-fetch) and [openaq-fetch-lcs](https://github.com/openaq/openaq-fetch-lcs) take care of fetching new data and writing to [S3](https://openaq-fetches.s3.amazonaws.com/index.html). Lambda functions defined in [ingest/](openaq_fastapi/openaq_fastapi/ingest/) then load data into the database, defined in [openaq-db](https://github.com/openaq/openaq-db).

## Getting started
This repository holds the code for the OpenAQ API Version 2. Version 1 can be viewed in the [old repository](https://github.com/openaq/openaq-api).

This API is based on Python 3 and includes an AWS CDK project to help in deployment.

The API code is all in [openaq_fastapi](openaq_fastapi/)

All code related to deployment is located in [cdk](cdk/)

Further documentation can be found in each respective directory.

### Dependencies

Install prerequisites:
- [Docker](https://www.docker.com/)

## Local Development Environment
A Docker Compose Development Environment is included that includes both the API and the Database to help run locally.

In order to use the Docker environment, you must pull in the openaq-db submodule:
```
git submodule update --init --recursive
```

Using Docker Compose Directly
```
cd .devcontainer
docker-compose build
docker-compose up
```

The API and Database will start up. The API will be exposed at http://0.0.0.0:8888 on your local machine.

Alternatively, you can use VSCode with the Remote - Containers extension, you can start the development environment by clicking on the green box in the lower right hand corner of VSCode and select "Remote-Containers: Reopen in Container" from the menu that drops down. It will take a while the first time to pull down and build the docker images. The API will be exposed at http://0.0.0.0:8888 on your local machine.

### Getting Sample data

You can enter a terminal on the API Docker instance from another terminal by running:
```
docker-compose exec api /bin/bash
```

From either the VSCode terminal or the terminal as above on the API container, you can load a sample days worth of data using the Fetch data loader and the included sample data from 12/31/2020 by running the following on the API container. This will load the data and run the post-processing scripts that are normally run on a cron in production.  

```
./sample/load_sample_data.sh
```
**Note**: This process can take up to 20 minutes, be patient.

## Setting up your environment 
**Note: this isn't needed for setting up a local environment**

To set up your environment, create a .env file that includes the following variables

```
AWS_PROFILE=openaq-prod
OPENAQ_APIUSER_PW=
OPENAQ_RWUSER_PW=
PGUSER=
PGPASSWORD=
PGDATABASE=
PGHOST=
PGPORT=


DATABASE_URL="postgres://apiuser:${OPENAQ_APIUSER_PW}@${PGHOST}:${PGPORT}/${PGDATABASE}?sslmode=require"
DATABASE_WRITE_URL="postgres://rwuser:${OPENAQ_RWUSER_PW}@${PGHOST}:${PGPORT}/${PGDATABASE}?sslmode=require"


# Prod
OPENAQ_ENV='staging'
OPENAQ_FASTAPI_URL=""

OPENAQ_FETCH_BUCKET=openaq-fetches
OPENAQ_ETL_BUCKET=openaq-fetches
```