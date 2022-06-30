# OpenAQ API Version 2
[![Slack Chat](https://img.shields.io/badge/Chat-Slack-ff69b4.svg "Join us. Anyone is welcome!")](https://join.slack.com/t/openaq/shared_invite/zt-yzqlgsva-v6McumTjy2BZnegIK9XCVw)

![Deploy](https://github.com/openaq/openaq-api-v2/actions/workflows/deploy-prod.yml/badge.svg)


## Overview
This is the main API for the [OpenAQ](https://openaq.org) project. It is a web-accessible API that provides endpoints to query the real-time and historical air quality measurements on the platform.

The API is accessible at [api.openaq.org](https://api.openaq.org) and documentation can be found at [docs.openaq.org](https://docs.openaq.org/).

### Platform Overview
The OpenAQ Data format is explained in [openaq-data-format](https://github.com/openaq/openaq-data-format).


[openaq-fetch](https://github.com/openaq/openaq-fetch) and [openaq-fetch-lcs](https://github.com/openaq/openaq-fetch-lcs) take care of fetching new data and writing to [S3](https://openaq-fetches.s3.amazonaws.com/index.html). Lambda functions defined in [ingest/](openaq_fastapi/openaq_fastapi/ingest/) then load data into the database, defined in [openaq-db](https://github.com/openaq/openaq-db).

## Getting started
This repository holds the code for the OpenAQ API Version 2.

This API is based on Python 3 and includes an AWS CDK project to help in deployment.

The API code is all in [openaq_fastapi](openaq_fastapi/)

All code related to deployment is located in [cdk](cdk/)

Further documentation can be found in each respective directory.

### Dependencies

Install prerequisites:
- [Docker](https://www.docker.com/)

## Local Development Environment
There are a few ways to run the API locally

### settings
Settings can be loaded using `.env` files and multiple files can be kept and used. The easiest way to manage multiple environment files is to add an extension describing your environment. For example, if I wanted to keep a production, staging and local environment I would save them as `.env.production`, `.env.staging` and `.env.local` each with their own settings.

### uvicorn
The easiest way to run the API locally is to use uvicorn. Make sure that you have your settings (`.env`) file setup. Once that is done you can run the following from the `openaq_fastapi/openaq_fastapi` directory. Variables from the `.env` files can be overrode by setting them inline.
```
# Run using the default .env file
uvicorn main:app --reload
```
You can also specify which `.env` file to load by passing the `ENV` variable. This should not include the `.env.` prefix
```
# Run our production environment
ENV=production uvicorn main:app --reload
```
And you can always override variables by setting them inline. This is handy for when you want to change something for the purpose of debugging.
```
# Run the staging environment and add verbose logging
ENV=staging LOG_LEVEL=debug uvicorn main:app --reload
```

### Historic method
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

## Rate limiting

In the production environment rate limiting is handled in two places, AWS WAF and at the application level with [Starlette Middleware](https://www.starlette.io/middleware/). The application rate limiting is configurable via environment variables. The rate limiting middleware requires access to an instance of [redis](https://redis.io/). For local development [docker](https://www.docker.com/) can be a convenient method to set up a local redis instance. With docker, use the following commend:

```sh
docker run --name redis -p 6379:6379 -d redis:6.2-alpine 
```

Now a redis instance will be available at ``` http://localhost:6379 ```. Configure the REDIS_HOST to `localhost` and REDIS_PORT to `6379`. 

### Rate limiting values

Rate limiting can be toggled off for local develop via the `RATE_LIMITING` environment variable. Other rate limiting values are:
* `RATE_AMOUNT` - The number of requests allowed without a valid API key
* `RATE_AMOUNT_KEY` - The number of requests allow with a valid API key
* `RATE_TIME` - The number of minutes for the rate

e.g. `RATE_AMOUNT=5` and `RATE_TIME=1` would allow 5 requests per 1 minute.

N.B. - With AWS WAF rate limiting also occurs at the cloudfront stage. The application level rate limiting should be less than or equal to the value set at AWS WAF.


## Contributing
There are a lot of ways to contribute to this project, more details can be found in the [contributing guide](CONTRIBUTING.md).

## Projects using the API

- openaq-browser [site](http://dolugen.github.io/openaq-browser) | [code](https://github.com/dolugen/openaq-browser) - A simple browser to provide a graphical interface to the data.
- openaq [code](https://github.com/nickolasclarke/openaq) - An isomorphic Javascript wrapper for the API
- py-openaq [code](https://github.com/dhhagan/py-openaq) - A Python wrapper for the API
- ropenaq [code](https://github.com/ropenscilabs/ropenaq) - An R package for the API

For more projects that are using OpenAQ API, checkout the [OpenAQ.org Community](https://openaq.org/#/community) page.
