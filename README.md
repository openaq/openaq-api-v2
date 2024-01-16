# OpenAQ API
[![Slack Chat](https://img.shields.io/badge/Chat-Slack-ff69b4.svg "Join us. Anyone is welcome!")](https://join.slack.com/t/openaq/shared_invite/zt-yzqlgsva-v6McumTjy2BZnegIK9XCVw)

![Deploy](https://github.com/openaq/openaq-api-v2/actions/workflows/deploy-prod.yml/badge.svg)


## Overview
This repository contains the source code for the [OpenAQ API](https://api.openaq.org), a publicly-accessible API that provides endpoints to query the real-time and historical air quality measurements on the OpenAQ platform.

> [!NOTE]  
> This repository is for setting up and deploying the OpenAQ API. If you just wish to access the public API to query data from the OpenAQ platform visit https://api.openaq.org or https://docs.openaq.org to learn more. 


### Platform Overview

[openaq-fetch](https://github.com/openaq/openaq-fetch) and [openaq-fetch-lcs](https://github.com/openaq/openaq-fetch-lcs) take care of fetching new data and writing to [S3](https://openaq-fetches.s3.amazonaws.com/index.html). Lambda functions defined in [ingest/](openaq_api/openaq_api/ingest/) then load data into the database, defined in [openaq-db](https://github.com/openaq/openaq-db).

## Getting started

The API code is all in [openaq_api](openaq_api/)

Deployment is managed with Amazon Web Services (AWS) Cloud Development Kit (CDK).

### Dependencies

## Local Development Environment

In production the OpenAQ API runs on AWS Lambda with the help of the [mangum](https://mangum.io/) library. This allows the application to run in a serverless envionment and take advantage of async Python and FastAPI routing. Despite the serverless deployment, running the API locally as a standard FastAPI application is largely unchanged, making local development much easier. 

### Settings
Settings can be loaded using `.env` files and multiple files can be kept and used. The easiest way to manage multiple environment files is to add an extension describing your environment. For example, if I wanted to keep a production, staging and local environment I would save them as `.env.production`, `.env.staging` and `.env.local` each with their own settings.

### uvicorn
The easiest way to run the API locally is to use uvicorn. Make sure that you have your settings (`.env`) file setup. Once that is done you can run the following from the `openaq_api/openaq_api` directory. Variables from the `.env` files can be overrode by setting them inline.
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

## Setting up your environment

### Local development environment

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

### AWS Cloud Deployment Environment

Additional environmnet variables are required for a full deployment to the AWS Cloud. 

## Rate limiting

In the production environment rate limiting is handled in two places, AWS WAF and at the application level with [Starlette Middleware](https://www.starlette.io/middleware/). The application rate limiting is configurable via environment variables. The rate limiting middleware requires access to an instance of a [redis](https://redis.io/) cluster. For local development, [docker](https://www.docker.com/) can be a convenient method to set up a local redis cluster. With docker, use the following command:

```sh
docker run -e "IP=0.0.0.0" -p 7000-7005:7000-7005 grokzen/redis-cluster:7.0.7
```

Now a redis instance will be available at ``` http://localhost:7000 ```. Configure the REDIS_HOST to `localhost` and REDIS_PORT to `7000`. 

> [!TIP]
> On some macOS system port 7000 is used by Airplay which can complicate the mapping of ports from the Docker container. The easiest option is to disable the Airplay reciever in system settings. `System settings -> General -> Airplay receiver (toggle off)`

### Rate limiting values

Rate limiting can be toggled off for local develop via the `RATE_LIMITING` environment variable. Other rate limiting values are:
* `RATE_AMOUNT` - The number of requests allowed without a valid API key
* `RATE_AMOUNT_KEY` - The number of requests allow with a valid API key
* `RATE_TIME` - The number of minutes for the rate

e.g. `RATE_AMOUNT=5` and `RATE_TIME=1` would allow 5 requests per 1 minute.

> [!NOTE] 
> With AWS WAF, rate limiting also occurs at the cloudfront stage. The application level rate limiting should be less than or equal to the value set at AWS WAF.


## Contributing
There are a lot of ways to contribute to this project, more details can be found in the [contributing guide](CONTRIBUTING.md).
