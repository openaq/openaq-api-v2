# OpenAQ Version 2 API
This repository holds the code for the OpenAQ Version 2 API

This API is based on Python 3 and includes an AWS CDK project to help in deployment.

All code related to deployment is located in [cdk](cdk/)

The API code is all in [openaq_fastapi](openaq_fastapi/)

Further documentation can be found in each respective directory.

## Setting up your environment ##
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

## Docker Compose Development Environment ##
A Docker Compose Development Environment is included that includes both the API and the Database.

In order to use the Docker environment, you must pull in the openaq-db submodule.

```
git submodule update --init --recursive
```

Using VSCode with the Remote - Containers extension, you can start the development environment by clicking on the green box in the lower right hand corner of VSCode and select "Remote-Containers: Reopen in Container" from the menu that drops down. It will take a while the first time to pull down and build the docker images. Once started, the API will be exposed at http://0.0.0.0:8888 on your local machine.

Using Docker Compose Directly
```
cd .devcontainer
docker-compose build
docker-compose up
```

The API and Database will start up. The API will be exposed at http://0.0.0.0:8888 on your local machine.
You can enter a terminal on the API Docker instance from another terminal by running:
```
docker-compose exec -it api /bin/bash
```

From either the VSCode terminal or the terminal as above on the API container, you can load a sample days worth of data using the Fetch data loader and the included sample data from 12/31/2020 by running the following on the API container. This will load the data and run the post-processing scripts that are normally run on a cron in production.

```
/workspace/sample/load_sample_data.sh
```
