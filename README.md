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
