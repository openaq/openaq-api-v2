# OPENAQ Version 2 API

The OpenAQ Version 2 API, located in the [openaq_fastapi](openaq_fastapi/), is built using Python 3 and [FastAPI](https://fastapi.tiangolo.com/). It takes advantage of [Pydantic](https://pydantic-docs.helpmanual.io/) for data validation.

## Getting Started
Install prerequisites:

- [git](https://git-scm.com)
- [Docker](https://www.docker.com/)

Clone this repository locally (see these [instructions](https://help.github.com/en/articles/cloning-a-repository))

It is recommended to install this code in a virtual environment, [venv](https://docs.python.org/3/tutorial/venv.html) is preferred for Python3:

```
python3 -m venv /path/to/new/virtual/environment/openaqapi-venv
source openaqapi-venv/bin/activate

```
Install dependencies:

```
pip install -e .
```


## Development
A convenience executable is included to run a local development version of the API.

### Database Setup

First, set up your development database. Build an image from the Dockerfile:
```
docker build -t openaq-db-docker .
```
Create and run a container from the docker image:
```
docker run -d --name openaq-db -p 5432:5432 openaq-db-docker
```
Check to see if it's running:
```
docker ps
```

### Environment Variables
Next, set up your environment variables. Create a file called `.env` in the parent directory with the following contents:

```
PGUSER=postgres
PGPASSWORD=postgres
PGDATABASE=testdb
PGHOST=localhost
PGPORT=5432


DATABASE_URL="postgres://${PGUSER}:${PGPASSWORD}@${PGHOST}:${PGPORT}/${PGDATABASE}"
DATABASE_WRITE_URL="postgres://${PGUSER}:${PGPASSWORD}@${PGHOST}:${PGPORT}/${PGDATABASE}"

#### Only needed for Production, can leave as is #####
AWS_PROFILE=openaq
OPENAQ_ENV='staging'
OPENAQ_FASTAPI_URL=""

OPENAQ_FETCH_BUCKET=openaq-fetches
OPENAQ_ETL_BUCKET=openaq-fetches
```

Now you are all set!

Run the API locally:
```
openaqapi
```



# OPENAQ Version 2 Ingest

The ETL process for the Version 2 Database is split into two parts. The [ingest](openaq_fastapi/ingest/) code is designed to load data that has been fetched and normalized by either [OpenAQ Fetch](https://github.com/openaq/openaq-fetch) or [OpenAQ LCS Fetch](https://github.com/openaq/openaq-lcs-fetch) into an s3 bucket.

After installing the Ingest Lambda, it is required to add a cloudwatch cron trigger (every 5 minutes recommended) and an s3 event trigger on the S3 Fetch Bucket. These can both be done from the AWS Lambda web console.
