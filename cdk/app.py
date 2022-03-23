import pathlib

import docker
import aws_cdk
from aws_cdk import (
    Tags,
)

# Stacks
from rollup_stack import RollupStack
from api_stack import ApiStack
from ingest_stack import IngestStack

# this is the only way that I can see to allow us to have
# one settings file and import it from there. I would recommend
# a better package structure in the future.
import os
import sys
p = os.path.abspath('../openaq_fastapi/openaq_fastapi')
sys.path.insert(1, p)
from settings import settings

code_dir = pathlib.Path(__file__).parent.absolute()
parent = code_dir.parent.absolute()
docker_dir = code_dir.parent.absolute()


def dictstr(item):
    return item[0], str(item[1])


env = dict(map(dictstr, settings.dict().items()))

# create package using docker
client = docker.from_env()
client.images.build(
    path=str(docker_dir),
    dockerfile="Dockerfile",
    tag="openaqfastapi",
    nocache=False,
)
client.containers.run(
    image="openaqfastapi",
    command="/bin/sh -c 'cp /tmp/package.zip /local/package.zip'",
    remove=True,
    volumes={str(code_dir): {"bind": "/local/", "mode": "rw"}},
    user=0,
)


app = aws_cdk.App()


ingest = IngestStack(
    app,
    f"openaq-ingest-{settings.OPENAQ_ENV}",
    fetch_bucket=settings.OPENAQ_FETCH_BUCKET,
    package_directory=code_dir,
    env_variables=env,
    lambda_timeout=30,
)
Tags.of(ingest).add("Project", settings.OPENAQ_ENV)


api = ApiStack(
    app,
    f"openaq-api-{settings.OPENAQ_ENV}",
    package_directory=code_dir,
    env_variables=env,
    lambda_timeout=30,
)
Tags.of(api).add("Project", settings.OPENAQ_ENV)


rollup = RollupStack(
    app,
    f"openaq-rollup-{settings.OPENAQ_ENV}",
    package_directory=code_dir,
    env_variables=env,
    lambda_timeout=900,
)
Tags.of(rollup).add("Project", settings.OPENAQ_ENV)

app.synth()
