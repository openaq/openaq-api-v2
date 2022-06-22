import aws_cdk
from aws_cdk import (
    Tags,
)

from cdk.lambda_api_stack import LambdaApiStack
from cdk.lambda_ingest_stack import LambdaIngestStack
from cdk.lambda_rollup_stack import LambdaRollupStack

from settings import settings

# this is the only way that I can see to allow us to have
# one settings file and import it from there. I would recommend
# a better package structure in the future.
import os
import sys
p = os.path.abspath('../openaq_fastapi')
sys.path.insert(1, p)
from openaq_fastapi.settings import settings as lambda_env

app = aws_cdk.App()

api = LambdaApiStack(
    app,
    f"openaq-api-{settings.ENV}",
    env_name=settings.ENV,
    lambda_env=lambda_env,
    hosted_zone_name=settings.HOSTED_ZONE_NAME,
    hosted_zone_id=settings.HOSTED_ZONE_ID,
    lambda_timeout=settings.API_LAMBDA_TIMEOUT,
    memory_size=settings.API_LAMBDA_MEMORY_SIZE,
    domain_name=settings.DOMAIN_NAME,
    cert_arn=settings.CERTIFICATE_ARN,
    web_acl_id=settings.WEB_ACL_ID,
)

Tags.of(api).add("project", settings.PROJECT)
Tags.of(api).add("product", "api")
Tags.of(api).add("env", settings.ENV)

ingest = LambdaIngestStack(
    app,
    f"openaq-ingest-{settings.ENV}",
    env_name=settings.ENV,
    lambda_env=lambda_env,
    fetch_bucket=settings.FETCH_BUCKET,
    ingest_lambda_timeout=settings.INGEST_LAMBDA_TIMEOUT,
    ingest_lambda_memory_size=settings.INGEST_LAMBDA_MEMORY_SIZE,
    ingest_rate_minutes=15,
    topic_arn=settings.TOPIC_ARN,
)

Tags.of(ingest).add("project", settings.PROJECT)
Tags.of(ingest).add("product", "ingest")
Tags.of(ingest).add("env", settings.ENV)

rollup = LambdaRollupStack(
    app,
    f"openaq-rollup-{settings.ENV}",
    env_name=settings.ENV,
    lambda_env=lambda_env,
    lambda_timeout=settings.ROLLUP_LAMBDA_TIMEOUT,
    lambda_memory_size=settings.ROLLUP_LAMBDA_MEMORY_SIZE,
    rate_minutes=5,
)

Tags.of(rollup).add("project", settings.PROJECT)
Tags.of(rollup).add("product", "api")
Tags.of(rollup).add("env", settings.ENV)

app.synth()
