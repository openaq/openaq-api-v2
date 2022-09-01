import aws_cdk
from aws_cdk import (
    Environment,
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

p = os.path.abspath('../cloudfront_logs')
sys.path.insert(1, p)
from cloudfront_logs.settings import settings as cloudfront_logs_lambda_env

app = aws_cdk.App()

env = Environment(account=settings.CDK_ACCOUNT, region=settings.CDK_REGION)

api = LambdaApiStack(
    app,
    f"openaq-api-{settings.ENV}",
    env_name=settings.ENV,
    lambda_env=lambda_env,
    cloudfront_logs_lambda_env=cloudfront_logs_lambda_env,
    vpc_id=settings.VPC_ID,
    hosted_zone_name=settings.HOSTED_ZONE_NAME,
    hosted_zone_id=settings.HOSTED_ZONE_ID,
    api_lambda_timeout=settings.API_LAMBDA_TIMEOUT,
    api_lambda_memory_size=settings.API_LAMBDA_MEMORY_SIZE,
    cf_logs_lambda_timeout=settings.CF_LOG_LAMBDA_TIMEOUT,
    cf_logs_lambda_memory_size=settings.CF_LOGS_LAMBDA_MEMORY_SIZE,
    domain_name=settings.DOMAIN_NAME,
    cert_arn=settings.CERTIFICATE_ARN,
    web_acl_id=settings.WEB_ACL_ID,
    env=env,
)

Tags.of(api).add("project", settings.PROJECT)
Tags.of(api).add("product", "api")
Tags.of(api).add("env", settings.ENV)

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
