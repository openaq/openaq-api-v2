import aws_cdk
from aws_cdk import (
    Environment,
    Tags,
)

from stacks.lambda_api_stack import LambdaApiStack

from settings import settings

# this is the only way that I can see to allow us to have
# one settings file and import it from there. I would recommend
# a better package structure in the future.
import os
import sys

p = os.path.abspath("../api")
sys.path.insert(1, p)
from openaq_api.settings import settings as lambda_env

p = os.path.abspath("../cloudfront_logs")
sys.path.insert(1, p)
from cloudfront_logs.settings import settings as cloudfront_logs_lambda_env

app = aws_cdk.App()

env = Environment(account=settings.CDK_ACCOUNT, region=settings.CDK_REGION)

api = LambdaApiStack(
    app,
    f"openaq-api-{settings.ENV}",
    env_name=settings.ENV,
    lambda_env=lambda_env,
    vpc_id=settings.VPC_ID,
    redis_security_group_id=settings.REDIS_SECURITY_GROUP_ID,
    redis_port=settings.REDIS_PORT,
    api_lambda_timeout=settings.API_LAMBDA_TIMEOUT,
    api_lambda_memory_size=settings.API_LAMBDA_MEMORY_SIZE,
    hosted_zone_name=settings.HOSTED_ZONE_NAME,
    hosted_zone_id=settings.HOSTED_ZONE_ID,
    domain_name=settings.DOMAIN_NAME,
    cert_arn=settings.CERTIFICATE_ARN,
    cloudfront_logs_lambda_env=cloudfront_logs_lambda_env,
    cf_logs_lambda_memory_size=settings.CF_LOGS_LAMBDA_MEMORY_SIZE,
    cf_logs_lambda_timeout=settings.CF_LOG_LAMBDA_TIMEOUT,
    waf_evaluation_window_sec=settings.WAF_RATE_LIMIT_EVALUATION_WINDOW,
    waf_rate_limit=settings.WAF_RATE_LIMIT,
    waf_block_ips=settings.WAF_BLOCK_IPS,
    env=env,
)

Tags.of(api).add("project", settings.PROJECT)
Tags.of(api).add("product", "api")
Tags.of(api).add("env", settings.ENV)

app.synth()
