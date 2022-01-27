import pathlib
from pathlib import Path

import docker
import aws_cdk
from aws_cdk import (
    aws_lambda,
    aws_s3,
    Stack,
    Duration,
    CfnOutput,
    Tags,
    aws_events,
    aws_events_targets,
)
from aws_cdk.aws_apigatewayv2_alpha import HttpApi, HttpMethod
from aws_cdk.aws_apigatewayv2_integrations_alpha import HttpLambdaIntegration
from constructs import Construct

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

code_dir = pathlib.Path(__file__).parent.absolute()
docker_dir = code_dir.parent.absolute()


def dictstr(item):
    return item[0], str(item[1])


env = dict(map(dictstr, settings.dict().items()))
print(env)

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

# stagingpackage = aws_lambda.Code.from_asset(
#     str(pathlib.Path.joinpath(code_dir, "package.zip"))
# )
# prodpackage = aws_lambda.Code.from_asset(
#     str(pathlib.Path.joinpath(code_dir, "package.zip"))
# )
# ingestpackage = aws_lambda.Code.asset(
#     str(pathlib.Path.joinpath(code_dir, "package.zip"))
# )


class LambdaApiStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        **kwargs,
    ) -> None:
        """Define stack."""
        super().__init__(scope, id, *kwargs)

        package = aws_lambda.Code.from_asset(
            str(pathlib.Path.joinpath(code_dir, "package.zip"))
        )

        openaq_api = aws_lambda.Function(
            self,
            f"{id}-lambda",
            code=package,
            handler="openaq_fastapi.main.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            allow_public_subnet=True,
            memory_size=1512,
            timeout=Duration.seconds(30),
            environment=env,
        )

        api = HttpApi(
            self,
            f"{id}-endpoint",
            default_integration=HttpLambdaIntegration(
                "ApiIntegration",
                openaq_api,
            ),
            cors_preflight={
                "allow_headers": [
                    "Authorization",
                    "*",
                ],
                "allow_methods": [
                    HttpMethod.GET,
                    HttpMethod.HEAD,
                    HttpMethod.OPTIONS,
                    HttpMethod.POST,
                ],
                "allow_origins": ["*"],
                "max_age": Duration.days(10),
            },
        )

        CfnOutput(self, "Endpoint", value=api.url)


class LambdaIngestStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        **kwargs,
    ) -> None:
        """Lambda plus cronjob to ingest metadata, realtime and pipeline data"""
        super().__init__(scope, id, *kwargs)

        package = aws_lambda.Code.from_asset(
            str(pathlib.Path.joinpath(code_dir, "package.zip"))
        )

        ingest_function = aws_lambda.Function(
            self,
            f"{id}-ingestlambda",
            code=package,
            handler="openaq_fastapi.ingest.handler.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            allow_public_subnet=True,
            memory_size=1512,
            timeout=Duration.seconds(settings.INGEST_TIMEOUT),
            environment=env,
        )

        aws_events.Rule(
            self,
            f"{id}-ingest-event-rule",
            schedule=aws_events.Schedule.cron(minute="0/15"),
            targets=[
                aws_events_targets.LambdaFunction(ingest_function),
            ],
        )

        openaq_fetch_bucket = aws_s3.Bucket.from_bucket_name(
            self, "{id}-OPENAQ-FETCH-BUCKET", settings.OPENAQ_FETCH_BUCKET
        )

        openaq_fetch_bucket.grant_read(ingest_function)


app = aws_cdk.App()
print(f"openaq-lcs-api{settings.OPENAQ_ENV}")
staging = LambdaApiStack(app, "openaq-lcs-apistaging")
prod = LambdaApiStack(app, "openaq-lcs-api")

ingest = LambdaIngestStack(
    app, f"openaq-lcs-ingest-{settings.OPENAQ_ENV}"
)

Tags.of(staging).add("devseed", "true")
Tags.of(staging).add("lcs", "true")
Tags.of(ingest).add("devseed", "true")
Tags.of(ingest).add("lcs", "true")
Tags.of(prod).add("devseed", "true")
Tags.of(prod).add("lcs", "true")
app.synth()
