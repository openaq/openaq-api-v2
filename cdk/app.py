import pathlib
from pathlib import Path
import os

import docker
from aws_cdk import aws_lambda, aws_s3, aws_ec2, core, aws_logs, aws_iam
from aws_cdk.aws_apigatewayv2 import HttpApi, HttpMethod, CfnStage, HttpStage
from aws_cdk.aws_apigatewayv2_integrations import LambdaProxyIntegration
from pydantic import BaseSettings
import aws_cdk.aws_lambda_event_sources as EventSources
import aws_cdk.aws_s3_notifications as s3n

code_dir = pathlib.Path(__file__).parent.absolute()
parent = code_dir.parent.absolute()
env_file = Path.joinpath(parent, ".env")


class Settings(BaseSettings):
    DATABASE_URL: str
    DATABASE_WRITE_URL: str
    OPENAQ_ENV: str = "staging"
    OPENAQ_FASTAPI_URL: str
    TESTLOCAL: bool = True
    OPENAQ_FETCH_BUCKET: str
    OPENAQ_ETL_BUCKET: str

    class Config:
        env_file = env_file


settings = Settings()
# I dont see the reason for this variable
# OPENAQ_FETCH_BUCKET = "openaq-fetches"


code_dir = pathlib.Path(__file__).parent.absolute()
docker_dir = code_dir.parent.absolute()


def dictstr(item):
    return item[0], str(item[1])

env = dict(map(dictstr, settings.dict().items()))

# create package using docker
client = docker.from_env()
print("Building client image", docker_dir)
client.images.build(
    path=str(docker_dir),
    dockerfile="Dockerfile",
    tag="openaqfastapi",
    nocache=False,
)
print("Running client image")
client.containers.run(
    image="openaqfastapi",
    command="/bin/sh -c 'cp /tmp/package.zip /local/package.zip'",
    remove=True,
    volumes={str(code_dir): {"bind": "/local/", "mode": "rw"}},
    user=0,
)

print("Packaging code")

stagingpackage = aws_lambda.Code.asset(
    str(pathlib.Path.joinpath(code_dir, "package.zip"))
)
prodpackage = aws_lambda.Code.asset(
    str(pathlib.Path.joinpath(code_dir, "package.zip"))
)
ingestpackage = aws_lambda.Code.asset(
    str(pathlib.Path.joinpath(code_dir, "package.zip"))
)



class LambdaApiStack(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        id: str,
        package,
        **kwargs,
    ) -> None:
        """Define stack."""
        super().__init__(scope, id, **kwargs)

        openaq_api = aws_lambda.Function(
            self,
            f"{id}-api-lambda",
            code=package,
            handler="openaq_fastapi.main.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            allow_public_subnet=True,
            memory_size=1512,
            timeout=core.Duration.seconds(30),
            environment=env,
        )

        api = HttpApi(
            self,
            f"{id}-api-endpoint",
            api_name = f"{id}-endpoint",
            create_default_stage=False, # cant auto create the access log wit default settings
            default_integration=LambdaProxyIntegration(handler=openaq_api),
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
                "max_age": core.Duration.days(10),
            },
        )

        log = aws_logs.LogGroup(
            self,
            f"{id}-http-gateway-log",
        )

        stage = CfnStage(
            self,
            f"{id}-stage",
            api_id = api.http_api_id,
            stage_name = "$default",
            auto_deploy = True,
            access_log_settings = CfnStage.AccessLogSettingsProperty(
                destination_arn=log.log_group_arn,
                format='{ "requestId":"$context.requestId", "ip": "$context.identity.sourceIp", "requestTime":"$context.requestTime", "httpMethod":"$context.httpMethod","routeKey":"$context.routeKey", "status":"$context.status","protocol":"$context.protocol", "responseLength":"$context.responseLength", "responseLatency": $context.responseLatency, "path": "$context.path"}',
            )
        )

        #core.CfnOutput(self, "Endpoint", value=api.url)


class LambdaIngestStack(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        id: str,
        bucket: str,
        package,
        **kwargs,
    ) -> None:
        """Define stack."""
        super().__init__(scope, id, **kwargs)

        ingest_function = aws_lambda.Function(
            self,
            f"{id}-ingest-lambda",
            code=package,
            handler="openaq_fastapi.ingest.handler.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            allow_public_subnet=True,
            memory_size=1512,
            timeout=core.Duration.seconds(900),
            environment=env,
        )

        openaq_fetch_bucket = aws_s3.Bucket.from_bucket_name(
            self,
            "{id}-OPENAQ-FETCH-BUCKET",
            bucket,
        )

        openaq_fetch_bucket.grant_read(ingest_function)

        # openaq_fetch_bucket.add_event_notification(
        #     event = aws_s3.EventType.OBJECT_CREATED_PUT,
        #     dest = s3n.LambdaDestination(ingest_function),
        #     filters = [
        #         aws_s3.NotificationKeyFilter(prefix="stations/"),
        #         aws_s3.NotificationKeyFilter(prefix="measures/"),
        #         aws_s3.NotificationKeyFilter(prefix="versions/")
        #     ]
        # )

        # add the bucket trigger to queue the file

        #ingest_function.add_event_source(s3PutEventSource);

        # # add the cron job to schedule the ingest
        #     props.queue.grantSendMessages(scheduler);
        #     new events.Rule(this, `${interval}Rule`, {
        #         schedule: events.Schedule.rate(duration),
        #         targets: [new eventTargets.LambdaFunction(scheduler)],
        #     });



app = core.App()
print(f"openaq-lcs-api-{settings.OPENAQ_ENV} using {env['OPENAQ_FETCH_BUCKET']}")

prod = LambdaApiStack(
    app,
    "openaq-lcs-api",
    package=prodpackage,
)

# staging = LambdaApiStack(
#     app,
#     f"openaq-lcs-api-{settings.OPENAQ_ENV}",
#     package=stagingpackage
# )

api = LambdaApiStack(
    app,
    f"{env['OPENAQ_ENV']}-api",
    package=stagingpackage,
    env=core.Environment(
        account=os.getenv('CDK_DEFAULT_ACCOUNT'),
        region=os.getenv('CDK_DEFAULT_REGION')
    ),
)

ingest = LambdaIngestStack(
    app,
    f"{env['OPENAQ_ENV']}-ingest",
    env['OPENAQ_FETCH_BUCKET'],
    package=ingestpackage,
    env=core.Environment(
        account=os.getenv('CDK_DEFAULT_ACCOUNT'),
        region=os.getenv('CDK_DEFAULT_REGION')
    ),
)


#core.Tags.of(staging).add("devseed", "true")
#core.Tags.of(staging).add("lcs", "true")
#core.Tags.of(ingest).add("devseed", "true")
#core.Tags.of(ingest).add("lcs", "true")
#core.Tags.of(prod).add("devseed", "true")
#core.Tags.of(prod).add("lcs", "true")
app.synth()
