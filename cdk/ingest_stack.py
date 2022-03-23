import pathlib

from aws_cdk import (
    aws_lambda,
    aws_s3,
    Stack,
    Duration,
    aws_events,
    aws_events_targets,
)
from constructs import Construct


class IngestStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        fetch_bucket: str,
        package_directory: str,
        env_variables,
        lambda_timeout: int = 900,
        memory_size: int = 1512,
        **kwargs,
    ) -> None:
        """Lambda plus cronjob to ingest metadata,
        realtime and pipeline data"""
        super().__init__(scope, id, *kwargs)

        package = aws_lambda.Code.from_asset(
            str(pathlib.Path.joinpath(package_directory, "package.zip"))
        )

        ingest_function = aws_lambda.Function(
            self,
            f"{id}-ingestlambda",
            code=package,
            handler="openaq_fastapi.ingest.handler.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            allow_public_subnet=True,
            memory_size=memory_size,
            timeout=Duration.seconds(lambda_timeout),
            environment=env_variables,
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
            self,
            "{id}-OPENAQ-FETCH-BUCKET", fetch_bucket,
        )

        openaq_fetch_bucket.grant_read(ingest_function)
