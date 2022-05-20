import os
import subprocess
from typing import Dict

from aws_cdk import (
    aws_lambda,
    aws_s3,
    Stack,
    Duration,
    aws_events,
    aws_events_targets,
)

from constructs import Construct


class LambdaIngestStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        env_name: str,
        fetch_bucket: str,
        ingest_lambda_timeout: int,
        ingest_lambda_memory_size: int,
        ingest_rate_minutes: int = 15,
        **kwargs,
    ) -> None:
        """Lambda plus cronjob to ingest metadata,
        realtime and pipeline data"""
        super().__init__(scope, id, *kwargs)

        ingest_function = aws_lambda.Function(
            self,
            f"{id}-ingestlambda",
            code=aws_lambda.Code.from_asset(
                path='../openaq_fastapi',
                exclude=[
                    'venv',
                    '__pycache__',
                    'pytest_cache',
                ],
            ),
            handler="openaq_fastapi.ingest.handler.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            allow_public_subnet=True,
            memory_size=ingest_lambda_memory_size,
            timeout=Duration.seconds(ingest_lambda_timeout),
            layers=[
                self.create_dependencies_layer(
                    self,
                    f"{env_name}",
                    'ingest'
                ),
            ],
        )

        aws_events.Rule(
            self,
            f"{id}-ingest-event-rule",
            schedule=aws_events.Schedule.cron(
                minute=f"0/{ingest_rate_minutes}"
            ),
            targets=[
                aws_events_targets.LambdaFunction(ingest_function),
            ],
        )

        openaq_fetch_bucket = aws_s3.Bucket.from_bucket_name(
            self, "{env_name}-FETCH-BUCKET", fetch_bucket
        )

        openaq_fetch_bucket.grant_read(ingest_function)
