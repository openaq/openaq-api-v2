import pathlib

from aws_cdk import (
    aws_lambda,
    Stack,
    Duration,
    aws_events,
    aws_events_targets,
)
from constructs import Construct


class LambdaRollupStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        package_directory: str,
        env_variables,
        lambda_timeout: int = 900,
        memory_size: int = 1512,
        **kwargs,
    ) -> None:
        """Lambda plus cronjob to rollup data"""
        super().__init__(scope, id, *kwargs)

        package = aws_lambda.Code.from_asset(
            str(pathlib.Path.joinpath(package_directory, "package.zip"))
        )

        rollup_function = aws_lambda.Function(
            self,
            f"{id}-rollup-lambda",
            code=package,
            handler="openaq_fastapi.ingest.handler.rollup_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            allow_public_subnet=True,
            memory_size=memory_size,
            timeout=Duration.seconds(lambda_timeout),
            environment=env_variables,
        )

        aws_events.Rule(
            self,
            f"{id}-rollup-hourly-event-rule",
            schedule=aws_events.Schedule.cron(minute="0/5"),
            targets=[
                aws_events_targets.LambdaFunction(rollup_function),
            ],
        )
