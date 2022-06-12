from typing import Dict

from aws_cdk import (
    aws_lambda,
    Stack,
    Duration,
    aws_events,
    aws_events_targets,
)
from constructs import Construct

from cdk.utils import (
    stringify_settings,
    create_dependencies_layer,
)


class LambdaRollupStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        env_name: str,
        lambda_env: Dict,
        lambda_timeout: int = 900,
        lambda_memory_size: int = 1512,
        rate_minutes: int = 5,
        **kwargs,
    ) -> None:
        """Lambda plus cronjob to rollup data"""
        super().__init__(scope, id, *kwargs)

        rollup_function = aws_lambda.Function(
            self,
            f"{id}-rollup-lambda",
            code=aws_lambda.Code.from_asset(
                path='../openaq_fastapi',
                exclude=[
                    'venv',
                    '__pycache__',
                    'pytest_cache',
                ],
            ),
            handler="openaq_fastapi.ingest.handler.rollup_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            allow_public_subnet=True,
            memory_size=lambda_memory_size,
            timeout=Duration.seconds(lambda_timeout),
            environment=stringify_settings(lambda_env),
            layers=[
                create_dependencies_layer(
                    self,
                    f"{env_name}",
                    'api' # just use the same layer for now
                ),
            ],
        )

        aws_events.Rule(
            self,
            f"{id}-rollup-hourly-event-rule",
            schedule=aws_events.Schedule.cron(
                minute=f"0/{rate_minutes}"
            ),
            targets=[
                aws_events_targets.LambdaFunction(rollup_function),
            ],
        )
