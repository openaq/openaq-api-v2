import pathlib

from aws_cdk import (
    aws_lambda,
    Stack,
    Duration,
    aws_logs as _logs,
)
from aws_cdk.aws_apigatewayv2 import CfnStage
from aws_cdk.aws_apigatewayv2_alpha import HttpApi, HttpMethod
from aws_cdk.aws_apigatewayv2_integrations_alpha import HttpLambdaIntegration
from constructs import Construct


class ApiStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        package_directory: str,
        env_variables,
        lambda_timeout: int = 30,
        memory_size: int = 1512,
        **kwargs,
    ) -> None:
        """FastAPI lambda API"""
        super().__init__(scope, id, *kwargs)

        package = aws_lambda.Code.from_asset(
            str(pathlib.Path.joinpath(package_directory, "package.zip"))
        )

        openaq_api = aws_lambda.Function(
            self,
            f"{id}-lambda",
            code=package,
            handler="openaq_fastapi.main.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            allow_public_subnet=True,
            memory_size=memory_size,
            timeout=Duration.seconds(lambda_timeout),
            environment=env_variables,
        )

        api = HttpApi(
            self,
            f"{id}-endpoint",
            create_default_stage=False,
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

        log = _logs.LogGroup(
            self,
            f"{id}-http-gateway-log",
        )

        CfnStage(
            self,
            f"{id}-stage",
            api_id=api.http_api_id,
            stage_name="$default",
            auto_deploy=True,
            access_log_settings=CfnStage.AccessLogSettingsProperty(
                destination_arn=log.log_group_arn,
                format='{"requestId":"$context.requestId", "ip": "$context.identity.sourceIp", "requestTime":"$context.requestTime", "httpMethod":"$context.httpMethod","routeKey":"$context.routeKey", "status":"$context.status","protocol":"$context.protocol", "responseLength":"$context.responseLength", "responseLatency": $context.responseLatency, "path": "$context.path"}',
            )
        )

        print(api)
