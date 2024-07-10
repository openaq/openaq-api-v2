from pathlib import Path
from typing import Union


from aws_cdk import (
    Environment,
    RemovalPolicy,
    aws_ec2,
    aws_elasticache,
    aws_s3,
    aws_sqs,
    aws_lambda,
    aws_logs,
    aws_iam,
    Stack,
    Duration,
    CfnOutput,
    Fn,
    aws_s3_notifications,
)
from pydantic_settings import BaseSettings


import aws_cdk
from aws_cdk.aws_apigatewayv2 import CfnStage
from aws_cdk.aws_apigatewayv2_alpha import HttpApi, HttpMethod
from aws_cdk.aws_apigatewayv2_integrations_alpha import HttpLambdaIntegration
from aws_cdk.aws_lambda_event_sources import SqsEventSource


from constructs import Construct
from stacks.utils import (
    stringify_settings,
    create_dependencies_layer,
)


class LambdaApiStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        env: Environment,
        env_name: str,
        lambda_env: BaseSettings,
        api_lambda_memory_size: int,
        api_lambda_timeout: int,
        vpc_id: Union[str, None],
        redis_port: int,
        redis_security_group_id: str,
        **kwargs,
    ) -> None:
        """Lambda to handle api requests"""
        super().__init__(scope, id, env=env, **kwargs)

        if vpc_id is None:
            vpc = None
            lambda_sec_group = None
        else:
            vpc = aws_ec2.Vpc.from_lookup(self, f"{id}-vpc", vpc_id=vpc_id)

            lambda_sec_group = aws_ec2.SecurityGroup(
                self,
                f"openaq-api-lambda-sec-group_{env_name}",
                security_group_name=f"openaq-api-lambda-sec-group_{env_name}",
                vpc=vpc,
                allow_all_outbound=True,
            )

        lambda_env = stringify_settings(lambda_env)

        security_groups = None
        if lambda_sec_group:
            security_groups = [lambda_sec_group]

        openaq_api = aws_lambda.Function(
            self,
            f"openaq-api-{env_name}-lambda",
            code=aws_lambda.Code.from_asset(
                path="../openaq_api",
                exclude=[
                    "venv",
                    "__pycache__",
                    "pytest_cache",
                ],
            ),
            handler="openaq_api.main.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            architecture=aws_lambda.Architecture.X86_64,
            vpc=vpc,
            allow_public_subnet=True,
            memory_size=api_lambda_memory_size,
            environment=lambda_env,
            security_groups=security_groups,
            timeout=Duration.seconds(api_lambda_timeout),
            layers=[
                create_dependencies_layer(
                    self,
                    f"{env_name}",
                    "api",
                    Path("../openaq_api/requirements.txt"),
                ),
            ],
        )

        openaq_api.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["ses:SendEmail", "SES:SendRawEmail"],
                resources=["*"],
                effect=aws_iam.Effect.ALLOW,
            )
        )


        if redis_security_group_id:
            redis_security_group = aws_ec2.SecurityGroup.from_security_group_id(
                self,
                "SG",
                redis_security_group_id,
                mutable=False
            )

            redis_security_group.add_ingress_rule(
                peer=lambda_sec_group,
                description="Allow Redis connection",
                connection=aws_ec2.Port.tcp(redis_port),
            )

        api = HttpApi(
            self,
            f"{id}-endpoint",
            create_default_stage=True,
            default_integration=HttpLambdaIntegration(
                f"openaq-api-integration-{env_name}",
                openaq_api,
            ),
            cors_preflight={
                "allow_headers": [
                    "*",
                ],
                "allow_methods": [
                    HttpMethod.POST,
                    HttpMethod.GET,
                    HttpMethod.HEAD,
                    HttpMethod.OPTIONS,
                ],
                "allow_origins": ["*"],
                "max_age": Duration.days(10),
            },
        )

        # When you dont include a default stage the api object does not include the url
        # However, the urls are all standard based on the api_id and the region
        api_url = f"https://{api.http_api_id}.execute-api.{self.region}.amazonaws.com"
        # TODO setup origin header to prevent traffic to API gateway directly
        CfnOutput(self, "Endpoint", value=api_url)
