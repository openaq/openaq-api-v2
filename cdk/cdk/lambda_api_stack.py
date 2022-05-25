from typing import Dict, Optional


from aws_cdk import (
    aws_lambda,
    aws_logs,
    Stack,
    Duration,
    CfnOutput,
    Fn,
    aws_route53 as route53,
    aws_route53_targets as targets,
    aws_certificatemanager as acm,
    aws_cloudfront_origins as origins,
    aws_cloudfront as cloudfront
)
from aws_cdk.aws_apigatewayv2 import CfnStage
from aws_cdk.aws_apigatewayv2_alpha import HttpApi, HttpMethod
from aws_cdk.aws_apigatewayv2_integrations_alpha import HttpLambdaIntegration


from constructs import Construct
from cdk.utils import (
    stringify_settings,
    create_dependencies_layer,
)


class LambdaApiStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        env_name: str,
        lambda_env: Dict,
        memory_size: int,
        lambda_timeout: int,
        hosted_zone_name: Optional[str],
        hosted_zone_id: Optional[str],
        domain_name: Optional[str],
        cert_arn: Optional[str],
        web_acl_id: Optional[str],
        **kwargs,
    ) -> None:
        """Lambda to handle api requests"""
        super().__init__(scope, id, *kwargs)

        openaq_api = aws_lambda.Function(
            self,
            f"{id}-lambda",
            code=aws_lambda.Code.from_asset(
                path='../openaq_fastapi',
                exclude=[
                    'venv',
                    '__pycache__',
                    'pytest_cache',
                ],
            ),
            handler="openaq_fastapi.main.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            allow_public_subnet=True,
            memory_size=memory_size,
            environment=stringify_settings(lambda_env),
            timeout=Duration.seconds(lambda_timeout),
            layers=[
                create_dependencies_layer(self, f"{env_name}", 'api'),
            ],
        )

        api = HttpApi(
            self,
            f"{id}-endpoint",
            create_default_stage=False,
            default_integration=HttpLambdaIntegration(
                f"openaq-api-integration-{env_name}",
                openaq_api,
            ),
            cors_preflight={
                "allow_headers": [
                    "*",
                ],
                "allow_methods": [
                    HttpMethod.GET,
                    HttpMethod.HEAD,
                    HttpMethod.OPTIONS,
                ],
                "allow_origins": ["*"],
                "max_age": Duration.days(10),
            },
        )

        log = aws_logs.LogGroup(
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

        # When you dont include a default stage the api object does not include the url
        # However, the urls are all standard based on the api_id and the region
        api_url = f'https://{api.http_api_id}.execute-api.{self.region}.amazonaws.com'
        # TODO setup origin header to prevent traffic to API gateway directly
        CfnOutput(self, "Endpoint", value=api_url)

        if domain_name and cert_arn and web_acl_id and hosted_zone_id and hosted_zone_name:

            hosted_zone = route53.HostedZone.from_hosted_zone_attributes(
                self,
                f"openaq-api-hosted-zone-{env_name}",
                hosted_zone_id=hosted_zone_id,
                zone_name=hosted_zone_name
            )

            cert = acm.Certificate.from_certificate_arn(self, "openaq-api-cert", cert_arn)

            cache_policy = cloudfront.CachePolicy(self, f"openaq-api-cache-policy-{env_name}",
                cache_policy_name=f"openaq-api-cache-policy-{env_name}",
                default_ttl=Duration.seconds(60),
                min_ttl=Duration.minutes(0),
                max_ttl=Duration.days(7),
                cookie_behavior=cloudfront.CacheCookieBehavior.none(),
                header_behavior=cloudfront.CacheHeaderBehavior.allow_list("Origin"),
                enable_accept_encoding_gzip=True,
                enable_accept_encoding_brotli=True
            )

            origin_url = Fn.select(2, Fn.split("/", api_url)) # required to split url into compatible format for dist

            dist = cloudfront.Distribution(self, f"openaq-api-dist-{env_name}",
                    default_behavior=cloudfront.BehaviorOptions(
                    origin=origins.HttpOrigin(
                        origin_url
                    ),
                    allowed_methods=cloudfront.AllowedMethods.ALLOW_GET_HEAD_OPTIONS,
                    compress=True,
                    cache_policy=cache_policy
                ),
                domain_names=[domain_name],
                certificate=cert,
                web_acl_id=web_acl_id,
                enable_logging=True
            )

            route53.ARecord(self, f"openaq-api-alias-record-{env_name}",
                            zone=hosted_zone,
                            record_name=domain_name,
                            target=route53.RecordTarget.from_alias(targets.CloudFrontTarget(dist))
                            )

            CfnOutput(self, "Dist", value=dist.distribution_domain_name)
        else:
            print(f"""
            Could not add domain: {domain_name}
            cert: {cert_arn}
            zone_id: {hosted_zone_id}
            zone_name: {hosted_zone_name}
            """)
