from pathlib import Path
from typing import Dict, List, Union


from aws_cdk import (
    Environment,
    RemovalPolicy,
    aws_ec2,
    aws_s3,
    aws_sqs,
    aws_lambda,
    aws_logs,
    Stack,
    Duration,
    CfnOutput,
    Fn,
    aws_s3_notifications,
    aws_route53 as route53,
    aws_route53_targets as targets,
    aws_certificatemanager as acm,
    aws_cloudfront_origins as origins,
    aws_cloudfront as cloudfront
)
import aws_cdk
from aws_cdk.aws_apigatewayv2 import CfnStage
from aws_cdk.aws_apigatewayv2_alpha import HttpApi, HttpMethod
from aws_cdk.aws_apigatewayv2_integrations_alpha import HttpLambdaIntegration
from aws_cdk.aws_lambda_event_sources import SqsEventSource


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
        env: Environment,
        env_name: str,
        lambda_env: Dict,
        cloudfront_logs_lambda_env: Dict,
        api_lambda_memory_size: int,
        api_lambda_timeout: int,
        vpc_id: str,
        cf_logs_lambda_memory_size: Union[int, None],
        cf_logs_lambda_timeout: Union[int, None],
        hosted_zone_name: Union[str, None],
        hosted_zone_id: Union[str, None],
        domain_name: Union[str, None],
        cert_arn: Union[str, None],
        web_acl_id: Union[str, None],
        **kwargs,
    ) -> None:
        """Lambda to handle api requests"""
        super().__init__(scope, id, env=env, **kwargs)

        vpc = aws_ec2.Vpc.from_lookup(
            self,
            f"{id}-vpc",
            vpc_id=vpc_id
        )

        openaq_api = aws_lambda.Function(
            self,
            f"openaq-api-{env_name}-lambda",
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
            vpc=vpc,
            allow_public_subnet=True,
            memory_size=api_lambda_memory_size,
            environment=stringify_settings(lambda_env),
            timeout=Duration.seconds(api_lambda_timeout),
            layers=[
                create_dependencies_layer(self, f"{env_name}", 'api', Path('../openaq_fastapi/requirements.txt')),
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
                query_string_behavior=cloudfront.CacheQueryStringBehavior.all(),
                enable_accept_encoding_gzip=True,
                enable_accept_encoding_brotli=True
            )


            log_bucket = aws_s3.Bucket(self, f"openaq-api-dist-log-{env_name}",
                bucket_name=f"openaq-api-dist-log-{env_name}",
                auto_delete_objects=True,
                public_read_access=False,
                removal_policy=RemovalPolicy.DESTROY,
                lifecycle_rules=[
                    aws_s3.LifecycleRule(
                        id=f"openaq-api-dist-log-lifecycle-rule-{env_name}",
                        enabled=True,
                        expiration=aws_cdk.Duration.days(7)
                    )
                ]
            )

            log_event_queue = aws_sqs.Queue(self, f"openaq-api-cf-log-event-queue-{env_name}")

            log_bucket.add_event_notification(
                aws_s3.EventType.OBJECT_CREATED_PUT, 
                aws_s3_notifications.SqsDestination(log_event_queue)
            )

            cloudfront_access_log_group = aws_logs.LogGroup(
                self,
                f"openaq-api-{env_name}-cf-access-log",
                log_group_name = f"openaq-api-{env_name}-cf-access-log",
                retention=aws_logs.RetentionDays.ONE_YEAR
            )

            cloudfront_access_log_group.add_stream(
                f"openaq-api-{env_name}-cf-access-log-stream",
                log_stream_name=f"openaq-api-{env_name}-cf-access-log-stream",
            )

            log_lambda = aws_lambda.Function(
                self,
                f"openaq-api-cloudfront-logs-{env_name}-lambda",
                code=aws_lambda.Code.from_asset(
                    path='../cloudfront_logs',
                    exclude=[
                        'venv',
                        '__pycache__',
                        'pytest_cache',
                    ],
                ),
                handler="cloudfront_logs.main.handler",
                runtime=aws_lambda.Runtime.PYTHON_3_8,
                allow_public_subnet=True,
                memory_size=cf_logs_lambda_memory_size,
                environment=stringify_settings(cloudfront_logs_lambda_env),
                timeout=Duration.seconds(cf_logs_lambda_timeout),
                layers=[
                    create_dependencies_layer(self, f"{env_name}", 'cloudfront_logs', Path('../cloudfront_logs/requirements.txt')),
                ],
            )

            log_lambda.add_event_source(SqsEventSource(log_event_queue,
                batch_size=10,
                max_batching_window=Duration.minutes(1),
                report_batch_item_failures=True
            ))

            log_bucket.grant_read(log_lambda)

            origin_url = Fn.select(2, Fn.split("/", api_url)) # required to split url into compatible format for dist

            dist = cloudfront.Distribution(self, f"openaq-api-dist-{env_name}",
                default_behavior=cloudfront.BehaviorOptions(
                    origin=origins.HttpOrigin(
                        origin_url
                    ),
                    allowed_methods=cloudfront.AllowedMethods.ALLOW_GET_HEAD_OPTIONS,
                    compress=True,
                    cache_policy=cache_policy,
                    viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS
                ),
                domain_names=[domain_name],
                certificate=cert,
                web_acl_id=web_acl_id,
                enable_logging=True,
                log_bucket=log_bucket
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
