from pathlib import Path
from typing import List


from aws_cdk import (
    Environment,
    RemovalPolicy,
    aws_ec2,
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
    aws_route53 as route53,
    aws_route53_targets as targets,
    aws_certificatemanager as acm,
    aws_cloudfront_origins as origins,
    aws_cloudfront as cloudfront,
)
from pydantic_settings import BaseSettings


import aws_cdk
from aws_cdk.aws_wafv2 import CfnWebACL
from aws_cdk.aws_apigatewayv2 import HttpApi
from aws_cdk.aws_apigatewayv2_integrations import HttpLambdaIntegration
from aws_cdk.aws_lambda_event_sources import SqsEventSource


from constructs import Construct

from stacks.utils import (
    stringify_settings,
    create_dependencies_layer,
)

from stacks.waf_rules import (
    custom_response_bodies,
    amazon_ip_reputation_list,
    known_bad_inputs_rule_set,
    api_key_header_rule,
    retired_endpoints_rule,
    ip_rate_limiter,
    ip_block_rule,
)


def create_waf(
    stack: Construct,
    limit: int,
    evaluation_window_sec: int,
    block_ips: List[str] | None,
) -> CfnWebACL:
    rules = [
        amazon_ip_reputation_list,
        known_bad_inputs_rule_set,
        api_key_header_rule,
        retired_endpoints_rule,
        ip_rate_limiter(limit, evaluation_window_sec),
    ]
    if block_ips:
        rules.append(ip_block_rule(stack, block_ips))
    waf = CfnWebACL(
        stack,
        "OpenAQAPICloudFrontWebACL",
        default_action=CfnWebACL.DefaultActionProperty(allow={}),
        scope="CLOUDFRONT",
        visibility_config=CfnWebACL.VisibilityConfigProperty(
            cloud_watch_metrics_enabled=True,
            metric_name="API-WAF",
            sampled_requests_enabled=True,
        ),
        rules=rules,
        custom_response_bodies=custom_response_bodies,
    )
    return waf


class LambdaApiStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        env: Environment,
        env_name: str,
        lambda_env: BaseSettings,
        cloudfront_logs_lambda_env: BaseSettings,
        api_lambda_memory_size: int,
        api_lambda_timeout: int,
        vpc_id: str | None,
        redis_port: int,
        redis_security_group_id: str,
        cf_logs_lambda_memory_size: int | None,
        cf_logs_lambda_timeout: int | None,
        hosted_zone_name: str | None,
        hosted_zone_id: str | None,
        domain_name: str | None,
        cert_arn: str | None,
        waf_evaluation_window_sec: int | None,
        waf_rate_limit: int | None,
        waf_block_ips: List[str] | None,
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
                path="../api",
                exclude=[
                    "venv",
                    "__pycache__",
                    "pytest_cache",
                ],
            ),
            handler="main.handler",
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
                    aws_lambda.Runtime.PYTHON_3_11,
                ),
            ],
        )

        add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["ses:SendEmail", "SES:SendRawEmail"],
                resources=["*"],
                effect=aws_iam.Effect.ALLOW,
            )
        )

        print(f"{redis_security_group_id} - {redis_port}")
        if redis_security_group_id:
            redis_security_group = aws_ec2.SecurityGroup.from_lookup_by_id(
                self,
                "SG",
                security_group_id=redis_security_group_id,
            )
            print(f"{redis_security_group.security_group_id} {redis_port}")
            redis_security_group.add_ingress_rule(
                peer=lambda_sec_group,
                description="Allow Redis connection",
                connection=aws_ec2.Port.tcp(6379),
                remote_rule=True,
            )

        api = HttpApi(
            self,
            f"{id}-endpoint",
            create_default_stage=True,
            default_integration=HttpLambdaIntegration(
                f"openaq-api-integration-{env_name}",
                openaq_api,
            ),
        )

        # When you dont include a default stage the api object does not include the url
        # However, the urls are all standard based on the api_id and the region
        api_url = f"https://{api.http_api_id}.execute-api.{self.region}.amazonaws.com"
        # TODO setup origin header to prevent traffic to API gateway directly
        CfnOutput(self, "Endpoint", value=api_url)

        if domain_name and cert_arn and hosted_zone_id and hosted_zone_name:
            hosted_zone = route53.HostedZone.from_hosted_zone_attributes(
                self,
                f"openaq-api-hosted-zone-{env_name}",
                hosted_zone_id=hosted_zone_id,
                zone_name=hosted_zone_name,
            )

            cert = acm.Certificate.from_certificate_arn(
                self, f"openaq-api-cert-{env_name}", cert_arn
            )

            log_bucket = aws_s3.Bucket(
                self,
                f"openaq-api-cf-dist-log-{env_name}",
                bucket_name=f"openaq-api-cf-dist-log-{env_name}",
                auto_delete_objects=False,
                public_read_access=False,
                removal_policy=RemovalPolicy.DESTROY,
                object_ownership=aws_s3.ObjectOwnership.OBJECT_WRITER,
                lifecycle_rules=[
                    aws_s3.LifecycleRule(
                        id=f"openaq-api-cf-dist-log-lifecycle-rule-{env_name}",
                        enabled=True,
                        expiration=aws_cdk.Duration.days(7),
                    )
                ],
            )

            log_event_queue = aws_sqs.Queue(
                self,
                f"openaq-api-cf-log-event-queue-{env_name}",
                visibility_timeout=Duration.seconds(cf_logs_lambda_timeout),
            )

            log_bucket.add_event_notification(
                aws_s3.EventType.OBJECT_CREATED_PUT,
                aws_s3_notifications.SqsDestination(log_event_queue),
            )

            cloudfront_access_log_group = aws_logs.LogGroup(
                self,
                f"openaq-api-{env_name}-cf-access-log",
                retention=aws_logs.RetentionDays.ONE_YEAR,
            )

            cloudfront_access_log_group.add_stream(
                f"openaq-api-{env_name}-cf-access-log-stream",
                log_stream_name=f"openaq-api-{env_name}-cf-access-log-stream",
            )

            log_lambda = aws_lambda.Function(
                self,
                f"openaq-api-cloudfront-logs-{env_name}-lambda",
                code=aws_lambda.Code.from_asset(
                    path="../cloudfront_logs",
                    exclude=[
                        "venv",
                        "__pycache__",
                        "pytest_cache",
                    ],
                ),
                handler="cloudfront_logs.main.handler",
                runtime=aws_lambda.Runtime.PYTHON_3_11,
                allow_public_subnet=True,
                memory_size=cf_logs_lambda_memory_size,
                environment=stringify_settings(cloudfront_logs_lambda_env),
                timeout=Duration.seconds(cf_logs_lambda_timeout),
                layers=[
                    create_dependencies_layer(
                        self,
                        f"{env_name}",
                        "cloudfront_logs",
                        aws_lambda.Runtime.PYTHON_3_11,
                    ),
                ],
            )

            log_lambda.add_event_source(
                SqsEventSource(
                    log_event_queue,
                    batch_size=10,
                    max_batching_window=Duration.minutes(1),
                    report_batch_item_failures=True,
                )
            )

            log_bucket.grant_read(log_lambda)

            origin_url = Fn.select(
                2, Fn.split("/", api_url)
            )  # required to split url into compatible format for dist

            dist_origin_request_policy = cloudfront.OriginRequestPolicy(
                self,
                f"OpenAQAPIDistOriginRequestPolicy-{env_name}",
                origin_request_policy_name=f"OpenAQAPIDistOriginRequestPolicy_{env_name}",
                header_behavior=cloudfront.OriginRequestHeaderBehavior.allow_list(
                    "x-api-key", "X-API-Key", "User-Agent"
                ),
                query_string_behavior=cloudfront.OriginRequestQueryStringBehavior.all(),
            )

            waf = create_waf(
                self, waf_rate_limit, waf_evaluation_window_sec, waf_block_ips
            )

            dist = cloudfront.Distribution(
                self,
                f"openaq-api-dist-{env_name}",
                http_version=cloudfront.HttpVersion.HTTP2_AND_3,
                price_class=cloudfront.PriceClass.PRICE_CLASS_ALL,
                default_behavior=cloudfront.BehaviorOptions(
                    origin=origins.HttpOrigin(origin_url),
                    allowed_methods=cloudfront.AllowedMethods.ALLOW_ALL,
                    compress=True,
                    cache_policy=cloudfront.CachePolicy.CACHING_DISABLED,
                    viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                    origin_request_policy=dist_origin_request_policy,
                ),
                domain_names=[domain_name],
                certificate=cert,
                web_acl_id=waf.attr_arn,
                enable_logging=True,
                log_bucket=log_bucket,
            )

            route53.ARecord(
                self,
                f"openaq-api-alias-record-{env_name}",
                zone=hosted_zone,
                record_name=domain_name,
                target=route53.RecordTarget.from_alias(targets.CloudFrontTarget(dist)),
            )

            CfnOutput(self, "Dist", value=dist.distribution_domain_name)
        else:
            print(
                f"""
            Could not add domain: {domain_name}
            cert: {cert_arn}
            zone_id: {hosted_zone_id}
            zone_name: {hosted_zone_name}
            """
            )
