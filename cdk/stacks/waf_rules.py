from typing import List
from aws_cdk.aws_wafv2 import CfnWebACL, CfnIPSet
from constructs import Construct


custom_response_bodies = {
    "ForbiddenMessage": CfnWebACL.CustomResponseBodyProperty(
        content='{"message": "Forbidden. Violation of rate limit and excessive requests"}',
        content_type="APPLICATION_JSON",
    ),
    "UnauthorizedMessage": CfnWebACL.CustomResponseBodyProperty(
        content='{"message": "Unauthorized. A valid API key must be provided in the X-API-Key header."}',
        content_type="APPLICATION_JSON",
    ),
    "GoneMessage": CfnWebACL.CustomResponseBodyProperty(
        content='{"message": "Gone. Version 1 and Version 2 API endpoints are retired and no longer available. Please migrate to Version 3 endpoints."}',
        content_type="APPLICATION_JSON",
    ),
}

amazon_ip_reputation_list = CfnWebACL.RuleProperty(
    name="AWS-AWSManagedRulesAmazonIpReputationList",
    priority=0,
    statement=CfnWebACL.StatementProperty(
        managed_rule_group_statement=CfnWebACL.ManagedRuleGroupStatementProperty(
            vendor_name="AWS", name="AWSManagedRulesAmazonIpReputationList"
        )
    ),
    override_action=CfnWebACL.OverrideActionProperty(none={}),
    visibility_config=CfnWebACL.VisibilityConfigProperty(
        sampled_requests_enabled=True,
        cloud_watch_metrics_enabled=True,
        metric_name="AWS-AWSManagedRulesAmazonIpReputationList",
    ),
)

known_bad_inputs_rule_set = CfnWebACL.RuleProperty(
    name="AWS-AWSManagedRulesKnownBadInputsRuleSet",
    priority=1,
    statement=CfnWebACL.StatementProperty(
        managed_rule_group_statement=CfnWebACL.ManagedRuleGroupStatementProperty(
            vendor_name="AWS", name="AWSManagedRulesKnownBadInputsRuleSet"
        )
    ),
    override_action=CfnWebACL.OverrideActionProperty(none={}),
    visibility_config=CfnWebACL.VisibilityConfigProperty(
        sampled_requests_enabled=True,
        cloud_watch_metrics_enabled=True,
        metric_name="AWS-AWSManagedRulesKnownBadInputsRuleSet",
    ),
)

api_key_header_rule = CfnWebACL.RuleProperty(
    name="CheckXApiKeyHeader",
    priority=2,
    action=CfnWebACL.RuleActionProperty(
        block=CfnWebACL.BlockActionProperty(
            custom_response=CfnWebACL.CustomResponseProperty(
                response_code=401, custom_response_body_key="UnauthorizedMessage"
            )
        )
    ),
    statement=CfnWebACL.StatementProperty(
        and_statement=CfnWebACL.AndStatementProperty(
            statements=[
                CfnWebACL.StatementProperty(
                    not_statement=CfnWebACL.NotStatementProperty(
                        statement=CfnWebACL.StatementProperty(
                            size_constraint_statement=CfnWebACL.SizeConstraintStatementProperty(
                                field_to_match=CfnWebACL.FieldToMatchProperty(
                                    single_header={"Name": "x-api-key"}
                                ),
                                comparison_operator="GT",
                                size=0,
                                text_transformations=[
                                    CfnWebACL.TextTransformationProperty(
                                        priority=0, type="NONE"
                                    )
                                ],
                            )
                        )
                    )
                ),
                CfnWebACL.StatementProperty(
                    byte_match_statement=CfnWebACL.ByteMatchStatementProperty(
                        search_string="/v3/",
                        field_to_match=CfnWebACL.FieldToMatchProperty(uri_path={}),
                        text_transformations=[
                            CfnWebACL.TextTransformationProperty(
                                priority=0, type="NONE"
                            )
                        ],
                        positional_constraint="CONTAINS",
                    )
                ),
            ]
        )
    ),
    visibility_config=CfnWebACL.VisibilityConfigProperty(
        sampled_requests_enabled=True,
        cloud_watch_metrics_enabled=True,
        metric_name="CheckXApiKeyHeader",
    ),
)

retired_endpoints_rule = CfnWebACL.RuleProperty(
    name="retiredVersionsEndpoints",
    priority=3,
    action=CfnWebACL.RuleActionProperty(
        block=CfnWebACL.BlockActionProperty(
            custom_response=CfnWebACL.CustomResponseProperty(
                response_code=410, custom_response_body_key="GoneMessage"
            )
        )
    ),
    statement=CfnWebACL.StatementProperty(
        or_statement=CfnWebACL.OrStatementProperty(
            statements=[
                CfnWebACL.StatementProperty(
                    byte_match_statement=CfnWebACL.ByteMatchStatementProperty(
                        search_string="/v1/",
                        field_to_match=CfnWebACL.FieldToMatchProperty(uri_path={}),
                        text_transformations=[
                            CfnWebACL.TextTransformationProperty(
                                priority=0, type="NONE"
                            )
                        ],
                        positional_constraint="CONTAINS",
                    )
                ),
                CfnWebACL.StatementProperty(
                    byte_match_statement=CfnWebACL.ByteMatchStatementProperty(
                        search_string="/v2/",
                        field_to_match=CfnWebACL.FieldToMatchProperty(uri_path={}),
                        text_transformations=[
                            CfnWebACL.TextTransformationProperty(
                                priority=0, type="NONE"
                            )
                        ],
                        positional_constraint="CONTAINS",
                    )
                ),
            ]
        )
    ),
    visibility_config=CfnWebACL.VisibilityConfigProperty(
        sampled_requests_enabled=True,
        cloud_watch_metrics_enabled=True,
        metric_name="retiredVersionsEndpoints",
    ),
)


def ip_rate_limiter(
    limit: int, evaluation_window_sec: int = 60
) -> CfnWebACL.RuleProperty:
    return CfnWebACL.RuleProperty(
        name="IPRateLimiter",
        priority=4,
        statement=CfnWebACL.StatementProperty(
            rate_based_statement=CfnWebACL.RateBasedStatementProperty(
                aggregate_key_type="IP",
                evaluation_window_sec=evaluation_window_sec,
                limit=limit,
            )
        ),
        action=CfnWebACL.RuleActionProperty(block={}),
        visibility_config=CfnWebACL.VisibilityConfigProperty(
            sampled_requests_enabled=True,
            cloud_watch_metrics_enabled=True,
            metric_name="IpRateLimiter",
        ),
    )


def ip_block_rule(stack: Construct, ips: List[str]) -> CfnWebACL.RuleProperty:
    ip_set = CfnIPSet(
        stack,
        "waf_ip_block_set",
        addresses=ips,
        ip_address_version="IPV4",
        scope="CLOUDFRONT",
        description="Set of IPs to specifically block to prevent abuse",
        name="IP block list",
    )

    return CfnWebACL.RuleProperty(
        name="IpBlockRule",
        priority=4,
        statement=CfnWebACL.StatementProperty(
            ip_set_reference_statement=CfnWebACL.IPSetReferenceStatementProperty(
                arn=ip_set.attr_arn
            )
        ),
        action=CfnWebACL.RuleActionProperty(
            block=CfnWebACL.BlockActionProperty(
                custom_response=CfnWebACL.CustomResponseProperty(
                    response_code=410, custom_response_body_key="ForbiddenMessage"
                )
            )
        ),
        visibility_config=CfnWebACL.VisibilityConfigProperty(
            sampled_requests_enabled=True,
            cloud_watch_metrics_enabled=True,
            metric_name="IpBlockRule",
        ),
    )
