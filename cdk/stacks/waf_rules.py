from aws_cdk.aws_wafv2 import CfnWebACL

CfnWebACL.ManagedRuleGroupConfigProperty()
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

ip_rate_limiter = CfnWebACL.RuleProperty(
    name="IPRateLimiter",
    priority=4,
    statement=CfnWebACL.StatementProperty(
        rate_based_statement=CfnWebACL.RateBasedStatementProperty(
            aggregate_key_type="IP", evaluation_window_sec=300, limit=7500
        )
    ),
    action=CfnWebACL.RuleActionProperty(block={}),
    visibility_config=CfnWebACL.VisibilityConfigProperty(
        sampled_requests_enabled=True,
        cloud_watch_metrics_enabled=True,
        metric_name="IpRateLimiter",
    ),
)
