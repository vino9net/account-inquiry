import aws_cdk as cdk
from aws_cdk.assertions import Match, Template

from account_inquiry.config import DeploySettings
from account_inquiry.stack import AccountInquiryStack


def _synth(deploy_env: str = "feature") -> Template:
    app = cdk.App()
    stack = AccountInquiryStack(app, "TestStack", settings=DeploySettings(deploy_env=deploy_env))
    return Template.from_stack(stack)


def test_table_has_composite_key():
    template = _synth()
    template.has_resource_properties(
        "AWS::DynamoDB::GlobalTable",
        {
            "KeySchema": Match.array_with(
                [
                    {"AttributeName": "id", "KeyType": "HASH"},
                    {"AttributeName": "sid", "KeyType": "RANGE"},
                ]
            )
        },
    )


def test_feature_env_destroys_table_and_stream():
    template = _synth("feature")
    template.has_resource("AWS::DynamoDB::GlobalTable", {"DeletionPolicy": "Delete"})
    template.has_resource("AWS::Kinesis::Stream", {"DeletionPolicy": "Delete"})


def test_prod_env_retains_table_and_stream():
    template = _synth("prod")
    template.has_resource("AWS::DynamoDB::GlobalTable", {"DeletionPolicy": "Retain"})
    template.has_resource("AWS::Kinesis::Stream", {"DeletionPolicy": "Retain"})


def test_ingest_lambda_reports_batch_item_failures():
    template = _synth()
    template.has_resource_properties(
        "AWS::Lambda::EventSourceMapping",
        {
            "FunctionResponseTypes": ["ReportBatchItemFailures"],
            "BisectBatchOnFunctionError": True,
        },
    )


def test_appsync_uses_iam_auth():
    template = _synth()
    template.has_resource_properties(
        "AWS::AppSync::GraphQLApi",
        {"AuthenticationType": "AWS_IAM"},
    )
