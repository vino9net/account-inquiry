"""Account Inquiry stack: DynamoDB projection + AppSync API, fed by a Kinesis Data
Stream of transfer events.

    Kinesis (transfers) --event source mapping--> Lambda (ingest) --TransactWriteItems--> DynamoDB
                                                                                              |
                                                                    AppSync (IAM auth) --------+
                                                                    JS resolvers, direct DDB DS

Table design (see ingest/ddb.py for the write side):

    Account item:      id=customer_id (N)   sid="ACC_<account_id>" (S)
    Transaction item:  id=account_id  (N)   sid="TRX_<transfer_id>" (S)

Both item types share the table so "all accounts for a customer" and "all transactions
for an account" are each a single Query, no GSI needed — the partition key just means a
different thing depending on which item type you land on.
"""

from __future__ import annotations

import os.path
from pathlib import Path
from typing import Any

from aws_cdk import BundlingOptions, CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_appsync as appsync
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kinesis as kinesis
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_lambda_event_sources as lambda_event_sources
from aws_cdk import aws_logs as logs
from aws_cdk import aws_sqs as sqs
from constructs import Construct

from account_inquiry.bundling import PipLocalBundling
from account_inquiry.config import DeploySettings

_SRC_DIR = os.path.abspath(os.path.dirname(__file__))
# The Lambda handler is addressed as `account_inquiry.ingest.handler.lambda_handler`,
# so the asset root must be `src/` (the parent of the account_inquiry package) — not
# `_SRC_DIR` itself — or the zip won't have `account_inquiry/` as a top-level package.
_SRC_PARENT_DIR = os.path.dirname(_SRC_DIR)
_INGEST_REQUIREMENTS = ["boto3>=1.34", "aws-lambda-powertools>=3.0"]


class AccountInquiryStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, settings: DeploySettings, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.settings = settings

        table = self._table()
        stream = self._kinesis_stream()
        ingest_fn = self._ingest_lambda(table, stream)
        api = self._appsync_api(table)

        if settings.ci_principal_arn:
            # An existing (not stack-managed) IAM user/role — e.g. the CI credential
            # GitHub Actions deploys with — imported by ARN so CDK can attach an inline
            # policy to it. See grant_query() below for why this is needed at all:
            # AppSync's IAM auth checks the caller's own identity policy, not a
            # resource policy on the API.
            ci_principal = iam.User.from_user_arn(
                self, "CiTestPrincipal", settings.ci_principal_arn
            )
            self.grant_query(ci_principal)

        CfnOutput(self, "AccountsTableName", value=table.table_name)
        CfnOutput(self, "TransfersStreamName", value=stream.stream_name)
        CfnOutput(self, "AccountInquiryApiUrl", value=api.graphql_url)
        CfnOutput(self, "IngestFunctionName", value=ingest_fn.function_name)

    # ------------------------------------------------------------------ storage

    def _table(self) -> dynamodb.TableV2:
        removal = RemovalPolicy.RETAIN if self.settings.retain_data else RemovalPolicy.DESTROY
        return dynamodb.TableV2(
            self,
            "AccountsTable",
            partition_key=dynamodb.Attribute(name="id", type=dynamodb.AttributeType.NUMBER),
            sort_key=dynamodb.Attribute(name="sid", type=dynamodb.AttributeType.STRING),
            billing=dynamodb.Billing.on_demand(),
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=self.settings.retain_data
            ),
            deletion_protection=self.settings.retain_data,
            removal_policy=removal,
        )

    def _kinesis_stream(self) -> kinesis.Stream:
        removal = RemovalPolicy.RETAIN if self.settings.retain_data else RemovalPolicy.DESTROY
        return kinesis.Stream(
            self,
            "TransfersStream",
            stream_name=self.settings.kinesis_stream_name,
            shard_count=self.settings.kinesis_shard_count,
            retention_period=Duration.hours(24),
            removal_policy=removal,
        )

    # ------------------------------------------------------------------ ingest lambda

    def _ingest_lambda(self, table: dynamodb.TableV2, stream: kinesis.Stream) -> _lambda.Function:
        dlq = sqs.Queue(
            self,
            "IngestDlq",
            retention_period=Duration.days(14),
            removal_policy=RemovalPolicy.DESTROY,
        )

        # boto3/botocore already ship in the managed runtime, but we pin our own copy
        # for TransactWriteItems/exception behaviour instead of trusting whatever the
        # runtime happens to bundle. aws-lambda-powertools does NOT ship in the runtime
        # and has no stable public layer ARN worth hardcoding here (varies by region,
        # version, and architecture) — bundling it in is the portable choice.
        #
        # Tries pip on the host first (see bundling.py — safe because both deps are
        # pure Python); falls back to Docker only if that's not available.
        function_name = f"{self.stack_name}-ingest"
        log_group = logs.LogGroup(
            self,
            "IngestFunctionLogGroup",
            log_group_name=f"/aws/lambda/{function_name}",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        # jsii's generated Protocol stubs for ILocalBundling/IEventSourceDlq don't
        # structurally match their own documented/runtime signatures — a known
        # jsii-stub limitation (confirmed via runtime testing), not a real type error.
        local_bundling: Any = PipLocalBundling(Path(_SRC_DIR), _INGEST_REQUIREMENTS)

        fn = _lambda.Function(
            self,
            "IngestFunction",
            function_name=function_name,
            runtime=_lambda.Runtime.PYTHON_3_13,
            architecture=_lambda.Architecture.ARM_64,
            handler="account_inquiry.ingest.handler.lambda_handler",
            code=_lambda.Code.from_asset(
                _SRC_PARENT_DIR,
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_13.bundling_image,
                    local=local_bundling,
                    command=[
                        "bash",
                        "-c",
                        "pip install "
                        + " ".join(_INGEST_REQUIREMENTS)
                        + " --target /asset-output/"
                        " && mkdir -p /asset-output/account_inquiry"
                        " && cp account_inquiry/__init__.py /asset-output/account_inquiry/"
                        " && cp -r account_inquiry/ingest /asset-output/account_inquiry/",
                    ],
                ),
            ),
            memory_size=512,
            timeout=Duration.seconds(30),
            log_group=log_group,
            environment={
                "DDB_TABLE_NAME": table.table_name,
                "DEPLOY_ENV": self.settings.deploy_env,
                "POWERTOOLS_SERVICE_NAME": "account-inquiry-ingest",
                # namespace is set explicitly in handler.py, not via env var — see the
                # comment there for why.
            },
        )

        table.grant_write_data(fn)
        # The account-not-found guard in ddb.py needs read on the account keys too, but
        # TransactWriteItems only ever writes — no separate grant_read_data needed.

        dlq_destination: Any = lambda_event_sources.SqsDlq(dlq)
        fn.add_event_source(
            lambda_event_sources.KinesisEventSource(
                stream,
                starting_position=_lambda.StartingPosition.TRIM_HORIZON,
                batch_size=1000,
                # Whole seconds only (CFN/Kinesis constraint) — 1s caps the low-traffic
                # tail latency; a full batch of 1000 still triggers immediately.
                max_batching_window=Duration.seconds(1),
                retry_attempts=3,
                bisect_batch_on_error=True,
                report_batch_item_failures=True,
                on_failure=dlq_destination,
                parallelization_factor=1,  # raise once measured; see ARCH_DESIGN.md D7 lesson
            )
        )

        return fn

    # ------------------------------------------------------------------ appsync api

    def _appsync_api(self, table: dynamodb.TableV2) -> appsync.GraphqlApi:
        api = appsync.GraphqlApi(
            self,
            "Api",
            name="account-inquiry",
            definition=appsync.Definition.from_file(f"{_SRC_DIR}/schema.graphql"),
            authorization_config=appsync.AuthorizationConfig(
                default_authorization=appsync.AuthorizationMode(
                    authorization_type=appsync.AuthorizationType.IAM
                )
            ),
            xray_enabled=True,
            log_config=appsync.LogConfig(field_log_level=appsync.FieldLogLevel.ALL),
        )
        if not self.settings.retain_data:
            api.apply_removal_policy(RemovalPolicy.DESTROY)

        ds = api.add_dynamo_db_data_source("AccountsDataSource", table)

        ds.create_resolver(
            "GetAccountsForCustomerResolver",
            type_name="Query",
            field_name="getAccountsForCustomer",
            code=appsync.Code.from_asset(f"{_SRC_DIR}/resolvers/get_accounts_for_customer.js"),
            runtime=appsync.FunctionRuntime.JS_1_0_0,
        )
        ds.create_resolver(
            "GetTransactionsForAccountResolver",
            type_name="Query",
            field_name="getTransactionsForAccount",
            code=appsync.Code.from_asset(f"{_SRC_DIR}/resolvers/get_transactions_for_account.js"),
            runtime=appsync.FunctionRuntime.JS_1_0_0,
        )

        return api

    def grant_query(self, principal: iam.IGrantable) -> None:
        """Convenience for whoever wires up a caller (mobile BFF, integration tests)."""
        iam.Grant.add_to_principal(
            grantee=principal,
            actions=["appsync:GraphQL"],
            resource_arns=[f"arn:{self.partition}:appsync:{self.region}:{self.account}:*"],
        )
