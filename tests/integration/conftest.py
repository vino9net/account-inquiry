"""Fixtures for integration tests that run against a real deployed AccountInquiryStack.

Connection info comes from the stack's CloudFormation outputs (see `TESTING_STACK_NAME`
below) rather than hardcoded names/URLs, so the same tests run unmodified against either
the ephemeral per-PR stack or the persistent staging stack — whichever one
`TESTING_STACK_NAME` points at for a given CI job.
"""

from __future__ import annotations

import os
from typing import Any

import boto3
import pytest
import requests
from botocore.exceptions import ClientError
from requests_aws4auth import AWS4Auth

_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-west-2")
_STACK_NAME = os.environ.get("TESTING_STACK_NAME", "AccountInquiryStack")

_outputs: dict[str, str] = {}


def _stack_output(key: str) -> str:
    """Cached CloudFormation stack output lookup — one describe_stacks call per test
    session, not per fixture."""
    global _outputs
    if not _outputs:
        client = boto3.client("cloudformation", region_name=_REGION)
        try:
            response = client.describe_stacks(StackName=_STACK_NAME)
        except ClientError as e:
            raise Exception(f"cannot find stack {_STACK_NAME!r} in {_REGION}") from e
        _outputs = {o["OutputKey"]: o["OutputValue"] for o in response["Stacks"][0]["Outputs"]}

    try:
        return _outputs[key]
    except KeyError:
        raise Exception(
            f"stack {_STACK_NAME!r} has no output {key!r} (has: {sorted(_outputs)})"
        ) from None


@pytest.fixture(scope="session")
def api_url() -> str:
    return _stack_output("AccountInquiryApiUrl")


@pytest.fixture(scope="session")
def table_name() -> str:
    return _stack_output("AccountsTableName")


@pytest.fixture(scope="session")
def stream_name() -> str:
    return _stack_output("TransfersStreamName")


@pytest.fixture(scope="session")
def api_auth() -> AWS4Auth:
    session = boto3.Session()
    credentials = session.get_credentials()
    return AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        _REGION,
        "appsync",
        session_token=credentials.token,
    )


@pytest.fixture(scope="session")
def graphql_query(api_url, api_auth):
    """Factory fixture: returns a callable that runs one signed GraphQL query/mutation
    against the deployed API and returns the parsed JSON response."""

    def _run(query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        response = requests.post(
            api_url,
            json={"query": query, "variables": variables or {}},
            auth=api_auth,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    return _run


@pytest.fixture
def ddb_table(table_name: str):
    return boto3.resource("dynamodb", region_name=_REGION).Table(table_name)


@pytest.fixture
def kinesis_client():
    return boto3.client("kinesis", region_name=_REGION)
