#!/usr/bin/env python3
"""Manual smoke-test CLI for the deployed AppSync API.

The API only accepts SigV4-signed requests from an IAM principal explicitly granted
appsync:GraphQL on it (see stack.py's grant_query()/CI_IAM_PRINCIPAL_ARN) — there is no
API key auth mode. This signs with whatever AWS credentials are already active
(AWS_PROFILE, AWS_ACCESS_KEY_ID/SECRET, etc.), the same way tests/integration/conftest.py
does, so it only works for a principal that's actually been granted access.

Examples:
    uv run python scripts/query_appsync.py accounts 900001
    uv run python scripts/query_appsync.py transactions 900101
    uv run python scripts/query_appsync.py raw \
        'query { getAccountsForCustomer(customerId: "1") { id balance } }'

    # against a different stack (e.g. a PR's disposable one):
    uv run python scripts/query_appsync.py accounts 1 --stack-name AccountInquiryStack-pr-42
"""

from __future__ import annotations

import argparse
import json
import sys

import boto3
import requests
from requests_aws4auth import AWS4Auth

_ACCOUNTS_QUERY = """
query ($customerId: ID!) {
    getAccountsForCustomer(customerId: $customerId) {
        id
        accountId
        name
        currency
        balance
        availBalance
        status
        updatedAt
    }
}
"""

_TRANSACTIONS_QUERY = """
query ($accountId: ID!) {
    getTransactionsForAccount(accountId: $accountId) {
        accountId
        transferId
        type
        amount
        currency
        memo
        status
        processedAt
        updatedAt
    }
}
"""


def _stack_output(stack_name: str, region: str, key: str) -> str:
    client = boto3.client("cloudformation", region_name=region)
    response = client.describe_stacks(StackName=stack_name)
    outputs = {o["OutputKey"]: o["OutputValue"] for o in response["Stacks"][0]["Outputs"]}
    try:
        return outputs[key]
    except KeyError:
        raise SystemExit(
            f"stack {stack_name!r} has no output {key!r} (has: {sorted(outputs)})"
        ) from None


def _api_auth(region: str) -> AWS4Auth:
    session = boto3.Session()
    credentials = session.get_credentials()
    if credentials is None:
        raise SystemExit("no AWS credentials found (set AWS_PROFILE or AWS_ACCESS_KEY_ID/SECRET)")
    return AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        "appsync",
        session_token=credentials.token,
    )


def _run_query(api_url: str, auth: AWS4Auth, query: str, variables: dict) -> dict:
    response = requests.post(
        api_url, json={"query": query, "variables": variables}, auth=auth, timeout=30
    )
    response.raise_for_status()
    return response.json()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--stack-name",
        default="AccountInquiryStack-staging",
        help="CloudFormation stack to resolve the API URL from (default: %(default)s)",
    )
    parser.add_argument("--region", default="us-west-2", help="AWS region (default: %(default)s)")
    parser.add_argument(
        "--api-url", help="skip the CloudFormation lookup, query this URL directly"
    )

    sub = parser.add_subparsers(dest="command", required=True)

    accounts = sub.add_parser("accounts", help="getAccountsForCustomer")
    accounts.add_argument("customer_id")

    transactions = sub.add_parser("transactions", help="getTransactionsForAccount")
    transactions.add_argument("account_id")

    raw = sub.add_parser("raw", help="run an arbitrary GraphQL query/mutation")
    raw.add_argument("query")
    raw.add_argument("--variables", default="{}", help="JSON-encoded variables object")

    args = parser.parse_args()

    api_url = args.api_url or _stack_output(args.stack_name, args.region, "AccountInquiryApiUrl")
    auth = _api_auth(args.region)

    if args.command == "accounts":
        result = _run_query(api_url, auth, _ACCOUNTS_QUERY, {"customerId": args.customer_id})
    elif args.command == "transactions":
        result = _run_query(api_url, auth, _TRANSACTIONS_QUERY, {"accountId": args.account_id})
    else:
        result = _run_query(api_url, auth, args.query, json.loads(args.variables))

    print(json.dumps(result, indent=2))
    return 1 if "errors" in result else 0


if __name__ == "__main__":
    sys.exit(main())
