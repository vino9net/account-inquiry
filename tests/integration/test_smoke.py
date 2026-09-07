"""Read-only checks safe to run against the persistent staging stack: no writes, no
Kinesis records. These only prove that AppSync IAM auth + the JS resolvers + the
DynamoDB data source are wired correctly — they don't assert on whatever live data
happens to already be in the table, since staging is fed continuously by whatever is
driving core-sim.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.smoke


def test_get_accounts_for_customer_is_reachable(graphql_query):
    result = graphql_query(
        """
        query ($customerId: ID!) {
            getAccountsForCustomer(customerId: $customerId) {
                id
                accountId
                currency
                balance
            }
        }
        """,
        {"customerId": "0"},
    )

    assert "errors" not in result, result
    assert result["data"]["getAccountsForCustomer"] is not None


def test_get_transactions_for_account_is_reachable(graphql_query):
    result = graphql_query(
        """
        query ($accountId: ID!) {
            getTransactionsForAccount(accountId: $accountId) {
                transferId
                type
                amount
            }
        }
        """,
        {"accountId": "0"},
    )

    assert "errors" not in result, result
    assert result["data"]["getTransactionsForAccount"] is not None
