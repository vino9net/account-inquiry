"""Exercises the full path end to end: a transfer record placed on the real Kinesis
stream must be picked up by the deployed ingest Lambda and show up as a debit/credit
transaction pair with both balances moved — read back through the same AppSync API a
real client would use.

Only safe to run against a disposable stack: it seeds account rows directly and puts a
live record on the stream. Never point TESTING_STACK_NAME at the persistent staging
stack for this module.
"""

from __future__ import annotations

import time
from decimal import Decimal

from account_inquiry.ingest.record import new_ulid, pack, ulid_to_str

FROM_CUSTOMER_ID = 900_001
TO_CUSTOMER_ID = 900_002
FROM_ACCOUNT_ID = 900_101
TO_ACCOUNT_ID = 900_102
STARTING_BALANCE = Decimal(1_000_000)
TRANSFER_AMOUNT = 500

_TRANSACTIONS_QUERY = """
query ($accountId: ID!) {
    getTransactionsForAccount(accountId: $accountId) {
        transferId
        type
        amount
        currency
        memo
    }
}
"""

_ACCOUNTS_QUERY = """
query ($customerId: ID!) {
    getAccountsForCustomer(customerId: $customerId) {
        accountId
        balance
    }
}
"""


def _seed_account(ddb_table, *, customer_id: int, account_id: int) -> None:
    ddb_table.put_item(
        Item={
            "id": customer_id,
            "sid": f"ACC_{account_id}",
            "currency": "SGD",
            "balance": STARTING_BALANCE,
            "avail_balance": STARTING_BALANCE,
            "status": "active",
            "updated_at": int(time.time() * 1000),
        }
    )


def _poll_for_transaction(
    graphql_query,
    account_id: int,
    transfer_id: str,
    attempts: int = 12,
    delay_seconds: float = 5.0,
) -> list[dict]:
    for _ in range(attempts):
        result = graphql_query(_TRANSACTIONS_QUERY, {"accountId": str(account_id)})
        assert "errors" not in result, result
        transactions = result["data"]["getTransactionsForAccount"]
        if any(t["transferId"] == transfer_id for t in transactions):
            return transactions
        time.sleep(delay_seconds)
    raise AssertionError(
        f"transfer {transfer_id} did not appear within {attempts * delay_seconds:.0f}s"
    )


def test_transfer_moves_balance_and_appears_in_history(
    ddb_table, kinesis_client, stream_name, graphql_query
):
    _seed_account(ddb_table, customer_id=FROM_CUSTOMER_ID, account_id=FROM_ACCOUNT_ID)
    _seed_account(ddb_table, customer_id=TO_CUSTOMER_ID, account_id=TO_ACCOUNT_ID)

    ulid = new_ulid()
    transfer_id = ulid_to_str(ulid)
    data = pack(
        ulid=ulid,
        from_account=FROM_ACCOUNT_ID,
        to_account=TO_ACCOUNT_ID,
        from_customer_id=FROM_CUSTOMER_ID,
        to_customer_id=TO_CUSTOMER_ID,
        amount=TRANSFER_AMOUNT,
        currency="SGD",
        created_at=int(time.time() * 1000),
        status=1,
        memo="integration test",
    )
    kinesis_client.put_record(
        StreamName=stream_name, Data=data, PartitionKey=str(FROM_ACCOUNT_ID)
    )

    debit_leg = next(
        t
        for t in _poll_for_transaction(graphql_query, FROM_ACCOUNT_ID, transfer_id)
        if t["transferId"] == transfer_id
    )
    assert debit_leg["type"] == "DEBIT"
    assert debit_leg["amount"] == -TRANSFER_AMOUNT

    credit_leg = next(
        t
        for t in _poll_for_transaction(graphql_query, TO_ACCOUNT_ID, transfer_id)
        if t["transferId"] == transfer_id
    )
    assert credit_leg["type"] == "CREDIT"
    assert credit_leg["amount"] == TRANSFER_AMOUNT

    accounts = graphql_query(_ACCOUNTS_QUERY, {"customerId": str(FROM_CUSTOMER_ID)})["data"][
        "getAccountsForCustomer"
    ]
    from_account = next(a for a in accounts if a["accountId"] == str(FROM_ACCOUNT_ID))
    assert from_account["balance"] == float(STARTING_BALANCE - TRANSFER_AMOUNT)
