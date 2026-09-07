"""Handler logic against a moto-mocked DynamoDB — no real AWS needed."""

from __future__ import annotations

import base64
import os
from decimal import Decimal

import boto3
import pytest
from moto import mock_aws

from account_inquiry.ingest.handler import lambda_handler, process_record
from account_inquiry.ingest.record import new_ulid, pack, ulid_to_str

TABLE_NAME = "test-accounts"


@pytest.fixture
def ddb_table():
    with mock_aws():
        os.environ["DDB_TABLE_NAME"] = TABLE_NAME
        client = boto3.client("dynamodb", region_name="us-east-1")
        client.create_table(
            TableName=TABLE_NAME,
            AttributeDefinitions=[
                {"AttributeName": "id", "AttributeType": "N"},
                {"AttributeName": "sid", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "id", "KeyType": "HASH"},
                {"AttributeName": "sid", "KeyType": "RANGE"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        # seed two accounts: customer 100 owns account 1, customer 200 owns account 2
        resource = boto3.resource("dynamodb", region_name="us-east-1")
        table = resource.Table(TABLE_NAME)
        table.put_item(
            Item={
                "id": 100,
                "sid": "ACC_1",
                "currency": "SGD",
                "balance": Decimal(1_000_000),
                "avail_balance": Decimal(1_000_000),
            }
        )
        table.put_item(
            Item={
                "id": 200,
                "sid": "ACC_2",
                "currency": "SGD",
                "balance": Decimal(1_000_000),
                "avail_balance": Decimal(1_000_000),
            }
        )
        yield table


def _transfer_record(**overrides) -> bytes:
    kw = {
        "ulid": new_ulid(),
        "from_account": 1,
        "to_account": 2,
        "from_customer_id": 100,
        "to_customer_id": 200,
        "amount": 500,
        "currency": "SGD",
        "created_at": 1_700_000_000_000,
        "status": 1,
        "memo": "lunch",
    }
    kw.update(overrides)
    return pack(**kw)


def test_process_record_moves_balance_and_writes_both_legs(ddb_table):
    data = _transfer_record()
    process_record(TABLE_NAME, data)

    debit = ddb_table.get_item(Key={"id": 100, "sid": "ACC_1"})["Item"]
    credit = ddb_table.get_item(Key={"id": 200, "sid": "ACC_2"})["Item"]
    assert debit["balance"] == Decimal(1_000_000 - 500)
    assert credit["balance"] == Decimal(1_000_000 + 500)


def test_process_record_writes_signed_transaction_legs(ddb_table):
    ulid = new_ulid()
    data = _transfer_record(ulid=ulid)
    process_record(TABLE_NAME, data)

    transfer_id = ulid_to_str(ulid)
    debit_txn = ddb_table.get_item(Key={"id": 1, "sid": f"TRX_{transfer_id}"})["Item"]
    credit_txn = ddb_table.get_item(Key={"id": 2, "sid": f"TRX_{transfer_id}"})["Item"]
    assert debit_txn["type"] == "DEBIT"
    assert debit_txn["amount"] == Decimal(-500)
    assert credit_txn["type"] == "CREDIT"
    assert credit_txn["amount"] == Decimal(500)


def test_duplicate_delivery_is_a_noop_not_a_double_apply(ddb_table):
    data = _transfer_record()
    process_record(TABLE_NAME, data)
    process_record(TABLE_NAME, data)  # redelivery, e.g. relay restart resending

    debit = ddb_table.get_item(Key={"id": 100, "sid": "ACC_1"})["Item"]
    assert debit["balance"] == Decimal(1_000_000 - 500)  # moved once, not twice


def test_transfer_to_unknown_account_does_not_move_balance(ddb_table):
    data = _transfer_record(to_account=999_999, to_customer_id=999)
    with pytest.raises(Exception):
        process_record(TABLE_NAME, data)

    debit = ddb_table.get_item(Key={"id": 100, "sid": "ACC_1"})["Item"]
    assert debit["balance"] == Decimal(1_000_000)  # untouched — all-or-nothing


def _kinesis_event(records: list[bytes]) -> dict:
    return {
        "Records": [
            {
                "kinesis": {
                    "data": base64.b64encode(data).decode(),
                    "sequenceNumber": str(i),
                },
                "eventID": f"shardId-000000000000:{i}",
            }
            for i, data in enumerate(records)
        ]
    }


def test_lambda_handler_reports_bad_record_without_failing_whole_batch(ddb_table):
    good = _transfer_record()
    bad = b"not a valid record"
    event = _kinesis_event([good, bad])

    result = lambda_handler(event, context=None)

    assert result["batchItemFailures"] == [{"itemIdentifier": "1"}]
    debit = ddb_table.get_item(Key={"id": 100, "sid": "ACC_1"})["Item"]
    assert debit["balance"] == Decimal(1_000_000 - 500)  # the good record still applied
