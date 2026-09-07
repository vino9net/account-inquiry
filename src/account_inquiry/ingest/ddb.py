"""Builds the atomic 4-action DynamoDB transaction for one transfer.

Table shape (single table, overloaded partition key — see stack.py):

    Account item:      id=customer_id (N)  sid="ACC_<account_id>" (S)
    Transaction item:  id=account_id  (N)  sid="TRX_<transfer_id>" (S)

Both legs of a transfer — the two transaction Puts and the two balance Updates — go into
one ``TransactWriteItems`` call. That gets us two things at once:

* **Atomicity.** All four writes land or none do. No window where a balance moved but
  its transaction record didn't get written (or vice versa).
* **Idempotency for free.** Each transaction Put carries
  ``ConditionExpression="attribute_not_exists(sid)"``. On a redelivered record (Kinesis
  is at-least-once; a relay restart resends un-acked entries — see the caller for how
  that's detected and handled), the condition fails, which cancels the *whole*
  transaction — the balance updates never get a second look-in. No separate dedup table.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from boto3.dynamodb.types import TypeSerializer

from account_inquiry.ingest.record import TransferRecord

DEBIT = "DEBIT"
CREDIT = "CREDIT"

# boto3.client("dynamodb") is the low-level client (needed for TransactWriteItems'
# per-item ConditionExpression, which the higher-level `resource` API doesn't expose
# the same way) — it takes DynamoDB's typed attribute-value wire format
# (`{"N": "1"}`), not plain Python values. TypeSerializer does that conversion.
_serialize = TypeSerializer().serialize


def _serialize_map(values: dict[str, Any]) -> dict[str, Any]:
    return {k: _serialize(v) for k, v in values.items()}


def build_transact_items(
    table_name: str, record: TransferRecord, updated_at: int
) -> list[dict[str, Any]]:
    """``updated_at``: epoch ms, captured by the caller immediately before this call —
    it's the "write time" half of the processed_at/updated_at latency pair (see
    ``handler.py``)."""
    amount = Decimal(record.amount)

    return [
        _put_transaction(
            table_name,
            account_id=record.from_account,
            transfer_id=record.id,
            type_=DEBIT,
            amount=-amount,
            record=record,
            updated_at=updated_at,
        ),
        _put_transaction(
            table_name,
            account_id=record.to_account,
            transfer_id=record.id,
            type_=CREDIT,
            amount=amount,
            record=record,
            updated_at=updated_at,
        ),
        _update_balance(
            table_name,
            customer_id=record.from_customer_id,
            account_id=record.from_account,
            delta=-amount,
            updated_at=updated_at,
        ),
        _update_balance(
            table_name,
            customer_id=record.to_customer_id,
            account_id=record.to_account,
            delta=amount,
            updated_at=updated_at,
        ),
    ]


def _put_transaction(
    table_name: str,
    *,
    account_id: int,
    transfer_id: str,
    type_: str,
    amount: Decimal,
    record: TransferRecord,
    updated_at: int,
) -> dict[str, Any]:
    return {
        "Put": {
            "TableName": table_name,
            "Item": _serialize_map(
                {
                    "id": account_id,
                    "sid": f"TRX_{transfer_id}",
                    "type": type_,
                    "amount": amount,
                    "currency": record.currency,
                    "memo": record.memo,
                    "status": "COMPLETED",
                    # business time (ledger commit) vs write time (this Lambda) — the
                    # gap between them is the whole point of keeping both.
                    "processed_at": record.created_at,
                    "updated_at": updated_at,
                }
            ),
            "ConditionExpression": "attribute_not_exists(sid)",
        }
    }


def _update_balance(
    table_name: str,
    *,
    customer_id: int,
    account_id: int,
    delta: Decimal,
    updated_at: int,
) -> dict[str, Any]:
    return {
        "Update": {
            "TableName": table_name,
            "Key": _serialize_map({"id": customer_id, "sid": f"ACC_{account_id}"}),
            "UpdateExpression": (
                "ADD balance :delta, avail_balance :delta SET updated_at = :updated_at"
            ),
            "ExpressionAttributeValues": _serialize_map(
                {":delta": delta, ":updated_at": updated_at}
            ),
            # The account must already exist — a transfer can't materialize a new
            # account. If it doesn't, this condition fails and the whole transaction
            # (including the transaction Puts) is cancelled; the record surfaces as a
            # real failure, not a silent phantom-account balance.
            "ConditionExpression": "attribute_exists(id)",
        }
    }
