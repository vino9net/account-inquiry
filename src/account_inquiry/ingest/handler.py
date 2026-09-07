"""Kinesis -> DynamoDB ingestion Lambda.

Fans each transfer record out into a debit-account / credit-account transaction pair
and applies both balance deltas — atomically, per transfer (ddb.py) — and reports
per-record failures back to the event source mapping so one bad record doesn't force a
retry of the whole batch (ARCH: batchItemFailures, bisect-on-error, DLQ are all
configured on the KinesisEventSource in stack.py; this handler just has to cooperate
with that contract by reporting failures correctly).
"""

from __future__ import annotations

import base64
import os
import time
from typing import Any

import boto3
from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.typing import LambdaContext
from botocore.exceptions import ClientError

from account_inquiry.ingest.ddb import build_transact_items
from account_inquiry.ingest.record import unpack

logger = Logger()
# Namespace set explicitly rather than relying solely on POWERTOOLS_METRICS_NAMESPACE:
# metric flushing raises SchemaValidationError with no namespace at all, which would
# take down the whole handler (including the batchItemFailures it hasn't returned yet)
# in any environment that forgot to set the env var — a local test run included.
metrics = Metrics(namespace="AccountInquiry")

_dynamodb = boto3.client("dynamodb")


def _table_name() -> str:
    name = os.environ.get("DDB_TABLE_NAME")
    if not name:
        raise RuntimeError("DDB_TABLE_NAME is not set")
    return name


def _is_conditional_check_failure(exc: ClientError) -> bool:
    """True only if the *transaction-item* Put is what failed the condition — i.e. this
    transfer was already applied by a prior delivery of the same record. Any other
    cancellation reason (throttling, the account-not-found guard on the balance Update,
    etc.) is a real failure and must propagate so the record is retried / DLQ'd."""
    reasons = exc.response.get("CancellationReasons", [])
    # Put actions for the two transaction items are indices 0 and 1 (see ddb.py).
    return any(r.get("Code") == "ConditionalCheckFailed" for r in reasons[:2])


def process_record(table_name: str, data: bytes) -> None:
    record = unpack(data)
    updated_at = int(time.time() * 1000)
    items = build_transact_items(table_name, record, updated_at)

    try:
        _dynamodb.transact_write_items(TransactItems=items)
    except _dynamodb.exceptions.TransactionCanceledException as exc:
        if _is_conditional_check_failure(exc):
            logger.info("transfer.already_applied", transfer_id=record.id)
            metrics.add_metric(name="TransferAlreadyApplied", unit=MetricUnit.Count, value=1)
            return
        raise

    latency_ms = updated_at - record.created_at
    logger.info("transfer.applied", transfer_id=record.id, latency_ms=latency_ms)
    metrics.add_metric(name="TransferApplied", unit=MetricUnit.Count, value=1)
    metrics.add_metric(name="IngestionLatencyMs", unit=MetricUnit.Milliseconds, value=latency_ms)


@metrics.log_metrics
def lambda_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    table_name = _table_name()
    failures: list[dict[str, str]] = []

    for rec in event["Records"]:
        seq = rec["kinesis"]["sequenceNumber"]
        try:
            data = base64.b64decode(rec["kinesis"]["data"])
            process_record(table_name, data)
        except Exception:
            # Kinesis retries only records at/after the returned itemIdentifier on this
            # shard, so one bad record doesn't force the whole batch to be reprocessed.
            logger.exception("transfer.processing_failed", sequence_number=seq)
            failures.append({"itemIdentifier": seq})

    return {"batchItemFailures": failures}
