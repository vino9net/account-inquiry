#!/usr/bin/env python3
"""Reseed the account-inquiry DynamoDB table from core-sim's Redis account data.

core-sim (the producer, sibling repo) is the source of truth for accounts:
`core_sim.engines.redis_lua.RedisLuaEngine.seed()` writes one `acct:<account_id>` hash
per account — fields `account_no`, `currency`, `balance`, `avail_balance`,
`customer_id`, `status` — keyed by a 0-based account id, with customer ownership
assigned deterministically (see `_assign_customer_ids` there).

This repo's ingest Lambda never creates Account items on its own: `ingest/ddb.py`'s
balance-update action carries `ConditionExpression="attribute_exists(id)")` specifically
so a transfer can't materialize a phantom account. That means nothing in this repo ever
seeds the DynamoDB table — until a script like this one runs, every transfer for an
account that hasn't been copied over fails with ConditionalCheckFailed (see ddb.py /
handler.py's `_is_conditional_check_failure`).

By default this WIPES every item (accounts AND transactions) in the target DynamoDB
table before writing the copied accounts back, so the table's account balances always
start in sync with Redis's — pass --no-wipe to upsert account rows on top of whatever is
already there instead (existing transaction history is left alone either way when
--no-wipe is set).

Pass --wipe-only to empty the table and stop — no Redis connection is made and nothing
is written back, for whenever you want the table emptied without immediately reseeding
it (e.g. no Redis reachable right now). Note: DynamoDB on-demand storage is free
regardless of item count, so this is about getting a clean, empty table on purpose, not
about saving money — there's no cost reason to wipe this table on its own.

Examples:
    # staging, default Redis (redis://localhost:6379 - e.g. after `kubectl port-forward`)
    uv run python scripts/seed_from_redis.py --yes

    uv run python scripts/seed_from_redis.py --redis-url redis://localhost:6380 --yes

    # empty the table on its own, don't reseed (no Redis needed for this one)
    uv run python scripts/seed_from_redis.py --stack-name AccountInquiryStack-staging \\
        --wipe-only --yes

    # a disposable PR stack, upsert only (keep existing transaction history)
    uv run python scripts/seed_from_redis.py --stack-name AccountInquiryStack-pr-42 \\
        --no-wipe --yes

    # see what it would do without writing anything
    uv run python scripts/seed_from_redis.py --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from decimal import Decimal
from typing import Any

import boto3
import redis
from botocore.config import Config

_TABLE_OUTPUT_KEY = "AccountsTableName"
# A single-threaded scan-then-batch-delete of a multi-million-item table took tens of
# minutes in practice (each BatchWriteItem call is a real network round trip, ~25 items
# at a time) — found slow enough during a real teardown to be worth parallelizing.
# DynamoDB's own Scan API is what makes this safe: Segment/TotalSegments hands each
# worker a disjoint slice of the table, so there's no overlap or coordination needed.
_WIPE_PARALLELISM = 16


def _stack_table_name(stack_name: str, region: str) -> str:
    client = boto3.client("cloudformation", region_name=region)
    response = client.describe_stacks(StackName=stack_name)
    outputs = {o["OutputKey"]: o["OutputValue"] for o in response["Stacks"][0]["Outputs"]}
    try:
        return outputs[_TABLE_OUTPUT_KEY]
    except KeyError:
        raise SystemExit(
            f"stack {stack_name!r} has no output {_TABLE_OUTPUT_KEY!r} "
            f"(has: {sorted(outputs)})"
        ) from None


def _read_redis_accounts(redis_url: str) -> list[dict[str, Any]]:
    """Mirrors the hash shape RedisLuaEngine.seed() writes (core-sim/src/core_sim/engines/
    redis_lua.py) — one `acct:<account_id>` hash per account."""
    r = redis.from_url(redis_url, decode_responses=True)
    r.ping()
    accounts = []
    for key in r.scan_iter(match="acct:*", count=1000):
        raw = r.hgetall(key)
        if not raw:
            continue
        accounts.append(
            {
                "account_id": int(key.split(":", 1)[1]),
                "account_no": raw["account_no"],
                "currency": raw["currency"],
                "balance": int(raw["balance"]),
                "avail_balance": int(raw["avail_balance"]),
                "customer_id": int(raw["customer_id"]),
                "status": int(raw["status"]),
            }
        )
    accounts.sort(key=lambda a: a["account_id"])
    return accounts


def _wipe_segment(
    table, segment: int, total_segments: int, progress: list[int], lock: threading.Lock
) -> int:
    """Scans and deletes table's `segment`-th slice of `total_segments` — DynamoDB
    guarantees segments partition the table with no overlap, so N of these can run
    concurrently against the same table with no coordination between them.

    `progress`/`lock` are a shared counter the caller polls to print a live rate —
    without one, a multi-million-item wipe gives no sign of life for minutes at a time,
    indistinguishable from having hung."""
    deleted = 0
    scan_kwargs: dict[str, Any] = {
        "ProjectionExpression": "id, sid",
        "Segment": segment,
        "TotalSegments": total_segments,
    }
    with table.batch_writer() as batch:
        while True:
            page = table.scan(**scan_kwargs)
            for item in page["Items"]:
                batch.delete_item(Key={"id": item["id"], "sid": item["sid"]})
                deleted += 1
            if page["Items"]:
                with lock:
                    progress[0] += len(page["Items"])
            if "LastEvaluatedKey" not in page:
                break
            scan_kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    return deleted


def _wipe_table(table, parallelism: int = _WIPE_PARALLELISM) -> int:
    """Deletes every item in the table (both ACC_ and TRX_ items) — a full reset, not
    just the account rows this script is about to rewrite, so stale transaction history
    from a prior seed generation never lingers against new balances.

    `table`'s underlying client is safe to share across threads (boto3 clients are
    documented thread-safe for making calls); each thread gets its own `batch_writer()`
    with its own internal buffer, so no state is shared between workers either."""
    progress = [0]
    lock = threading.Lock()
    start = time.monotonic()
    with ThreadPoolExecutor(max_workers=parallelism) as pool:
        pending = {
            pool.submit(_wipe_segment, table, segment, parallelism, progress, lock)
            for segment in range(parallelism)
        }
        futures = set(pending)
        while pending:
            done, pending = wait(pending, timeout=5)
            elapsed = time.monotonic() - start
            with lock:
                n = progress[0]
            rate = n / elapsed if elapsed > 0 else 0.0
            print(
                f"  ... {n} items deleted so far ({rate:.0f}/s, {elapsed:.0f}s elapsed)",
                file=sys.stderr,
            )
        return sum(f.result() for f in futures)


def _write_accounts(table, accounts: list[dict[str, Any]]) -> int:
    now_ms = int(time.time() * 1000)
    with table.batch_writer() as batch:
        for acct in accounts:
            batch.put_item(
                Item={
                    "id": acct["customer_id"],
                    "sid": f"ACC_{acct['account_id']}",
                    "account_no": acct["account_no"],
                    "currency": acct["currency"],
                    "balance": Decimal(acct["balance"]),
                    "avail_balance": Decimal(acct["avail_balance"]),
                    "status": "active" if acct["status"] == 1 else "inactive",
                    "updated_at": now_ms,
                }
            )
    return len(accounts)


def _plan_description(args: argparse.Namespace, n_accounts: int) -> tuple[str, str]:
    """(dry-run action phrase, confirmation-prompt verb phrase) for the mode args select."""
    if args.wipe_only:
        return (
            "delete every item in",
            "DELETE ALL ITEMS (accounts + transactions) in and leave empty",
        )
    if not args.no_wipe:
        return (
            f"wipe every item, then write {n_accounts} account items to",
            "WIPE ALL ITEMS (accounts + transactions) in and reseed",
        )
    return f"upsert {n_accounts} account items into", "upsert accounts into"


def _confirm(args: argparse.Namespace, table_name: str, verb: str) -> bool:
    if args.yes:
        return True
    answer = input(f"About to {verb} {table_name!r}. Type 'yes' to continue: ")
    return answer.strip().lower() == "yes"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--redis-url",
        default=os.environ.get("REDIS_URL", "redis://localhost:6379"),
        help="core-sim's Redis (default: $REDIS_URL or redis://localhost:6379, same "
        "default core-sim itself uses)",
    )
    parser.add_argument(
        "--stack-name",
        default="AccountInquiryStack-staging",
        help="CloudFormation stack to resolve the DynamoDB table name from "
        "(default: %(default)s)",
    )
    parser.add_argument(
        "--region", default="us-west-2", help="AWS region (default: %(default)s)"
    )
    parser.add_argument(
        "--table-name", help="skip the CloudFormation lookup, write directly to this table"
    )
    parser.add_argument(
        "--no-wipe",
        action="store_true",
        help="upsert account rows only — do not delete existing items first",
    )
    parser.add_argument(
        "--wipe-only",
        action="store_true",
        help="delete every item and stop — no Redis read, no write back "
        "(mutually exclusive with --no-wipe)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="read Redis and report what would happen, but touch no DynamoDB data",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="skip the interactive confirmation prompt (required for --no-wipe too, "
        "since this still overwrites any account rows already in the table)",
    )
    args = parser.parse_args()
    if args.wipe_only and args.no_wipe:
        raise SystemExit("--wipe-only and --no-wipe are mutually exclusive")

    accounts: list[dict[str, Any]] = []
    if not args.wipe_only:
        print(f"reading accounts from {args.redis_url} ...")
        accounts = _read_redis_accounts(args.redis_url)
        if not accounts:
            raise SystemExit(f"no acct:* keys found at {args.redis_url} — nothing to copy")
        n_customers = len({a["customer_id"] for a in accounts})
        print(f"found {len(accounts)} accounts across {n_customers} customers in Redis")

    table_name = args.table_name or _stack_table_name(args.stack_name, args.region)
    target = args.table_name or f"{args.stack_name} ({args.region})"
    print(f"target DynamoDB table: {table_name} [{target}]")

    dry_run_action, confirm_verb = _plan_description(args, len(accounts))

    if args.dry_run:
        print(f"--dry-run: would {dry_run_action} {table_name!r}. Stopping here.")
        return 0

    if not _confirm(args, table_name, confirm_verb):
        print("aborted.")
        return 1

    # Default max_pool_connections (10) is well under _WIPE_PARALLELISM (16) — found
    # this the hard way: a real wipe against a multi-million-item table showed a ~75s
    # stall before any progress at all, then a sustained ~200-370 items/s (hours for the
    # full table), consistent with 16 threads contending for 10 connections rather than
    # the scan/delete calls themselves being slow.
    pool_config = Config(max_pool_connections=_WIPE_PARALLELISM)
    table = boto3.resource("dynamodb", region_name=args.region, config=pool_config).Table(
        table_name
    )

    if args.wipe_only:
        print("wiping all items ...")
        deleted = _wipe_table(table)
        print(f"deleted {deleted} items; table left empty")
        return 0

    if not args.no_wipe:
        print("wiping existing items ...")
        deleted = _wipe_table(table)
        print(f"deleted {deleted} items")

    written = _write_accounts(table, accounts)
    print(f"wrote {written} account items")
    return 0


if __name__ == "__main__":
    sys.exit(main())
