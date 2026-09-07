# account-inquiry

## What this is

Account balance & transaction history read side for a demo banking system. Consumes a
Kinesis stream of fund-transfer events, fans each transfer out into a debit/credit
transaction pair in DynamoDB, and exposes the result via an AppSync GraphQL API.

```
Kinesis (transfers) --event source mapping--> Lambda (ingest) --TransactWriteItems--> DynamoDB
                                                                                          |
                                                                AppSync (IAM auth) --------+
                                                                JS resolvers, direct DDB DS
```

This is the consumer half of a two-repo pair. The producer — a core banking simulator
that publishes transfer events onto the Kinesis stream — lives in a sibling repo
(`core-sim`), not here. This repo only reads that stream; it does not run or own the
producer.

Rewritten from an earlier `accounts-api-stack` prototype (EventBridge-based, CDK v1-era
appsync-alpha, non-transactional writes). This version replaces EventBridge with
Kinesis, uses stable `aws_cdk.aws_appsync` with JS resolvers, and makes every write
atomic + idempotent via `TransactWriteItems`. Don't treat the old prototype's decisions
as load-bearing — several were superseded on purpose.

## Data model

Single DynamoDB table, partition key `id` (Number), sort key `sid` (String). The
partition key means a different thing depending on which item type you land on:

| Item type | `id` | `sid` | Notes |
|---|---|---|---|
| Account | `customer_id` | `ACC_<account_id>` | `balance`, `avail_balance`, `currency`, `status`, `updated_at` |
| Transaction | `account_id` | `TRX_<transfer_id>` | `type` (DEBIT/CREDIT), signed `amount`, `currency`, `memo`, `processed_at`, `updated_at` |

This lets both access patterns run as a single `Query`, no GSI:
- "All accounts + balances for a customer" (mobile app login) → `id = customer_id, sid begins_with "ACC_"`
- "Transaction history for one account" → `id = account_id, sid begins_with "TRX_"`

`amount` is **signed**: negative on the debit leg, positive on the credit leg — so
summing an account's transaction legs reconciles against its balance delta (same
conservation-check discipline as `core-sim`).

`processed_at` (from the wire event — when the transfer was applied at the ledger) vs
`updated_at` (Lambda write time) are both stored on transaction items, so
`updated_at - processed_at` gives per-record ingestion latency without needing to
reconstruct it from logs.

## Wire contract (Kinesis record)

`src/account_inquiry/ingest/record.py` — fixed-width 176-byte binary struct (`id`,
`from_account`, `to_account`, `from_customer_id`, `to_customer_id`, `amount`,
`currency`, `created_at`, `status`, `memo`). This project only consumes it; it is not
the producer. `core-sim`'s own `record.py` does not yet emit the customer-id fields —
this module is this repo's source of truth for the layout until that's added upstream.

## Code structure

```
src/account_inquiry/
  app.py           CDK entry point (loads .env, builds DeploySettings, synths the stack)
  stack.py         the CDK Stack: DynamoDB table, Kinesis stream, ingest Lambda +
                    event source mapping + DLQ, AppSync API + JS resolvers
  config.py        DeploySettings — env-driven, governs retain-vs-destroy per DEPLOY_ENV
  bundling.py       local (non-Docker) Lambda asset bundling — see "Bundling" below
  schema.graphql    AppSync schema: getAccountsForCustomer, getTransactionsForAccount
  resolvers/        AppSync JS resolvers (APPSYNC_JS runtime), one per query
  ingest/            the Lambda package — this subtree is what actually ships to Lambda
    record.py        wire format pack/unpack (see above)
    ddb.py            builds the 4-action TransactWriteItems call for one transfer
    handler.py         Kinesis batch handler: unpack, write, classify failures

tests/
  conftest.py       pins AWS_DEFAULT_REGION/fake creds before any module import —
                     handler.py's module-level boto3 client resolves region at import
                     time, so this can't be a fixture (runs too late)
  unit/
    test_record.py   wire format round-trip
    test_handler.py  ingestion logic against moto-mocked DynamoDB
    test_stack.py    CDK Template assertions (resource shape, removal policy per env)
  integration/       not yet implemented — see "Integration tests" below
```

## Idempotency & atomicity

Each transfer's two transaction Puts and two balance Updates go into one
`TransactWriteItems` call. Both transaction Puts carry
`ConditionExpression="attribute_not_exists(sid)"`. On a redelivered record (Kinesis is
at-least-once; a producer/relay restart can resend), the condition fails, which cancels
the *whole* transaction — no double-applied balance. `handler.py` distinguishes that
case (`TransactionCanceledException` with `ConditionalCheckFailed` on one of the first
two actions → log + no-op) from a genuine failure (re-raise → reported via
`batchItemFailures` so only that record is retried, not the whole batch).

## Running unit tests

```bash
uv sync
uv run pytest tests/unit -q
```

No AWS account or running services needed — DynamoDB is mocked via `moto`, CDK
assertions run against a locally synthesized template.

## Integration tests

Not yet implemented (`tests/integration/` doesn't exist yet). When added, the intent is
end-to-end against a real deployed stack: put a packed record on the Kinesis stream,
poll the AppSync API until the transaction/balance shows up, same pattern the old
`accounts-api-stack` prototype used (`api_url`/`api_auth` fixtures, IAM-signed
requests via `requests-aws4auth` — already a dev dependency in `pyproject.toml` for
this reason).

## CDK / deployment

```bash
cdk synth                                   # local only, no AWS calls
cdk bootstrap aws://<account-id>/<region>    # one-time per account/region, before first deploy
cdk deploy
```

`DEPLOY_ENV` (default `feature`) controls removal policy: `staging`/`prod`/`production`
retain the DynamoDB table and Kinesis stream (and turn on point-in-time recovery +
deletion protection) on stack teardown; anything else destroys them on `cdk destroy`.
See `config.py` (`DeploySettings.retain_data`).

`app.py` calls `load_dotenv()` before reading env vars, so a local `.env` (gitignored)
can set `AWS_PROFILE`, `DEPLOY_ENV`, `KINESIS_STREAM_NAME`, etc. without polluting the
shell. Values already exported in the shell take precedence over `.env`.

## Bundling

The ingest Lambda only needs `boto3` + `aws-lambda-powertools` — both pure Python, so
`bundling.py` tries `uv pip install --target` (or plain `pip` outside a uv env) on the
host first via CDK's `ILocalBundling`, and only falls back to Docker if that's
unavailable. This stops being safe the moment a dependency with a compiled extension
(cryptography, numpy, etc.) gets added to `ingest/` — at that point, go back to
Docker-only bundling for cross-platform correctness.

## Dev tooling

Same house style as `core-sim`: `uv`, `ruff`, `ty`, `pre-commit`.

```bash
uv run ruff check . && uv run ruff format .
uv run ty check
uv run pre-commit run --all-files
```
