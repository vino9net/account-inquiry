"""Deploy-time settings for the CDK app.

Not the same thing as the Lambda's own runtime env vars (DDB_TABLE_NAME etc, set as
Lambda environment variables by the stack) — this is what governs *how the stack itself
is built*, decided at `cdk synth`/`cdk deploy` time.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# Environments that hold real data: deletion protection stays on and destroying the
# stack orphans the table/stream instead of deleting them. Anything else (feature
# branches, ad hoc dev stacks) stays fully disposable so it doesn't accumulate orphaned
# resources in the account.
_RETAIN_ENVS = {"staging", "prod", "production"}


@dataclass(frozen=True)
class DeploySettings:
    deploy_env: str = "feature"
    # Kinesis stream names are unique per account/region, not per stack — stack.py
    # passes this as an explicit stream_name, so anything deploying alongside another
    # stack (e.g. a PR's disposable stack next to staging) must override this to a
    # distinct value or CDK's early validation rejects the changeset outright.
    kinesis_stream_name: str = "transfers"
    kinesis_shard_count: int = 1
    # ARN of an existing IAM user/role that should be able to call the AppSync API
    # (integration/smoke tests run as this principal). Optional — unset means nothing
    # extra is granted, which is fine for a stack nobody needs to query externally.
    ci_principal_arn: str | None = None

    @property
    def retain_data(self) -> bool:
        return self.deploy_env in _RETAIN_ENVS

    @classmethod
    def from_env(cls) -> DeploySettings:
        return cls(
            deploy_env=os.environ.get("DEPLOY_ENV", cls.deploy_env),
            kinesis_stream_name=os.environ.get("KINESIS_STREAM_NAME", cls.kinesis_stream_name),
            kinesis_shard_count=int(
                os.environ.get("KINESIS_SHARD_COUNT", str(cls.kinesis_shard_count))
            ),
            ci_principal_arn=os.environ.get("CI_IAM_PRINCIPAL_ARN"),
        )
