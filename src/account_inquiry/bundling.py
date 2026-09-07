"""Local-first Lambda asset bundling.

CDK's default Python bundling shells out to Docker, which is a hard dependency for
``cdk synth``/``pytest`` even though nothing here needs a container: boto3 and
aws-lambda-powertools are both pure-Python packages (no C extensions), so a plain
``pip install --target`` on the host produces artifacts that work fine on Lambda
regardless of host OS/arch. ``ILocalBundling`` lets CDK try that first and only fall
back to Docker if it fails (e.g. no `pip` on PATH) — so a dev machine or CI runner
without Docker running isn't blocked.

**Caveat**: this stops being safe the moment a dependency with a compiled extension is
added (e.g. anything pulling in cryptography, pydantic-core, numpy). At that point host
and Lambda wheels can diverge and this needs to go back to Docker (or `--platform`
cross-install flags) for that dependency.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import jsii
from aws_cdk import BundlingOptions, ILocalBundling


@jsii.implements(ILocalBundling)
class PipLocalBundling:
    """Bundles the `ingest` subpackage plus its (pure-Python) runtime deps. Ships only
    `ingest/` — the rest of `account_inquiry` (the CDK app itself, resolvers, schema)
    has no business inside the Lambda package."""

    def __init__(self, src_dir: Path, requirements: list[str]) -> None:
        self._src_dir = src_dir
        self._requirements = requirements

    def try_bundle(self, output_dir: str, options: BundlingOptions) -> bool:  # noqa: ARG002
        # `python -m pip` doesn't work here: a uv-managed venv has no pip installed
        # into it by default. `uv pip install --target` is the pip-compatible
        # equivalent that doesn't need one. Fall back to plain pip for anyone running
        # this outside a uv-managed environment.
        uv = shutil.which("uv")
        base_cmd = [uv, "pip", "install"] if uv else [sys.executable, "-m", "pip", "install"]
        install_cmd = [*base_cmd, *self._requirements, "--target", output_dir]
        try:
            subprocess.run(install_cmd, check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False

        pkg_out = Path(output_dir) / "account_inquiry"
        pkg_out.mkdir(parents=True, exist_ok=True)
        (pkg_out / "__init__.py").write_text("")
        shutil.copytree(self._src_dir / "ingest", pkg_out / "ingest", dirs_exist_ok=True)
        return True
