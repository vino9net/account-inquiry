"""Runs at conftest-import time — before any test module (and therefore before
account_inquiry.ingest.handler's module-level `boto3.client("dynamodb")`) is imported.
That client resolves its region at import time, so the region has to be pinned here,
not inside a fixture: a fixture body runs too late, after collection has already
imported the handler module with whatever region was ambient at that point.
"""

import os

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
