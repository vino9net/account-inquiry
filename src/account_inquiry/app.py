import os

import aws_cdk as cdk
from dotenv import load_dotenv

from account_inquiry.config import DeploySettings
from account_inquiry.stack import AccountInquiryStack

# Must run before DeploySettings.from_env() reads os.environ. Doesn't override
# variables already set in the shell (e.g. AWS_PROFILE exported by hand) — .env only
# fills in what isn't there. Silently a no-op if no .env file exists, so this is safe
# for CI, where env vars are set another way.
load_dotenv()

settings = DeploySettings.from_env()
stack_name = os.environ.get("STACK_NAME", "AccountInquiryStack")

app = cdk.App()
AccountInquiryStack(app, stack_name, settings=settings)
app.synth()
