import { util } from "@aws-appsync/utils";
import * as ddb from "@aws-appsync/utils/dynamodb";

// GraphQL `ID` arrives as a string; the table's partition key is Number (see stack.py) —
// every account belonging to a customer lives under sid begins_with "ACC_" in that same
// partition, so this is one Query, not a Scan.
//
// `Number(...)`/`Number.isFinite(...)` are NOT callable in the APPSYNC_JS sandboxed
// runtime (fails template validation with "Invalid function: Number" at deploy time,
// not at synth/unit-test time — CDK Template assertions don't execute the JS). Unary
// `+` plus the global `isNaN` are plain language operators/allowed globals, not
// disallowed function invocations, and do the same job.
export function request(ctx) {
  const customerId = +ctx.args.customerId;
  if (isNaN(customerId)) {
    util.error(`invalid customerId: ${ctx.args.customerId}`, "BadRequest");
  }
  return ddb.query({
    query: { id: { eq: customerId }, sid: { beginsWith: "ACC_" } },
  });
}

export function response(ctx) {
  if (ctx.error) {
    util.error(ctx.error.message, ctx.error.type);
  }
  return ctx.result.items.map((item) => ({
    id: `${item.id}`,
    accountId: item.sid.slice("ACC_".length),
    name: item.name,
    currency: item.currency,
    balance: item.balance,
    availBalance: item.avail_balance,
    status: item.status,
    // stored as epoch ms; AWSTimestamp is epoch seconds.
    updatedAt: Math.floor(item.updated_at / 1000),
  }));
}
