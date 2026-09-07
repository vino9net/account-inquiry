import { util } from "@aws-appsync/utils";
import * as ddb from "@aws-appsync/utils/dynamodb";

// Transaction items live under id=account_id, sid begins_with "TRX_" — same
// single-Query shape as the accounts-for-customer resolver, different partition.
//
// `Number(...)`/`Number.isFinite(...)` are NOT callable in the APPSYNC_JS sandboxed
// runtime (see get_accounts_for_customer.js) — unary `+` and the global `isNaN` do the
// same job without tripping the "Invalid function: Number" deploy-time validation.
export function request(ctx) {
  const accountId = +ctx.args.accountId;
  if (isNaN(accountId)) {
    util.error(`invalid accountId: ${ctx.args.accountId}`, "BadRequest");
  }
  return ddb.query({
    query: { id: { eq: accountId }, sid: { beginsWith: "TRX_" } },
  });
}

export function response(ctx) {
  if (ctx.error) {
    util.error(ctx.error.message, ctx.error.type);
  }
  return ctx.result.items.map((item) => ({
    accountId: `${item.id}`,
    transferId: item.sid.slice("TRX_".length),
    type: item.type,
    amount: item.amount,
    currency: item.currency,
    memo: item.memo,
    status: item.status,
    // stored as epoch ms; AWSTimestamp is epoch seconds.
    processedAt: Math.floor(item.processed_at / 1000),
    updatedAt: Math.floor(item.updated_at / 1000),
  }));
}
