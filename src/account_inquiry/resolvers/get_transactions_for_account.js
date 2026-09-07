import { util } from "@aws-appsync/utils";
import * as ddb from "@aws-appsync/utils/dynamodb";

// Transaction items live under id=account_id, sid begins_with "TRX_" — same
// single-Query shape as the accounts-for-customer resolver, different partition.
export function request(ctx) {
  const accountId = Number(ctx.args.accountId);
  if (!Number.isFinite(accountId)) {
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
