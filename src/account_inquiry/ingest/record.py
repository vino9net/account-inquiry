"""Fixed-width binary transfer record — the wire contract this Lambda consumes off Kinesis.

Mirrors the shape of ``core_sim``'s ``record.py`` exactly, field order included —
``core_sim`` is the producer, so its layout is the actual wire contract, not this
module's. (An earlier version of this module assumed ``from_customer_id``/
``to_customer_id`` sat right after ``to_account`` — core-sim actually appends them after
``memo``. Both layouts pack to the same 176 bytes, so that mismatch never raised a
struct error; it silently decoded every field after ``to_account`` into garbage instead.
See git history for the fix.) ``pack()`` below is only ever used by tests, to build
synthetic Kinesis payloads — this project is a consumer, not the producer.

Layout (little-endian, no padding — sizes add to exactly 176)::

    16s   id               transfer id, ULID, binary
    Q     from_account     uint64
    Q     to_account       uint64
    q     amount           int64, minor units, unsigned magnitude of the transfer
    4s    currency         3 chars + 1 pad
    q     created_at       int64, epoch ms — when the transfer was applied at the ledger
    B     status           uint8 (1=ok; only successful transfers reach the stream)
    100s  memo             UTF-8, null-padded/truncated to MEMO_SIZE bytes
    Q     from_customer_id uint64
    Q     to_customer_id   uint64
    7x    reserved
"""

from __future__ import annotations

import os
import struct
import time

MEMO_SIZE = 100

_RECORD = struct.Struct(f"<16sQQq4sqB{MEMO_SIZE}sQQ7x")
RECORD_SIZE = _RECORD.size
assert RECORD_SIZE == 176, f"record must be 176 bytes, got {RECORD_SIZE}"

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def new_ulid() -> bytes:
    """16-byte ULID: 48-bit ms timestamp + 80 bits of randomness. Test-data helper only —
    in production the transfer id is minted upstream, at the ledger."""
    return int(time.time() * 1000).to_bytes(6, "big") + os.urandom(10)


def ulid_to_str(raw: bytes) -> str:
    """Crockford base32, 26 chars — the canonical ULID text form, used as the DynamoDB
    ``sid`` suffix for transaction items."""
    n = int.from_bytes(raw, "big")
    out = bytearray(26)
    for i in range(25, -1, -1):
        out[i] = ord(_CROCKFORD[n & 0x1F])
        n >>= 5
    return out.decode("ascii")


def str_to_ulid(s: str) -> bytes:
    n = 0
    for ch in s:
        n = (n << 5) | _CROCKFORD.index(ch.upper())
    return n.to_bytes(16, "big")


class TransferRecord:
    __slots__ = (
        "id",
        "from_account",
        "to_account",
        "amount",
        "currency",
        "created_at",
        "status",
        "memo",
        "from_customer_id",
        "to_customer_id",
    )

    def __init__(
        self,
        id: str,  # noqa: A002
        from_account: int,
        to_account: int,
        amount: int,
        currency: str,
        created_at: int,
        status: int,
        memo: str,
        from_customer_id: int,
        to_customer_id: int,
    ) -> None:
        self.id = id
        self.from_account = from_account
        self.to_account = to_account
        self.amount = amount
        self.currency = currency
        self.created_at = created_at
        self.status = status
        self.memo = memo
        self.from_customer_id = from_customer_id
        self.to_customer_id = to_customer_id


def pack(
    ulid: bytes,
    from_account: int,
    to_account: int,
    amount: int,
    currency: str,
    created_at: int,
    status: int,
    memo: str = "",
    from_customer_id: int = 0,
    to_customer_id: int = 0,
) -> bytes:
    """Only used by tests to build synthetic Kinesis payloads — this project is a
    consumer, not the producer."""
    return _RECORD.pack(
        ulid,
        from_account,
        to_account,
        amount,
        currency.encode("ascii")[:4],
        created_at,
        status,
        memo.encode("utf-8")[:MEMO_SIZE],
        from_customer_id,
        to_customer_id,
    )


def unpack(buf: bytes, offset: int = 0) -> TransferRecord:
    (
        ulid,
        frm,
        to,
        amt,
        ccy,
        ts,
        status,
        memo,
        from_cust,
        to_cust,
    ) = _RECORD.unpack_from(buf, offset)
    return TransferRecord(
        id=ulid_to_str(ulid),
        from_account=frm,
        to_account=to,
        amount=amt,
        currency=ccy.rstrip(b"\x00").decode("ascii"),
        created_at=ts,
        status=status,
        memo=memo.rstrip(b"\x00").decode("utf-8", errors="replace"),
        from_customer_id=from_cust,
        to_customer_id=to_cust,
    )
