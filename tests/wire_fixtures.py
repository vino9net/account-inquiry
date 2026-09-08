"""Builds a transfer record's raw bytes from core-sim's *literal* wire format —
deliberately independent of ``account_inquiry.ingest.record``'s own ``pack()``/``_RECORD``.

A test that builds its Kinesis payload with ``account_inquiry.ingest.record.pack()`` and
reads it back with that same module's ``unpack()`` only proves the two are consistent
with *each other* — that's exactly how the from_customer_id/to_customer_id field-order
mismatch against the real producer (core-sim) shipped without a single test catching it.
This module hardcodes core-sim's actual layout (mirror of
``core_sim/src/core_sim/record.py``'s ``_RECORD``) so tests built on it validate against
the real wire contract instead.
"""

from __future__ import annotations

import struct

MEMO_SIZE = 100

# Mirrors core_sim/src/core_sim/record.py's _RECORD exactly. If core-sim's layout ever
# changes, update this line to match it — do NOT import account_inquiry.ingest.record's
# own _RECORD here, that would defeat the point.
_WIRE = struct.Struct(f"<16sQQq4sqB{MEMO_SIZE}sQQ7x")
WIRE_SIZE = _WIRE.size
assert WIRE_SIZE == 176, f"core-sim wire record must be 176 bytes, got {WIRE_SIZE}"


def pack_wire(
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
    return _WIRE.pack(
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
