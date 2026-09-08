from wire_fixtures import pack_wire

from account_inquiry.ingest.record import RECORD_SIZE, new_ulid, pack, ulid_to_str, unpack


def test_pack_unpack_round_trip():
    ulid = new_ulid()
    buf = pack(
        ulid,
        from_account=1,
        to_account=2,
        from_customer_id=100,
        to_customer_id=200,
        amount=500,
        currency="SGD",
        created_at=1_700_000_000_000,
        status=1,
        memo="lunch",
    )
    assert len(buf) == RECORD_SIZE

    record = unpack(buf)
    assert record.id == ulid_to_str(ulid)
    assert record.from_account == 1
    assert record.to_account == 2
    assert record.from_customer_id == 100
    assert record.to_customer_id == 200
    assert record.amount == 500
    assert record.currency == "SGD"
    assert record.created_at == 1_700_000_000_000
    assert record.status == 1
    assert record.memo == "lunch"


def test_memo_truncated_and_padding_stripped():
    ulid = new_ulid()
    buf = pack(
        ulid,
        from_account=1,
        to_account=2,
        from_customer_id=100,
        to_customer_id=200,
        amount=1,
        currency="SGD",
        created_at=0,
        status=1,
        memo="x" * 500,
    )
    record = unpack(buf)
    assert len(record.memo.encode("utf-8")) <= 100


def test_empty_memo_round_trips_as_empty_string():
    ulid = new_ulid()
    buf = pack(
        ulid,
        from_account=1,
        to_account=2,
        from_customer_id=100,
        to_customer_id=200,
        amount=1,
        currency="SGD",
        created_at=0,
        status=1,
    )
    assert unpack(buf).memo == ""


def test_unpack_matches_core_sims_actual_wire_layout():
    """Regression guard for the from_customer_id/to_customer_id field-order mismatch
    documented in record.py's module docstring: builds bytes via wire_fixtures.pack_wire
    (a copy of core-sim's literal struct format, independent of this module's own
    pack()) so this test fails if the two ever drift apart again, instead of passing
    because both sides of the round trip agree with each other but not with the real
    producer."""
    ulid = new_ulid()
    buf = pack_wire(
        ulid,
        from_account=1,
        to_account=2,
        amount=500,
        currency="SGD",
        created_at=1_700_000_000_000,
        status=1,
        memo="lunch",
        from_customer_id=100,
        to_customer_id=200,
    )
    assert len(buf) == RECORD_SIZE

    record = unpack(buf)
    assert record.id == ulid_to_str(ulid)
    assert record.from_account == 1
    assert record.to_account == 2
    assert record.amount == 500
    assert record.currency == "SGD"
    assert record.created_at == 1_700_000_000_000
    assert record.status == 1
    assert record.memo == "lunch"
    assert record.from_customer_id == 100
    assert record.to_customer_id == 200
