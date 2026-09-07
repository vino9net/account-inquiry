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
