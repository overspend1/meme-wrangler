"""Tests for data models."""

from meme_wrangler.models import Meme, BackupPayload


def test_meme_to_insert_tuple_order():
    m = Meme(
        id=1,
        owner_file_id="f1",
        mime_type="image",
        scheduled_ts=100,
        posted=0,
        created_ts=90,
        preview_file_id="f1",
        caption="cap",
    )
    t = m.to_insert_tuple()
    assert t == (1, "f1", "image", 100, 0, 90, "f1", "cap")


def test_meme_from_dict_missing_optional():
    d = {
        "id": 5,
        "owner_file_id": "x",
        "scheduled_ts": 200,
        "created_ts": 190,
    }
    m = Meme.from_dict(d)
    assert m.mime_type is None
    assert m.caption is None
    assert m.posted == 0


def test_backup_payload_to_dict():
    m = Meme(
        id=1,
        owner_file_id="f",
        mime_type="image",
        scheduled_ts=100,
        posted=0,
        created_ts=90,
    )
    bp = BackupPayload(
        version=2,
        generated_at="2025-01-01T00:00:00",
        memes=[m],
        scheduled_memes=[m],
    )
    d = bp.to_dict()
    assert d["version"] == 2
    assert len(d["memes"]) == 1
    assert d["memes"][0]["owner_file_id"] == "f"
