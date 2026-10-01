from src.cdc_merge import build_merge_condition
from src.watermark import EPOCH, LocalWatermarkStore, incremental_query, next_watermark


def test_store_roundtrip(tmp_path):
    s = LocalWatermarkStore(tmp_path)
    assert s.get("customers") == EPOCH
    s.set("customers", "2025-01-01T00:00:00")
    assert s.get("customers") == "2025-01-01T00:00:00"


def test_next_watermark_never_moves_back():
    assert next_watermark("2025-02-01", "2025-01-01") == "2025-02-01"
    assert next_watermark("2025-02-01", None) == "2025-02-01"
    assert next_watermark("2025-02-01", "2025-03-01") == "2025-03-01"


def test_queries():
    assert "updated_at > '2025'" in incremental_query("public.c", "updated_at", "2025")
    assert build_merge_condition(["a", "b"]) == "t.a = s.a AND t.b = s.b"
