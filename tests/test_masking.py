from src.masking import hash_value, mask_value


def test_hash_is_deterministic_and_normalized():
    assert hash_value("A@x.com ") == hash_value("a@x.com")
    assert len(hash_value("a@x.com")) == 64


def test_hash_none():
    assert hash_value(None) is None


def test_mask_keeps_last_four():
    assert mask_value("7329531630") == "******1630"
    assert mask_value("123") == "***"
