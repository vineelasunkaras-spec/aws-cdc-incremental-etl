"""PII protection helpers usable both as plain Python and as Spark column expressions."""
import hashlib
from typing import Optional

DEFAULT_SALT = "change-me"  # in production, pull from AWS Secrets Manager / KMS


def hash_value(value: Optional[str], salt: str = DEFAULT_SALT) -> Optional[str]:
    """Deterministic SHA-256 hash so joins on the hashed key still work."""
    if value is None:
        return None
    return hashlib.sha256(f"{salt}{value.strip().lower()}".encode("utf-8")).hexdigest()


def mask_value(value: Optional[str], visible: int = 4, mask_char: str = "*") -> Optional[str]:
    """Keep the last `visible` characters, mask the rest."""
    if value is None:
        return None
    if len(value) <= visible:
        return mask_char * len(value)
    return mask_char * (len(value) - visible) + value[-visible:]


def apply_pii_rules(df, hash_cols=(), mask_cols=(), salt: str = DEFAULT_SALT):
    """Apply hashing / masking to a Spark DataFrame."""
    from pyspark.sql import functions as F

    for c in hash_cols:
        df = df.withColumn(c, F.sha2(F.concat(F.lit(salt), F.lower(F.trim(F.col(c)))), 256))
    for c in mask_cols:
        df = df.withColumn(
            c,
            F.when(F.col(c).isNull(), None).otherwise(
                F.concat(F.regexp_replace(F.expr(f"substring({c}, 1, greatest(length({c}) - 4, 0))"), ".", "*"),
                         F.expr(f"right({c}, 4)"))
            ),
        )
    return df
