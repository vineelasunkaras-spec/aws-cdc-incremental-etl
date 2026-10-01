"""CDC upsert into Delta Lake with deduplication of change events."""
import argparse
from typing import List

import yaml


def build_merge_condition(keys: List[str]) -> str:
    return " AND ".join(f"t.{k} = s.{k}" for k in keys)


def dedupe_latest(df, keys: List[str], order_col: str):
    """Keep only the latest change event per primary key."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def merge_cdc(spark, changes_df, target_path: str, keys: List[str], order_col: str):
    """Apply inserts/updates/deletes. Expects an `op` column with I/U/D."""
    from delta.tables import DeltaTable

    latest = dedupe_latest(changes_df, keys, order_col)
    if not DeltaTable.isDeltaTable(spark, target_path):
        latest.filter("op != 'D'").drop("op").write.format("delta").save(target_path)
        return

    target = DeltaTable.forPath(spark, target_path)
    cols = [c for c in latest.columns if c != "op"]
    (
        target.alias("t")
        .merge(latest.alias("s"), build_merge_condition(keys))
        .whenMatchedDelete(condition="s.op = 'D'")
        .whenMatchedUpdate(condition="s.op != 'D'", set={c: f"s.{c}" for c in cols})
        .whenNotMatchedInsert(condition="s.op != 'D'", values={c: f"s.{c}" for c in cols})
        .execute()
    )


def main():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    from src.masking import apply_pii_rules

    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    p.add_argument("--table", required=True)
    args = p.parse_args()

    cfg = yaml.safe_load(open(args.config))
    tcfg, d = cfg["tables"][args.table], cfg["defaults"]

    spark = (
        SparkSession.builder.appName(f"cdc-{args.table}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    changes = spark.read.parquet(f"{d['raw_path']}/{args.table}")
    if "op" not in changes.columns:
        changes = changes.withColumn("op", F.lit("U"))
    pii = tcfg.get("pii_columns") or {}
    changes = apply_pii_rules(changes, pii.get("hash", []), pii.get("mask", []))
    merge_cdc(spark, changes, f"{d['curated_path']}/{args.table}", tcfg["primary_keys"], tcfg["watermark_column"])


if __name__ == "__main__":
    main()
