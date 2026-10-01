"""AWS Glue entry point: JDBC incremental extract -> raw S3 -> CDC merge."""
import sys

import yaml
from awsglue.context import GlueContext  # type: ignore
from awsglue.job import Job  # type: ignore
from awsglue.utils import getResolvedOptions  # type: ignore
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from src.cdc_merge import merge_cdc
from src.masking import apply_pii_rules
from src.watermark import incremental_query

args = getResolvedOptions(sys.argv, ["JOB_NAME", "TABLE", "CONFIG_PATH", "JDBC_URL", "SECRET_ID", "WATERMARK"])
sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

cfg = yaml.safe_load(open(args["CONFIG_PATH"]))
t, d = cfg["tables"][args["TABLE"]], cfg["defaults"]

src_df = (
    spark.read.format("jdbc")
    .option("url", args["JDBC_URL"])
    .option("dbtable", incremental_query(t["source"], t["watermark_column"], args["WATERMARK"]))
    .option("secretId", args["SECRET_ID"])
    .load()
    .withColumn("op", F.lit("U"))
)
src_df.write.mode("append").parquet(f"{d['raw_path']}/{args['TABLE']}")
pii = t.get("pii_columns") or {}
merge_cdc(spark, apply_pii_rules(src_df, pii.get("hash", []), pii.get("mask", [])),
          f"{d['curated_path']}/{args['TABLE']}", t["primary_keys"], t["watermark_column"])
job.commit()
