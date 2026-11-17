# Databricks notebook source
import re

CATALOG, SCHEMA = "practice", "schema_mapper"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
SOURCES = ["source_a", "source_b", "source_c"]

for src in SOURCES:
    df = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("header", "true")
          .option("cloudFiles.schemaLocation", f"{VOL}/_schemas/{src}")
          .load(f"{VOL}/incoming/{src}/"))
    df = df.toDF(*[re.sub(r"[^0-9a-zA-Z_]", "_", c) for c in df.columns])
    (df.writeStream
       .option("checkpointLocation", f"{VOL}/_checkpoints/{src}")
       .trigger(availableNow=True)
       .toTable(f"{CATALOG}.{SCHEMA}.raw_{src}")
       .awaitTermination())
    print(src, spark.table(f"{CATALOG}.{SCHEMA}.raw_{src}").count(), "rows")
