# Databricks notebook source
from functools import reduce
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F

CATALOG, SCHEMA = "practice", "schema_mapper"
SOURCES = ["source_a", "source_b", "source_c"]
TARGET = ["business_name", "dba_name", "address", "city", "state", "zip", "start_date"]
FORMATS = ["MM/dd/yyyy", "yyyy-MM-dd", "MM/dd/yyyy hh:mm:ss a",
           "yyyy-MM-dd'T'HH:mm:ss.SSS", "yyyy/MM/dd"]
DEFAULT_STATE = {"source_c": "CA"}

def standardize(src):
    raw = spark.table(f"{CATALOG}.{SCHEMA}.raw_{src}")
    m = {r.target_col: r.source_col
         for r in spark.table(f"{CATALOG}.{SCHEMA}.column_mappings").where(F.col("src") == src).collect()
         if r.source_col and r.source_col in raw.columns}
    df = raw.select([F.col(s).cast("string").alias(t) for t, s in m.items()])
    for c in TARGET:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    return (df.select(*TARGET)
              .withColumn("business_name", F.upper(F.trim("business_name")))
              .withColumn("dba_name", F.upper(F.trim("dba_name")))
              .withColumn("address", F.upper(F.trim("address")))
              .withColumn("city", F.initcap(F.trim("city")))
              .withColumn("state", F.upper(F.coalesce(F.trim("state"), F.lit(DEFAULT_STATE.get(src)))))
              .withColumn("zip", F.substring(F.regexp_replace("zip", r"\D", ""), 1, 5))
              .withColumn("start_date", F.coalesce(
                  *[F.try_to_timestamp("start_date", F.lit(f)) for f in FORMATS]).cast("date"))
              .withColumn("source", F.lit(src)))

staged = (reduce(DataFrame.unionByName, [standardize(s) for s in SOURCES])
          .withColumn("record_key", F.sha2(F.concat_ws("|", "business_name", "address", "zip"), 256))
          .dropDuplicates(["record_key"]))

tgt = f"{CATALOG}.{SCHEMA}.businesses_mapped"
if not spark.catalog.tableExists(tgt):
    staged.limit(0).write.saveAsTable(tgt)

(DeltaTable.forName(spark, tgt).alias("t")
   .merge(staged.alias("s"), "t.record_key = s.record_key")
   .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

display(spark.table(tgt).groupBy("source").agg(
    F.count("*").alias("rows"),
    F.sum(F.col("start_date").isNull().cast("int")).alias("missing_dates"),
    F.sum(F.col("state").isNull().cast("int")).alias("missing_state")))

