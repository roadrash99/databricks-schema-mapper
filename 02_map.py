# Databricks notebook source
import json, mlflow
from pyspark.sql import functions as F

CATALOG, SCHEMA = "practice", "schema_mapper"
SOURCES = ["source_a", "source_b", "source_c"]
TARGET = ["business_name", "dba_name", "address", "city", "state", "zip", "start_date"]
ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

@mlflow.trace
def propose_mapping(src):
    df = spark.table(f"{CATALOG}.{SCHEMA}.raw_{src}")
    cols = [c for c in df.columns if not c.startswith("_")]
    profile = {c: [r[0] for r in df.select(c).where(F.col(c).isNotNull()).limit(3).collect()]
               for c in cols}
    prompt = (
        f"You are mapping columns from a business registry file to this target schema: {TARGET}. "
        f"For each target field, pick the single best source column, or null if none fits. "
        f"Use each source column at most once. Return only JSON, no explanation, no markdown, "
        f'in the form {{"target_field": "source_column"}}. '
        f"Source columns with sample values: {json.dumps(profile, default=str)}"
    )
    out = spark.sql(f"SELECT ai_query('{ENDPOINT}', :p) AS m", args={"p": prompt}).first()["m"]
    return json.loads(out[out.find("{"): out.rfind("}") + 1])

rows = []
for src in SOURCES:
    mapping = propose_mapping(src)
    for target, source_col in mapping.items():
        rows.append((src, source_col, target))

(spark.createDataFrame(rows, "src string, source_col string, target_col string")
      .write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.column_mappings"))

display(spark.table(f"{CATALOG}.{SCHEMA}.column_mappings").orderBy("src", "target_col"))