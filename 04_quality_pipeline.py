from pyspark import pipelines as dp
from pyspark.sql import functions as F

SRC = "practice.schema_mapper.businesses_mapped"

rules = {
    "has_name": "business_name IS NOT NULL",
    "has_address": "address IS NOT NULL",
    "has_state": "state IS NOT NULL",
    "valid_zip": "zip IS NOT NULL AND length(zip) = 5",
    "valid_date": "start_date IS NULL OR start_date <= current_date()",
}

@dp.materialized_view(comment="Rows that pass every quality rule")
@dp.expect_all_or_drop(rules)
def businesses_clean():
    return spark.read.table(SRC)

@dp.materialized_view(comment="Rows that fail at least one rule, with the reason")
def businesses_quarantine():
    reason = F.concat_ws(", ", *[F.when(~F.expr(r), F.lit(n)) for n, r in rules.items()])
    return (spark.read.table(SRC)
            .withColumn("failure_reason", reason)
            .where(F.col("failure_reason") != ""))