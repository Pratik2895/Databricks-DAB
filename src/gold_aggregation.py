# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Business Aggregations
# MAGIC 
# MAGIC **Purpose**: Create business-ready aggregated tables in the Gold layer
# MAGIC 
# MAGIC **Key Features**:
# MAGIC - Business metrics and KPIs
# MAGIC - Aggregated dimensions
# MAGIC - Ready for BI tools and dashboards
# MAGIC - Optimized for query performance
# MAGIC 
# MAGIC **Medallion Layer**: Gold (Business/Aggregated)

# COMMAND ----------

from pyspark.sql.functions import (
    count, countDistinct, avg, sum as _sum, 
    max as _max, min as _min, current_timestamp,
    col, when, round as _round, lit
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "default", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Target: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Data

# COMMAND ----------

silver_table = f"{catalog}.{schema}.silver_customers"
df_silver = spark.read.table(silver_table)

print(f"Silver records: {df_silver.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Aggregation 1: Customer Summary

# COMMAND ----------

# Overall customer metrics
df_customer_summary = (df_silver
    .groupBy()
    .agg(
        countDistinct("customer_id").alias("total_customers"),
        count("*").alias("total_records"),
        _sum(when(col("has_email"), 1).otherwise(0)).alias("customers_with_email"),
        _sum(when(col("has_phone"), 1).otherwise(0)).alias("customers_with_phone"),
        _sum(when(col("is_complete"), 1).otherwise(0)).alias("complete_profiles")
    )
    .withColumn("email_coverage_pct", 
        _round((col("customers_with_email") / col("total_customers")) * 100, 2)
    )
    .withColumn("phone_coverage_pct", 
        _round((col("customers_with_phone") / col("total_customers")) * 100, 2)
    )
    .withColumn("completeness_pct", 
        _round((col("complete_profiles") / col("total_customers")) * 100, 2)
    )
    .withColumn("snapshot_date", current_timestamp())
    .withColumn("metric_name", lit("customer_summary"))
)

print("\n📊 Customer Summary Metrics:")
df_customer_summary.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Aggregation 2: Data Quality Metrics

# COMMAND ----------

# Data quality breakdown
df_quality_metrics = (df_silver
    .groupBy("is_complete")
    .agg(
        count("*").alias("customer_count")
    )
    .withColumn("profile_status", 
        when(col("is_complete") == True, "Complete")
        .otherwise("Incomplete")
    )
    .withColumn("snapshot_date", current_timestamp())
    .select(
        "profile_status",
        "customer_count",
        "snapshot_date"
    )
)

print("\n📊 Data Quality Metrics:")
df_quality_metrics.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Gold Tables

# COMMAND ----------

# Write customer summary
summary_table = f"{catalog}.{schema}.gold_customer_summary"

print(f"\nWriting to: {summary_table}")
(df_customer_summary.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(summary_table)
)
print(f"✅ Successfully wrote to {summary_table}")

# Write quality metrics
quality_table = f"{catalog}.{schema}.gold_quality_metrics"

print(f"\nWriting to: {quality_table}")
(df_quality_metrics.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(quality_table)
)
print(f"✅ Successfully wrote to {quality_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Gold Tables

# COMMAND ----------

spark.sql(f"OPTIMIZE {summary_table}")
print(f"✅ Optimized: {summary_table}")

spark.sql(f"OPTIMIZE {quality_table}")
print(f"✅ Optimized: {quality_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Summary

# COMMAND ----------

print("\n📊 Gold Layer Tables Created:")
print(f"1. {summary_table}")
print(f"2. {quality_table}")

print("\n✅ Gold layer aggregation complete!")

# COMMAND ----------

dbutils.notebook.exit("Success")
