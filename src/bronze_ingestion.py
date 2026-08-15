# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Data Ingestion
# MAGIC 
# MAGIC **Purpose**: Ingest raw data from source systems into the Bronze layer
# MAGIC 
# MAGIC **Key Features**:
# MAGIC - Reads from external sources (S3, ADLS, GCS, etc.)
# MAGIC - Minimal transformations
# MAGIC - Preserves raw data with audit metadata
# MAGIC - Supports incremental loads
# MAGIC 
# MAGIC **Medallion Layer**: Bronze (Raw/Landing)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, 
    input_file_name,
    lit,
    col
)
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Get parameters from job
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "default", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Target Catalog: {catalog}")
print(f"Target Schema: {schema}")
print(f"Full Target: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source Configuration

# COMMAND ----------

# Example: Reading from sample data
# Replace with your actual source path (S3, ADLS, etc.)
source_path = "/databricks-datasets/retail-org/customers/"

print(f"Source Path: {source_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Raw Data

# COMMAND ----------

# Read data with audit columns
df_raw = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(source_path)
)

print(f"Records read from source: {df_raw.count():,}")
print(f"Columns: {', '.join(df_raw.columns)}")

# Display sample
print("\nSample data:")
df_raw.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Audit Metadata

# COMMAND ----------

# Add audit columns for tracking
df_bronze = (df_raw
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
    .withColumn("ingestion_date", lit(datetime.now().strftime("%Y-%m-%d")))
    .withColumn("pipeline_run_id", lit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply("jobId")))
)

print(f"Records prepared for Bronze: {df_bronze.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Table

# COMMAND ----------

target_table = f"{catalog}.{schema}.bronze_customers"

print(f"Writing to table: {target_table}")

# Write with Delta optimizations
(df_bronze.write
    .format("delta")
    .mode("overwrite")  # Change to "append" for incremental loads
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"✅ Successfully wrote {df_bronze.count():,} records to {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Table

# COMMAND ----------

# Optimize the Delta table
spark.sql(f"OPTIMIZE {target_table}")
print(f"✅ Table optimized: {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Display final table stats
table_stats = spark.sql(f"""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT source_file) as source_files,
        MIN(ingestion_timestamp) as first_ingestion,
        MAX(ingestion_timestamp) as last_ingestion
    FROM {target_table}
""")

print("\n📊 Bronze Layer Summary:")
table_stats.display()

# COMMAND ----------

dbutils.notebook.exit("Success")
