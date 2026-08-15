# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Data Transformation
# MAGIC 
# MAGIC **Purpose**: Transform Bronze data into cleaned Silver layer tables
# MAGIC 
# MAGIC **Key Features**:
# MAGIC - Data quality checks and validation
# MAGIC - Deduplication
# MAGIC - Type casting and standardization
# MAGIC - Business logic transformations
# MAGIC - Remove PII or apply masking
# MAGIC 
# MAGIC **Medallion Layer**: Silver (Cleansed/Conformed)

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, upper, lower, regexp_replace, 
    when, coalesce, current_timestamp,
    row_number, desc
)
from pyspark.sql.types import StringType, IntegerType, DateType
from pyspark.sql.window import Window

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
# MAGIC ## Read Bronze Data

# COMMAND ----------

bronze_table = f"{catalog}.{schema}.bronze_customers"
df_bronze = spark.read.table(bronze_table)

print(f"Bronze records: {df_bronze.count():,}")
print(f"Bronze columns: {', '.join(df_bronze.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# Check for data quality issues
print("\n🔍 Data Quality Checks:")

# Null checks
null_counts = df_bronze.select([
    col(c).isNull().cast("int").alias(c) 
    for c in df_bronze.columns
]).agg(*[
    sum(c).alias(c) 
    for c in df_bronze.columns
])

print("\nNull counts per column:")
null_counts.display()

# Duplicate check
duplicate_count = df_bronze.count() - df_bronze.dropDuplicates(["customer_id"]).count()
print(f"\nDuplicates found: {duplicate_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Cleansing & Transformation

# COMMAND ----------

# Define window for deduplication (keep latest record)
window_spec = Window.partitionBy("customer_id").orderBy(desc("ingestion_timestamp"))

# Clean and transform data
df_silver = (df_bronze
    # Deduplication: Keep the latest record per customer_id
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
    
    # Filter out invalid records
    .filter(col("customer_id").isNotNull())
    .filter(col("customer_id") != "")
    
    # Data type casting
    .withColumn("customer_id", col("customer_id").cast(IntegerType()))
    
    # String cleaning and standardization
    .withColumn("customer_name", 
        when(col("customer_name").isNotNull(), trim(col("customer_name")))
        .otherwise(None)
    )
    .withColumn("email", 
        when(col("email").isNotNull(), lower(trim(col("email"))))
        .otherwise(None)
    )
    .withColumn("phone", 
        when(col("phone").isNotNull(), regexp_replace(col("phone"), "[^0-9]", ""))
        .otherwise(None)
    )
    
    # Add data quality flags
    .withColumn("has_email", col("email").isNotNull())
    .withColumn("has_phone", col("phone").isNotNull())
    .withColumn("is_complete", 
        col("customer_name").isNotNull() & 
        col("email").isNotNull() & 
        col("phone").isNotNull()
    )
    
    # Add processing timestamp
    .withColumn("silver_processed_timestamp", current_timestamp())
    
    # Select final columns
    .select(
        "customer_id",
        "customer_name",
        "email",
        "phone",
        "has_email",
        "has_phone",
        "is_complete",
        "ingestion_timestamp",
        "silver_processed_timestamp"
    )
)

print(f"\n✅ Silver records after cleansing: {df_silver.count():,}")
print(f"Records removed: {df_bronze.count() - df_silver.count():,}")

# Display sample
print("\nSample cleaned data:")
df_silver.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Summary

# COMMAND ----------

quality_summary = df_silver.agg(
    count("*").alias("total_records"),
    sum(when(col("has_email"), 1).otherwise(0)).alias("records_with_email"),
    sum(when(col("has_phone"), 1).otherwise(0)).alias("records_with_phone"),
    sum(when(col("is_complete"), 1).otherwise(0)).alias("complete_records")
)

print("\n📊 Data Quality Summary:")
quality_summary.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Table

# COMMAND ----------

target_table = f"{catalog}.{schema}.silver_customers"

print(f"Writing to table: {target_table}")

(df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"✅ Successfully wrote {df_silver.count():,} records to {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Table

# COMMAND ----------

spark.sql(f"OPTIMIZE {target_table}")
print(f"✅ Table optimized: {target_table}")

# COMMAND ----------

dbutils.notebook.exit("Success")
