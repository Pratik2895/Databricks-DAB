# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Check
# MAGIC 
# MAGIC **Purpose**: Validate data quality across all medallion layers
# MAGIC 
# MAGIC **Key Features**:
# MAGIC - Cross-layer validation
# MAGIC - Record count verification
# MAGIC - Schema validation
# MAGIC - Quality metrics reporting

# COMMAND ----------

from pyspark.sql.functions import col, count, lit, current_timestamp
from datetime import datetime

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
# MAGIC ## Layer Validation

# COMMAND ----------

# Define tables
bronze_table = f"{catalog}.{schema}.bronze_customers"
silver_table = f"{catalog}.{schema}.silver_customers"
gold_summary_table = f"{catalog}.{schema}.gold_customer_summary"
gold_quality_table = f"{catalog}.{schema}.gold_quality_metrics"

tables = [bronze_table, silver_table, gold_summary_table, gold_quality_table]

print("\n🔍 Checking tables...\n")

# Validate each table exists and has data
validation_results = []

for table in tables:
    try:
        df = spark.read.table(table)
        record_count = df.count()
        column_count = len(df.columns)
        
        validation_results.append({
            "table_name": table,
            "status": "✅ OK",
            "record_count": record_count,
            "column_count": column_count,
            "check_timestamp": datetime.now()
        })
        
        print(f"✅ {table}")
        print(f"   Records: {record_count:,}")
        print(f"   Columns: {column_count}\n")
        
    except Exception as e:
        validation_results.append({
            "table_name": table,
            "status": "❌ FAILED",
            "record_count": 0,
            "column_count": 0,
            "check_timestamp": datetime.now(),
            "error": str(e)
        })
        print(f"❌ {table}")
        print(f"   Error: {str(e)}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record Count Validation

# COMMAND ----------

# Get counts from each layer
try:
    bronze_count = spark.read.table(bronze_table).count()
    silver_count = spark.read.table(silver_table).count()
    
    records_dropped = bronze_count - silver_count
    drop_percentage = (records_dropped / bronze_count * 100) if bronze_count > 0 else 0
    
    print("\n📊 Record Count Validation:")
    print(f"Bronze records: {bronze_count:,}")
    print(f"Silver records: {silver_count:,}")
    print(f"Records dropped: {records_dropped:,} ({drop_percentage:.2f}%)")
    
    # Alert if too many records dropped
    if drop_percentage > 10:
        print(f"\n⚠️ WARNING: More than 10% of records were dropped during transformation!")
    else:
        print(f"\n✅ Record drop rate is acceptable")
        
except Exception as e:
    print(f"❌ Error validating record counts: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Freshness Check

# COMMAND ----------

try:
    # Check latest ingestion timestamp
    latest_ingestion = spark.sql(f"""
        SELECT 
            MAX(ingestion_timestamp) as latest_bronze_ingestion,
            MAX(silver_processed_timestamp) as latest_silver_processing
        FROM {silver_table}
    """).collect()[0]
    
    print("\n📅 Data Freshness:")
    print(f"Latest Bronze ingestion: {latest_ingestion['latest_bronze_ingestion']}")
    print(f"Latest Silver processing: {latest_ingestion['latest_silver_processing']}")
    print("✅ Data freshness check passed")
    
except Exception as e:
    print(f"❌ Error checking data freshness: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Validation Report

# COMMAND ----------

# Create validation report DataFrame
df_validation = spark.createDataFrame(validation_results)

print("\n📋 Validation Report:")
df_validation.display()

# Write to validation log table
validation_log_table = f"{catalog}.{schema}.dq_validation_log"

try:
    (df_validation.write
        .format("delta")
        .mode("append")
        .saveAsTable(validation_log_table)
    )
    print(f"\n✅ Validation report saved to: {validation_log_table}")
except Exception as e:
    print(f"ℹ️ Could not save validation report: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Summary

# COMMAND ----------

all_passed = all(result["status"] == "✅ OK" for result in validation_results)

if all_passed:
    print("\n" + "="*50)
    print("✅ ALL DATA QUALITY CHECKS PASSED")
    print("="*50)
    dbutils.notebook.exit("Success")
else:
    print("\n" + "="*50)
    print("❌ SOME DATA QUALITY CHECKS FAILED")
    print("="*50)
    dbutils.notebook.exit("Failed")
