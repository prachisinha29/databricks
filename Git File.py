# Databricks notebook source
import dlt
import pyspark.sql.functions as F

jsons_path_data = "/landing/raw_jsons/"


@dlt.table
def tracking_bronze_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(jsons_path_data)
        .select(
                F.unix_timestamp().alias("ingestion_timestamp"), 
                F.input_file_name().alias("file_name"),
                "*"
                )
        )

# COMMAND ----------

@dlt.create_table(name="tracking_silver", comment="testing pipeline")
@dlt.expect_or_drop("valid_id", "id IS NOT NULL") 
def tracking_silver():
    return (
        dlt.read("tracking_bronze_raw").select(       
            F.col("id").alias("new_id"),
            "*"                              
        ).withColumn("another_id", F.monotonically_increasing_id())
    )

# COMMAND ----------

@dlt.create_table(name="tracking_gold", comment="testing pipeline")

def tracking_gold():
    return (
        dlt.read("tracking_silver").groupBy("country").agg(F.count("*").alias("row_count"))
    )

# COMMAND ----------

print("This is test.")
