# Databricks notebook source
# MAGIC %pip install dbldatagen

# COMMAND ----------

import dbldatagen as dg

dbfs_path = "/landing"
jsons_path_data = f"{dbfs_path}/data/trackingevent.json"
dfSource = spark.read.format("json").option("multiline", "true").load(jsons_path_data)
#display(dfSource)

code =  dg.DataAnalyzer.scriptDataGeneratorFromSchema(dfSource.schema)

# COMMAND ----------


# Code snippet generated with Databricks Labs Data Generator (`dbldatagen`) DataAnalyzer class
# Install with `pip install dbldatagen` or in notebook with `%pip install dbldatagen`
# See the following resources for more details:
#
#   Getting Started - [https://databrickslabs.github.io/dbldatagen/public_docs/APIDOCS.html]
#   Github project - [https://github.com/databrickslabs/dbldatagen]
#
import dbldatagen as dg
import pyspark.sql.types

# Column definitions are stubs only - modify to generate correct data  
#
generation_spec = (
    dg.DataGenerator(sparkSession=spark, 
                     name='synthetic_data', 
                     rows=100000,
                     random=True,
                     )
    .withColumn('chassisNumber', 'string', template=r'\\w')
    .withColumn('chassisSeries', 'string', template=r'\\w')
    .withColumn('dataContentName', 'string', template=r'\\w')
    .withColumn('dataItems', 'struct<adBlueLevel:struct<lastChangeTimestamp:bigint,percent:double,timestamp:bigint>,bufferedTopRpmLogWithTimestamp:struct<lastChangeTimestamp:bigint,list:array<struct<lastChangeTimestamp:bigint,timestamp:bigint,values:struct<timestamp:struct<incomingTimestampValue:bigint,lastChangeTimestamp:bigint,timestamp:bigint>,topRpm:struct<lastChangeTimestamp:bigint,rotationsPerMinute:double,timestamp:bigint>>>>,timestamp:bigint>,bufferedTopSpeedLogWithTimestamp:struct<lastChangeTimestamp:bigint,list:array<struct<lastChangeTimestamp:bigint,timestamp:bigint,values:struct<timestamp:struct<incomingTimestampValue:bigint,lastChangeTimestamp:bigint,timestamp:bigint>,topSpeed:struct<kilometersPerHour:double,lastChangeTimestamp:bigint,timestamp:bigint>>>>,timestamp:bigint>,driveSessionId:struct<idReference:bigint,lastChangeTimestamp:bigint,timestamp:bigint>,engineStatus:struct<lastChangeTimestamp:bigint,state:string,timestamp:bigint>,lovEngineTime:struct<lastChangeTimestamp:bigint,seconds:bigint,timestamp:bigint>,lovVehicleDistance:struct<lastChangeTimestamp:bigint,meters:bigint,timestamp:bigint>,position:struct<altitude:bigint,gpsTimestamp:bigint,heading:bigint,lastChangeTimestamp:bigint,latitude:struct<latitude:bigint>,longitude:struct<longitude:bigint>,timestamp:bigint>,speed:struct<kilometersPerHour:double,lastChangeTimestamp:bigint,timestamp:bigint>,totalFuelLevel:struct<lastChangeTimestamp:bigint,timestamp:bigint,values:struct<diesel:struct<lastChangeTimestamp:bigint,percent:double,timestamp:bigint>>>>', expr='null')
    .withColumn('documentId', 'decimal(21,0)', minValue=0, maxValue=1000)
    .withColumn('expireAt', 'string', template=r'\\w')
    .withColumn('fleetOrganizationTimeZones', 'struct<fleetOrganizationPlatformId:string,timeZone:string>', expr='null', structType='array', numFeatures=(2,6))
    .withColumn('labels', 'string', template=r'\\w', structType='array', numFeatures=(2,6))
    .withColumn('platformDriveSessionIdentifier', 'string', template=r'\\w')
    .withColumn('platformFleetOrganizationIdentifiers', 'string', template=r'\\w', structType='array', numFeatures=(2,6))
    .withColumn('platformVehicleIdentifier', 'string', template=r'\\w')
    .withColumn('shardKey', 'bigint', minValue=0, maxValue=1000000)
    .withColumn('trackingTime', 'bigint', minValue=0, maxValue=1000000)
    .withColumn('triggerData', 'struct<lastChangeTimestamp:bigint,name:string,timestamp:bigint,values:struct<changeReason:struct<lastChangeTimestamp:bigint,state:string,timestamp:bigint>,previousDriveSession:struct<idReference:bigint,lastChangeTimestamp:bigint,timestamp:bigint>>>', expr='null')
    .withColumn('triggerTime', 'bigint', minValue=0, maxValue=1000000)
    .withColumn('triggerType', 'string', template=r'\\w')
    .withColumn('version', 'bigint', minValue=0, maxValue=1000000)
    .withColumn('vin', 'string', template=r'\\w')
    )

# COMMAND ----------

dfTestData = generation_spec.build()
display(dfTestData,10)

# COMMAND ----------

from pyspark.sql.functions import *

df_add_id = dfTestData.withColumn("combinedPK", concat_ws("-", "chassisNumber", "chassisSeries", "vin")) \
                     .withColumn("WK_PK", monotonically_increasing_id()) \
                     .withColumn("first_label", col("labels").getItem(0)) \
                     .select("version","triggerType", "dataContentName", "platformVehicleIdentifier", "platformFleetOrganizationIdentifiers") \
                     .orderBy(col("version").desc())
#display(df_add_id)

# COMMAND ----------

import dlt
@dlt.create_table(name="generated_tracking_data", comment="testing pipeline")
@dlt.expect_or_drop("valid_vin", "vin IS NOT NULL") 
def generated_tracking_data():
    return (
        dlt.read("df_add_id").select( "*" )
    )
