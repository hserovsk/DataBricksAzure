# Databricks notebook source
# MAGIC %md
# MAGIC ### Accessing Azure Data Lake using Access Keys ###

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "./includes/configuration"

# COMMAND ----------

# MAGIC %run "./includes/common_functions"

# COMMAND ----------

hs_account_key = dbutils.secrets.get(scope = 'hs-scopesecret-hs', key = 'storageaccountkey')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.learningdatabrickstorage.dfs.core.windows.net",
    hs_account_key
)

# COMMAND ----------

display(dbutils.fs.ls(f"{raw_folder_path}"))

# COMMAND ----------

display(spark.read.csv(f"{raw_folder_path}/pit_stops.json"))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType, DateType
from pyspark.sql.functions import col, current_timestamp, current_timestamp

# COMMAND ----------

pitstops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("stop", StringType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("duration", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pitstops_df = spark.read \
    .option("multiline", True) \
    .schema(pitstops_schema) \
    .json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

display(pitstops_df)

# COMMAND ----------

renamed_pitstops_df = pitstops_df.withColumnRenamed("raceId", "race_id") \
                                 .withColumnRenamed("driverId", "driver_id") \
                                 .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

renamed_pitstops_df.printSchema()

# COMMAND ----------

renamed_pitstops_df.schema.names

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

results_dropped_df = results_dropped_df.select("result_id", "driver_id", "constructor_id", "number", "grid", "position", "position_text",
                                               "position_order", "points", "laps", "time", "milliseconds", "fastest_lap", "rank", "fastest_lap_time",
                                               "fastest_lap_speed", "file_date", "ingestion_date", "race_id")

# COMMAND ----------

#renamed_pitstops_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")
renamed_pitstops_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops;

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------

dbutils.notebook.exit("Success")
