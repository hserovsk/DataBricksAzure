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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType, DateType
from pyspark.sql.functions import col, current_timestamp, current_timestamp

# COMMAND ----------

display(spark.read.csv(f"{raw_folder_path}"))

# COMMAND ----------

laptimes_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

laptimes_df = spark.read \
    .schema(laptimes_schema) \
    .option("multiline", True) \
    .csv(f"{raw_folder_path}/lap_times_split*.csv")

# COMMAND ----------

display(laptimes_df)

# COMMAND ----------

final_laptimes_df = laptimes_df.withColumnRenamed("raceId", "race_id") \
                               .withColumnRenamed("driverId", "driver_id") \
                               .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

final_laptimes_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

display(final_laptimes_df)

# COMMAND ----------

dbutils.notebook.exit("Success")
