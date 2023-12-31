# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType
from pyspark.sql.functions import col, current_timestamp, current_timestamp

# COMMAND ----------



# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                     StructField("raceId", IntegerType(), True),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("constructorId", IntegerType(), True),
                                     StructField("number", IntegerType(), True),
                                     StructField("grid", IntegerType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("positionText", StringType(), True),
                                     StructField("positionOrder", IntegerType(), True),
                                     StructField("points", FloatType(), True),
                                     StructField("laps", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True),
                                     StructField("fastestLap", IntegerType(), True),
                                     StructField("rank", IntegerType(), True),
                                     StructField("fastestLapTime", StringType(), True),
                                     StructField("fastestLapSpeed", FloatType(), True),
                                     StructField("statusId", IntegerType(), True),

])

# COMMAND ----------

results_df = spark.read \
    .schema(results_schema) \
    .json(f"{raw_folder_path}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId", "result_id") \
                                .withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                .withColumnRenamed("constructorId", "constructor_id") \
                                .withColumnRenamed("positionText", "position_text") \
                                .withColumnRenamed("positionOrder", "position_order") \
                                .withColumnRenamed("fastestLap", "fastest_lap") \
                                .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

results_dropped_df = results_renamed_df.drop("statusId")

# COMMAND ----------

display(results_dropped_df)

# COMMAND ----------

results_dropped_df.printSchema()

# COMMAND ----------

results_dropped_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")

# COMMAND ----------

check_df = spark.read.parquet(f"{processed_folder_path}/results")

# COMMAND ----------

display(check_df)

# COMMAND ----------

dbutils.notebook.exit("Success")
