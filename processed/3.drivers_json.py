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

display(spark.read.csv(f"{raw_folder_path}/circuits.csv"))

# COMMAND ----------

from pyspark.sql.types import StringType, DoubleType, StructField, StructType, IntegerType, DateType
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, concat

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
                                 ])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema), # adding schema created before 
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)
                                    ])

# COMMAND ----------

drivers_df = spark.read \
    .schema(drivers_schema) \
    .json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

renamed_drivers_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                              .withColumnRenamed("driverRef", "driver_ref") \
                              .withColumn("ingestion_date", current_timestamp()) \
                              .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

final_drivers_df = renamed_drivers_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/drivers"))

# COMMAND ----------

dbutils.notebook.exit("Success")
