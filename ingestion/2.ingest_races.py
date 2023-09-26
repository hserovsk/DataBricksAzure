# Databricks notebook source
# MAGIC %md
# MAGIC ### Accessing Azure Data Lake using Access Keys ###

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, current_timestamp, to_timestamp, concat, lit

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)


])

# COMMAND ----------

races_df = spark.read \
    .schema(races_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv", header=True)

# COMMAND ----------

display(races_df)

# COMMAND ----------

renamed_races_df = races_df.withColumnRenamed("raceId", "race_id") \
                           .withColumnRenamed("year", "race_year") \
                           .withColumnRenamed("circuitId", "circuit_id") \
                           .withColumn("race_timestamp", to_timestamp(concat(col("date"),lit(" "),col("time")), "yyyy-MM-dd HH:mm:ss")) \
                           .withColumn("ingestion_date", current_timestamp()) \
                           .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(renamed_races_df)

# COMMAND ----------

final_races_df = renamed_races_df.drop("url")

# COMMAND ----------

final_races_df.printSchema()

# COMMAND ----------

final_races_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/races"))

# COMMAND ----------

dbutils.notebook.exit("Success")
