# Databricks notebook source
# MAGIC %md
# MAGIC ### Taks 1 circuits.csv ###

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = "hs-scopesecret-hs")

# COMMAND ----------

hs_account_key = dbutils.secrets.get(scope = 'hs-scopesecret-hs', key = 'storageaccountkey')

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "./includes/configuration"

# COMMAND ----------

# MAGIC %run "./includes/common_functions"

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.learningdatabrickstorage.dfs.core.windows.net",
    hs_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@learningdatabrickstorage.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@learningdatabrickstorage.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("long", DoubleType(), True),
                                     StructField("alt", DoubleType(), True),
                                     StructField("url", StringType(), True)
                                     ])

# COMMAND ----------

circuits_df = spark.read \
    .schema(circuits_schema) \
    .csv(f"{raw_folder_path}/circuits.csv", header=True)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

renamed_circuits_df = circuits_df.withColumnRenamed("circuitId", "circuit_id") \
                                 .withColumnRenamed("circuitRef", "circuit_ref") \
                                 .withColumnRenamed("lat", "latitude") \
                                 .withColumnRenamed("long", "longitude") \
                                 .withColumnRenamed("alt", "altitude") \
                                 .withColumn("data_source", lit(v_data_source))


# COMMAND ----------

renamed_circuits_df = add_ingestion_date(renamed_circuits_df)

# COMMAND ----------

renamed_circuits_df.printSchema()

# COMMAND ----------

final_circuits_df = renamed_circuits_df.drop("url")

# COMMAND ----------

final_circuits_df.printSchema()

# COMMAND ----------

display(final_circuits_df)

# COMMAND ----------

final_circuits_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

dbutils.notebook.exit("Success")
