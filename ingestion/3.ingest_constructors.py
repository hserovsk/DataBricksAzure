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

constructors_schema = StructType(fields=[StructField("constructorId", IntegerType(), False),
                                         StructField("constructorRef", StringType(), True),
                                         StructField("name", StringType(), True),
                                         StructField("nationality", StringType(), True),
                                         StructField("url", StringType(), True)

])

# COMMAND ----------

constructors_df = spark.read \
    .schema(constructors_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_renamed_df = constructors_df.withColumnRenamed("constructorId", "constructor_id") \
                                         .withColumnRenamed("constructorRef", "constructor_ref") \
                                         .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

constructors_renamed_df = add_ingestion_date(constructors_renamed_df)

# COMMAND ----------

constructors_final_df = constructors_renamed_df.drop("url")

# COMMAND ----------

constructors_final_df.printSchema()

# COMMAND ----------

#constructors_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")
constructors_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/constructors"))

# COMMAND ----------

dbutils.notebook.exit("Success")
