# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_processed.results;

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType
from pyspark.sql.functions import col, current_timestamp, current_timestamp, lit

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
    .json(f"{raw_folder_path}/{v_file_date}/results.json")

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
                                .withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

results_dropped_df = results_renamed_df.drop("statusId")

# COMMAND ----------

display(results_dropped_df)

# COMMAND ----------

results_dropped_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1 - Incremental Load ###

# COMMAND ----------

#for race_id_list in results_dropped_df.select("race_id").distinct().collect():
#    if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#        spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

#results_dropped_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")
#results_dropped_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Method 2 - Incremental Load ###

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------



# COMMAND ----------

output_df = re_arrange_partition_column(results_dropped_df, "race_id")

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = re_arrange_partition_column(input_df, partition_column)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

overwrite_partition(results_dropped_df, "f1_processed", "results", "race_id")

# COMMAND ----------

#results_dropped_df = results_dropped_df.select("result_id", "driver_id", "constructor_id", "number", "grid", "position", "position_text",
#                                               "position_order", "points", "laps", "time", "milliseconds", "fastest_lap", "rank", "fastest_lap_time",
#                                               "fastest_lap_speed", "file_date", "ingestion_date", "race_id")

# COMMAND ----------

#if (spark._jsparkSession.catalog().tableExists(f"f1_processed.results")):
#    results_dropped_df.write.mode("overwrite").insertInto("f1_processed.results")
#else:
#    results_dropped_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.results;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

check_df = spark.read.parquet(f"{processed_folder_path}/results")

# COMMAND ----------

display(check_df)

# COMMAND ----------

dbutils.notebook.exit("Success")
