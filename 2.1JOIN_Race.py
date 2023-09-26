# Databricks notebook source
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

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("location", "circuit_location")
display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date")
display(races_df)

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("number", "drive_number") \
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name", "team")

display(constructors_df)

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
    .withColumnRenamed("time", "race_time")
display(results_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id)

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "drive_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position") \
    .withColumn("created_date", current_timestamp())
display(final_df)

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")
final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results;

# COMMAND ----------


