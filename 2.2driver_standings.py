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

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct, when, count, col, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

driver_standings_df = race_results_df \
    .groupBy("race_year", "driver_name", "team", "position") \
    .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(driver_standings_df.filter("race_year = 2020"))

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
driver_standings_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))
display(driver_standings_df.filter("race_year = 2020"))

# COMMAND ----------

#driver_standings_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")
driver_standings_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_presentation.driver_standings;

# COMMAND ----------


