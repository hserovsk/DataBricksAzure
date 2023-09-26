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

constructor_standings_df = race_results_df \
    .groupBy("race_year", "team") \
    .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(constructor_standings_df.filter("race_year = 2020"))

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
constructor_standings_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))
display(constructor_standings_df.filter("race_year = 2020"))

# COMMAND ----------

constructor_standings_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")
constructor_standings_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_presentation.constructor_standings;

# COMMAND ----------


