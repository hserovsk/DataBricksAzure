-- Databricks notebook source
-- MAGIC %python
-- MAGIC hs_account_key = dbutils.secrets.get(scope = 'hs-scopesecret-hs', key = 'storageaccountkey')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set(
-- MAGIC     "fs.azure.account.key.learningdatabrickstorage.dfs.core.windows.net",
-- MAGIC     hs_account_key
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating tables from csv files ###

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt DOUBLE,
  url STRING
)
USING csv
OPTIONS (path "abfss://raw@learningdatabrickstorage.dfs.core.windows.net/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING CSV
OPTIONS (path "abfss://raw@learningdatabrickstorage.dfs.core.windows.net/races.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating tables from json files ###

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Constructors ####
-- MAGIC #### Single Line JSON ####
-- MAGIC #### Simple Structure ####

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS (path "abfss://raw@learningdatabrickstorage.dfs.core.windows.net/constructors.json", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Drivers ####
-- MAGIC #### Single Line JSON ####
-- MAGIC #### COMPLEX Structure ####

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path "abfss://raw@learningdatabrickstorage.dfs.core.windows.net/drivers.json", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Results ####
-- MAGIC #### Single Line JSON ####
-- MAGIC #### Simple Structure ####

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points FLOAT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId INT
)
USING json
OPTIONS (path "abfss://raw@learningdatabrickstorage.dfs.core.windows.net/results.json", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.results;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Pit Stops ####
-- MAGIC #### Multi Line JSON ####
-- MAGIC #### Simple Structure ####

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
)
USING JSON
OPTIONS (path "abfss://raw@learningdatabrickstorage.dfs.core.windows.net/pit_stops.json", multiLine true, header true)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### lap times ####
-- MAGIC ##### multiple csv files ######

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING CSV
OPTIONS (path "abfss://raw@learningdatabrickstorage.dfs.core.windows.net/lap_times_split*.csv", header true)

-- COMMAND ----------

SELECT COUNT(1) FROM f1_raw.lap_times;

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### qulifying multiline json multiple files ####

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING JSON
OPTIONS (path "abfss://raw@learningdatabrickstorage.dfs.core.windows.net/qualifying_split_*.json", header true, multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;

-- COMMAND ----------


