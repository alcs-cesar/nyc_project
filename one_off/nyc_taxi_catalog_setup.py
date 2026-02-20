# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Taxi Catalog Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop existing objects

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP CATALOG IF EXISTS nyctaxi CASCADE 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Metastore objects

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG nyctaxi
# MAGIC MANAGED LOCATION 'abfss://unity-catalog-storage@dbstoragedcjzk3ipf4xru.dfs.core.windows.net/7405607775723193'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA nyctaxi.landing;
# MAGIC CREATE SCHEMA nyctaxi.bronze;
# MAGIC CREATE SCHEMA nyctaxi.silver;
# MAGIC CREATE SCHEMA nyctaxi.gold;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME nyctaxi.landing.data_sources