# Databricks notebook source
from datetime import datetime, date
from pyspark.sql.functions import date_format, count, sum, lit
from delta.tables import DeltaTable

# COMMAND ----------

spark.read.table("nyctaxi.bronze.yellow_trips_raw")\
    .groupBy( date_format("tpep_pickup_datetime", "yyyy-MM").alias("year_month") ) \
    .agg(count("*").alias("total_records"))\
    .orderBy("year_month")\
    .display()

# COMMAND ----------

spark.read.table("nyctaxi.silver.yellow_trips_cleansed")\
    .groupBy( date_format("tpep_pickup_datetime", "yyyy-MM").alias("year_month") ) \
    .agg(count("*").alias("total_records"))\
    .orderBy("year_month")\
    .display()

# COMMAND ----------

spark.read.table("nyctaxi.silver.yellow_trips_enriched")\
    .groupBy( date_format("tpep_pickup_datetime", "yyyy-MM").alias("year_month") ) \
    .agg(count("*").alias("total_records"))\
    .orderBy("year_month")\
    .display()

# COMMAND ----------

spark.read.table("nyctaxi.gold.daily_trip_summary")\
    .groupBy( date_format("pickup_date", "yyyy-MM").alias("year_month") ) \
    .agg(sum("total_trips").alias("total_records"))\
    .orderBy("year_month")\
    .display()

# COMMAND ----------

spark.read.table("nyctaxi.gold.daily_trip_summary").withColumnRenamed("vg_passengers_per_trip", "avg_passengers_per_trip").write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("nyctaxi.gold.daily_trip_summary")

# COMMAND ----------

spark.read.table("nyctaxi.silver.taxi_zone_lookup").display()

# COMMAND ----------

zone_dt = DeltaTable.forName(spark,"nyctaxi.silver.taxi_zone_lookup")

zone_dt.update(
    condition = "location_id = 1",
    set = { "end_date": lit(None).cast("timestamp") }
)
zone_dt.delete( "location_id = 999" )
zone_dt.delete( "location_id = 1 AND borough = 'NEWARK AIRPORT'" )

zone_dt.toDF().display()




# COMMAND ----------

now = date.today()
col = lit(f"{now.year}-{now.month:02d}-01").cast("date")

spark.createDataFrame([('Cesar', date(1987, 1, 27))], schema='name string, birthday date')\
    .withColumn("today", col)\
    .display()


