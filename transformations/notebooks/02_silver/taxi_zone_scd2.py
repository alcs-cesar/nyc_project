# Databricks notebook source
# MAGIC %md
# MAGIC # Taxi Zone Lookup

# COMMAND ----------

from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import current_timestamp, lit, col, when

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input Parameters

# COMMAND ----------

zone_landing_loc = "/Volumes/nyctaxi/landing/data_sources/lookup/taxi_zone_lookup.csv"
enriched_zone_loc = "nyctaxi.silver.taxi_zone_lookup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Input Data

# COMMAND ----------

def ingested_zone(ingested_zone_loc: str) -> DataFrame:
    return spark.read.format("csv").options(header=True, inferSchema=True).load(ingested_zone_loc)

def with_standard_schema(ingested: DataFrame) -> DataFrame:
    return ingested.select(
        col("LocationID").cast("int").alias("location_id"),
        col("Borough").alias("borough"),
        col("Zone").alias("zone"),
        col("service_zone").alias("service_zone"),
        current_timestamp().alias("effective_date"),
        lit(None).cast("timestamp").alias("end_date")
    )

def with_new_and_updated(ingested: DataFrame) -> DataFrame:
    ## new
    new_zone = spark.createDataFrame(
        [(999, "New Borough", "New Zone", "New Service Zone")],
        schema = "location_id int, borough string, zone string, service_zone string"
    ).withColumns({
        'effctive_date': current_timestamp(),
        'end_date': lit(None).cast("timestamp")
    })

    ingested_change = ingested.union(new_zone)

    ## updated
    return ingested_change.withColumn( "borough", when(col("location_id") == 1, "NEWARK AIRPORT").otherwise(col("borough")) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Logic

# COMMAND ----------

def zone_changing_dim(ingested_zone: DataFrame, enriched_zone_loc: str) -> DataFrame:
            
        
    ingested = ingested_zone.alias("i")
    existent_zones_dt = DeltaTable.forName(spark, enriched_zone_loc)
    end_timestamp = datetime.now()

    change_predicate = \
        ( col("a.borough") != col("i.borough") ) | \
        ( col("a.zone") != col("i.zone") ) | \
        ( col("a.service_zone") != col("i.service_zone") )

    same_zone = ( col("a.location_id") == col("i.location_id") )
    still_active = col("a.end_date").isNull()

    ## inactivate changed zones
    print("Closing existing zone records...")
    existent_zones_dt.alias("a")\
        .merge(
            source = ingested.alias("i"),
            condition = same_zone & still_active & change_predicate
        )\
        .whenMatchedUpdate(
            set = {
                "end_date": lit(end_timestamp).cast("timestamp")
            }
        )\
        .execute()

    print("Closed existing zone records")

    ## activate changed zones
    print("Activating existing zones (new records)...")

    changed_zone_ids = [
        row.location_id 
        for row in existent_zones_dt.toDF().filter(f"end_date = '{end_timestamp}'").select("location_id").collect()
        ]
    
    location_not_changed = ~col("i.location_id").isin(changed_zone_ids)

    print(f"changed_zones: {changed_zone_ids}")

    existent_zones_dt.alias("a")\
        .merge(
            source = ingested.alias("i"),
            condition = location_not_changed
        )\
        .whenNotMatchedInsert(
            values = {
                "location_id": col("i.location_id"),
                "borough": col("i.borough"),
                "zone": col("i.zone"),
                "service_zone": col("i.service_zone"),
                "effective_date": current_timestamp(),
                "end_date": lit(None).cast("timestamp")
            }
        )\
        .execute()

    print("Activated existing zones (new records)")

    ## new ingested zones
    print("Inserting new zones..")
    if changed_zone_ids:
        existent_zones_dt.alias("a")\
            .merge(
                source = ingested.alias("i"),
                condition = same_zone
            )\
            .whenNotMatchedInsert(
                values = {
                    "location_id": col("i.location_id"),
                    "borough": col("i.borough"),
                    "zone": col("i.zone"),
                    "service_zone": col("i.service_zone"),
                    "effective_date": current_timestamp(),
                    "end_date": lit(None).cast("timestamp")
                }
            )\
            .execute()
    print("Inserted new zones")
        

# COMMAND ----------

# MAGIC %md
# MAGIC ## High Level Steps

# COMMAND ----------

raw = ingested_zone(zone_landing_loc)
raw_with_schema = with_standard_schema(raw)
ingested_df = with_new_and_updated(raw_with_schema)


print("Ready to be processed the ingested zones")
zone_scd = zone_changing_dim(
    ingested_zone = ingested_df, 
    enriched_zone_loc = enriched_zone_loc
)