# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e8b06649-cbdc-4b98-89b9-ad244d20a675",
# META       "default_lakehouse_name": "dataml_lakehouse",
# META       "default_lakehouse_workspace_id": "eda47f07-ebf2-41e7-8215-a7c5aa53a9d8"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import pytz

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the current time in UTC
utc_now = datetime.utcnow()

# Convert to EST timezone
est_tz = pytz.timezone('US/Eastern')
est_now = utc_now.replace(tzinfo=pytz.utc).astimezone(est_tz)

# Format the timestamp in the desired format
ingestion_date = est_now.strftime("%Y-%m-%d %H:%M:%S")

# Define the function to add source_system, ingestion_date, and HASH_ID
def add_metadata_columns(df, p_DataSourceName, ingestion_date, hash_columns):
    
    return df.withColumn("source_system", lit(p_DataSourceName)) \
             .withColumn("ingestion_date", lit(ingestion_date)) \
             .withColumn("HASH_ID", sha2(concat_ws("-", *[col(c) for c in hash_columns]), 256))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

p_Domain = ''
p_ProjectName = ''
p_DataSourceName = ''
p_PlatformName = ''
p_EntityName = ''
landing_folder_path = ''
p_ProcessingTime = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Path setup
trips_path = f"{landing_folder_path}/{p_PlatformName}/{p_Domain}/{p_ProjectName}/{p_DataSourceName}/{p_EntityName}"

# Define the schema for the trips dataset
trips_schema = StructType([
    StructField("ride_id", StringType(), False),               # Unique identifier for each trip
    StructField("rideable_type", StringType(), True),          # Type of bike used for the trip
    StructField("started_at", TimestampType(), True),          # Start time of the trip
    StructField("ended_at", TimestampType(), True),            # End time of the trip
    StructField("start_station_name", StringType(), True),     # Name of the starting station
    StructField("start_station_id", StringType(), True),       # Identifier of the starting station
    StructField("end_station_name", StringType(), True),       # Name of the ending station
    StructField("end_station_id", StringType(), True),         # Identifier of the ending station
    StructField("start_lat", DoubleType(), True),              # Latitude of the starting station
    StructField("start_lng", DoubleType(), True),              # Longitude of the starting station
    StructField("end_lat", DoubleType(), True),                # Latitude of the ending station
    StructField("end_lng", DoubleType(), True),                # Longitude of the ending station
    StructField("member_casual", StringType(), True)           # Indicates if the rider is a member or casual user
])

# Load the CSV file into a DataFrame and extract the filename
trips_bronze_df = spark.read.format("csv").option('header', 'true').schema(trips_schema).load(trips_path) \
    .withColumn(
        "FileName", 
        regexp_replace(split(input_file_name(), r"\?").getItem(0), r".*/", "")
    ) \
    .withColumn("ProcessingTime", lit(p_ProcessingTime).cast(TimestampType()))
                      

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if the FileName already exists in the target table
existing_files = (
    spark.sql("SELECT DISTINCT FileName FROM citibikes_bronze.raw_trips").rdd.flatMap(lambda x: x).collect()
)

# Filter out files that are already processed
new_trips_bronze_df = trips_bronze_df.filter(~col("FileName").isin(existing_files))

# Append the filtered data to the Delta table
if not new_trips_bronze_df.isEmpty():
    new_trips_bronze_df.write.format("delta").mode("append").saveAsTable("citibikes_bronze.raw_trips")
else:
    print("No new files to append.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write the Bronze table to Delta format
trips_bronze_df.write.format("delta").mode("append").saveAsTable("citibikes_bronze.raw_trips")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Bronze Table To Silver Zone; DQ Checks & Incremental Load

# MARKDOWN ********************

# #### Data Quality Checks I

# CELL ********************

filtered_trips_df = spark.sql(f"""
SELECT
    ride_id, rideable_type, started_at, ended_at, start_station_name,
    start_station_id, end_station_name, end_station_id, start_lat, start_lng, 
    end_lat, end_lng, member_casual, FileName, ProcessingTime
FROM (
    SELECT 
        (UNIX_TIMESTAMP(ended_at) - UNIX_TIMESTAMP(started_at)) / 60 AS trip_duration_mins,
        ride_id, rideable_type, started_at, ended_at, start_station_name,
        start_station_id, end_station_name, end_station_id, start_lat, start_lng, 
        end_lat, end_lng, member_casual, FileName, ProcessingTime
    FROM citibikes_bronze.raw_trips
) s
WHERE 
    trip_duration_mins > 0
    AND start_station_id IS NOT NULL
    AND end_station_id IS NOT NULL
    AND start_station_id <> end_station_id
    AND CAST(ProcessingTime AS TIMESTAMP) = '{p_ProcessingTime}'
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Data Quality Check II: Not Neccessary

# CELL ********************

# Step 2: Clean non-numeric station IDs
cleaned_station_ids_df = spark.sql("""
SELECT 
ride_id, rideable_type, started_at, ended_at, start_station_name,
    Replace(start_station_id, '_Pillar', '_Pillar') AS start_station_id, 
    end_station_name, end_station_id, start_lat, start_lng, 
    end_lat, end_lng, member_casual, FileName, ProcessingTime
FROM citibikes_bronze.raw_trips
WHERE 
    NOT (start_station_id RLIKE '^[+-]?\\d+(\\.\\d+)?$')
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 3: Combine both filters into the final DataFrame
# Alias DataFrames
filtered_alias = filtered_trips_df.alias("filtered")
cleaned_alias = cleaned_station_ids_df.alias("cleaned")

# Perform the join and disambiguate columns using aliases
trips_bronze_table = (
    filtered_alias
    .join(
        cleaned_alias,
        col("filtered.ride_id") == col("cleaned.ride_id"),  # Join on common fields
        "left"
    )
    .select(
        col("filtered.ride_id").alias("RideId"),
        col("filtered.rideable_type").alias("RideableType"),
        col("filtered.started_at").alias("StartedAt"),
        col("filtered.ended_at").alias("EndedAt"),
        col("filtered.start_station_name").alias("StartStationName"),
        
        col("cleaned.start_station_id").alias("StartStationId"),
        col("filtered.end_station_name").alias("EndStationName"),
        col("filtered.end_station_id").alias("EndStationId"),
        col("filtered.start_lat").alias("StartLat"),
        col("filtered.start_lng").alias("StartLng"),
        col("filtered.end_lat").alias("EndLat"),
        col("filtered.end_lng").alias("EndLng"),
        col("filtered.member_casual").alias("MemberCasual"),
        col("filtered.FileName").alias("FileName"),
        col("filtered.ProcessingTime").alias("ProcessingTime")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the column list to concatenate for the hash key
hash_columns = [
    'RideId', 'RideableType', 'StartedAt', 'EndedAt', 'StartStationName',
    'StartStationId', 'EndStationName', 'EndStationId', 'StartLat', 'StartLng',
    'EndLat', 'EndLng', 'MemberCasual', 'FileName'
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

trips_bronze_table  = add_metadata_columns(trips_bronze_table, p_DataSourceName, ingestion_date, hash_columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

trips_bronze_table = trips_bronze_table.select(
    col("RideId"),
    col("RideableType"),
    col("StartedAt"),
    col("EndedAt"),
    col("StartStationName"),
    col("StartStationId"),
    col("EndStationName"),
    col("EndStationId"),
    col("StartLat"),
    col("StartLng"),
    col("EndLat"),
    col("EndLng"),
    col("MemberCasual"),
    col("FileName"),
    col("ProcessingTime"),
    col("source_system").alias("SourceSystem"),
    col("ingestion_date").alias("IngestionDate"),
    col("HASH_ID")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a temporary view from your DataFrame
trips_bronze_table.createOrReplaceTempView("vw_trips")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC MERGE INTO citibikes_silver.trips AS tgt
# MAGIC USING vw_trips AS src
# MAGIC ON tgt.RideId = src.RideId
# MAGIC WHEN MATCHED AND src.HASH_ID <> tgt.HASH_ID THEN
# MAGIC   UPDATE SET 
# MAGIC     tgt.RideableType = src.RideableType,
# MAGIC     tgt.StartedAt = src.StartedAt,
# MAGIC     tgt.EndedAt = src.EndedAt,
# MAGIC     tgt.StartStationName = src.StartStationName,
# MAGIC     tgt.StartStationId = src.StartStationId,
# MAGIC     tgt.EndStationName = src.EndStationName,
# MAGIC     tgt.EndStationId = src.EndStationId,
# MAGIC     tgt.StartLat = src.StartLat,
# MAGIC     tgt.StartLng = src.StartLng,
# MAGIC     tgt.EndLat = src.EndLat,
# MAGIC     tgt.EndLng = src.EndLng,
# MAGIC     tgt.MemberCasual = src.MemberCasual,
# MAGIC     tgt.FileName = src.FileName,
# MAGIC     tgt.ProcessingTime = src.ProcessingTime,
# MAGIC     tgt.SourceSystem = src.SourceSystem,
# MAGIC     tgt.IngestionDate = src.IngestionDate,
# MAGIC     tgt.HASH_ID = src.HASH_ID
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     RideId, RideableType, StartedAt, EndedAt, StartStationName, StartStationId,
# MAGIC     EndStationName, EndStationId, StartLat, StartLng, EndLat, EndLng, MemberCasual,
# MAGIC     FileName, ProcessingTime, SourceSystem, IngestionDate, HASH_ID
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.RideId, src.RideableType, src.StartedAt, src.EndedAt, src.StartStationName, src.StartStationId,
# MAGIC     src.EndStationName, src.EndStationId, src.StartLat, src.StartLng, src.EndLat, src.EndLng, src.MemberCasual,
# MAGIC     src.FileName, src.ProcessingTime, src.SourceSystem, src.IngestionDate, src.HASH_ID
# MAGIC   )


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
