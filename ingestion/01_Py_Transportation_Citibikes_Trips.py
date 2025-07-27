# CitiBike Bronze to Silver Pipeline - Patrick Okare

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import pytz

# Current time in Eastern Timezone
est_now = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone('US/Eastern'))
ingestion_date = est_now.strftime("%Y-%m-%d %H:%M:%S")

# Function to append metadata

def add_metadata_columns(df, source_system, ingestion_date, hash_columns):
    return df.withColumn("source_system", lit(source_system)) \
             .withColumn("ingestion_date", lit(ingestion_date)) \
             .withColumn("HASH_ID", sha2(concat_ws("-", *[col(c) for c in hash_columns]), 256))

# Parameters (set via notebook UI or script)
p_Domain = ''
p_ProjectName = ''
p_DataSourceName = ''
p_PlatformName = ''
p_EntityName = ''
landing_folder_path = ''
p_ProcessingTime = ''

# Define path
trips_path = f"{landing_folder_path}/{p_PlatformName}/{p_Domain}/{p_ProjectName}/{p_DataSourceName}/{p_EntityName}"

# Schema for raw trip data
trips_schema = StructType([
    StructField("ride_id", StringType(), False),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("ended_at", TimestampType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("start_lat", DoubleType(), True),
    StructField("start_lng", DoubleType(), True),
    StructField("end_lat", DoubleType(), True),
    StructField("end_lng", DoubleType(), True),
    StructField("member_casual", StringType(), True)
])

# Read raw files
trips_bronze_df = spark.read.format("csv").option("header", "true").schema(trips_schema).load(trips_path) \
    .withColumn("FileName", regexp_replace(split(input_file_name(), r"\\?").getItem(0), r".*/", "")) \
    .withColumn("ProcessingTime", lit(p_ProcessingTime).cast(TimestampType()))

# Filter already processed files
existing_files = spark.sql("SELECT DISTINCT FileName FROM citibikes_bronze.raw_trips").rdd.flatMap(lambda x: x).collect()
new_trips_bronze_df = trips_bronze_df.filter(~col("FileName").isin(existing_files))

if not new_trips_bronze_df.isEmpty():
    new_trips_bronze_df.write.format("delta").mode("append").saveAsTable("citibikes_bronze.raw_trips")
else:
    print("No new files to append.")

# Data Quality Check I
filtered_trips_df = spark.sql(f"""
SELECT * FROM (
    SELECT *, 
        (UNIX_TIMESTAMP(ended_at) - UNIX_TIMESTAMP(started_at)) / 60 AS trip_duration_mins
    FROM citibikes_bronze.raw_trips
) WHERE 
    trip_duration_mins > 0
    AND start_station_id IS NOT NULL
    AND end_station_id IS NOT NULL
    AND start_station_id <> end_station_id
    AND CAST(ProcessingTime AS TIMESTAMP) = '{p_ProcessingTime}'
""")

# Clean malformed station IDs
cleaned_station_ids_df = spark.sql("""
SELECT * FROM citibikes_bronze.raw_trips
WHERE NOT (start_station_id RLIKE '^[+-]?\\d+(\\.\\d+)?$')
""")

# Join both for Silver ready dataframe
trips_bronze_table = (
    filtered_trips_df.alias("filtered")
    .join(cleaned_station_ids_df.alias("cleaned"), "ride_id", "left")
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
        col("filtered.FileName"),
        col("filtered.ProcessingTime")
    )
)

# Generate HASH_ID
hash_columns = [
    'RideId', 'RideableType', 'StartedAt', 'EndedAt', 'StartStationName',
    'StartStationId', 'EndStationName', 'EndStationId', 'StartLat', 'StartLng',
    'EndLat', 'EndLng', 'MemberCasual', 'FileName'
]

trips_bronze_table = add_metadata_columns(trips_bronze_table, p_DataSourceName, ingestion_date, hash_columns)

# Final selection
trips_bronze_table = trips_bronze_table.select(
    "RideId", "RideableType", "StartedAt", "EndedAt", "StartStationName", "StartStationId",
    "EndStationName", "EndStationId", "StartLat", "StartLng", "EndLat", "EndLng",
    "MemberCasual", "FileName", "ProcessingTime", "source_system", "ingestion_date", "HASH_ID"
)

# Create temp view
trips_bronze_table.createOrReplaceTempView("vw_trips")

spark.sql("""
MERGE INTO citibikes_silver.trips AS tgt
USING vw_trips AS src
ON tgt.RideId = src.RideId
WHEN MATCHED AND src.HASH_ID <> tgt.HASH_ID THEN
  UPDATE SET 
    tgt.RideableType = src.RideableType,
    tgt.StartedAt = src.StartedAt,
    tgt.EndedAt = src.EndedAt,
    tgt.StartStationName = src.StartStationName,
    tgt.StartStationId = src.StartStationId,
    tgt.EndStationName = src.EndStationName,
    tgt.EndStationId = src.EndStationId,
    tgt.StartLat = src.StartLat,
    tgt.StartLng = src.StartLng,
    tgt.EndLat = src.EndLat,
    tgt.EndLng = src.EndLng,
    tgt.MemberCasual = src.MemberCasual,
    tgt.FileName = src.FileName,
    tgt.ProcessingTime = src.ProcessingTime,
    tgt.SourceSystem = src.SourceSystem,
    tgt.IngestionDate = src.IngestionDate,
    tgt.HASH_ID = src.HASH_ID
WHEN NOT MATCHED THEN
  INSERT (
    RideId, RideableType, StartedAt, EndedAt, StartStationName, StartStationId,
    EndStationName, EndStationId, StartLat, StartLng, EndLat, EndLng, MemberCasual,
    FileName, ProcessingTime, SourceSystem, IngestionDate, HASH_ID
  )
  VALUES (
    src.RideId, src.RideableType, src.StartedAt, src.EndedAt, src.StartStationName, src.StartStationId,
    src.EndStationName, src.EndStationId, src.StartLat, src.StartLng, src.EndLat, src.EndLng, src.MemberCasual,
    src.FileName, src.ProcessingTime, src.SourceSystem, src.IngestionDate, src.HASH_ID
  )
""")
