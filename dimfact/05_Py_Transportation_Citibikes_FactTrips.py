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

facttrip_df = spark.sql("""
    
   SELECT 
    ROW_NUMBER() OVER (ORDER BY T.RideID) + 
        COALESCE((SELECT MAX(F.TripKey) FROM citibikes_gold.facttrip F), 0) AS TripKey, -- Generate Surrogate Key
    COALESCE(T.RideID, 'Unknown') AS RideID,
    COALESCE(S.StationKey, -1) AS StartStationKey,
    COALESCE(ES.StationKey, -1) AS EndStationKey,
    COALESCE(R.RiderKey, -1) AS RiderKey,
    COALESCE(B.BikeKey, -1) AS BikeKey,
    COALESCE(D.Date_ID, -1) AS StartDateKey,
    TIMESTAMPDIFF(MINUTE, T.StartedAt, T.EndedAt) AS TripDurationMinutes,
    ROUND(
        6371 * 2 * ASIN(SQRT(
            POWER(SIN((RADIANS(SL.EndLatitude - SL.StartLatitude)) / 2), 2) +
            COS(RADIANS(SL.StartLatitude)) * COS(RADIANS(SL.EndLatitude)) *
            POWER(SIN((RADIANS(SL.EndLongitude - SL.StartLongitude)) / 2), 2)
        )), 2
    ) AS DistanceTraveled,
    CASE 
        WHEN EXTRACT(HOUR FROM T.StartedAt) BETWEEN 6 AND 12 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM T.StartedAt) BETWEEN 12 AND 18 THEN 'Afternoon'
        ELSE 'Evening'
    END AS TimeOfDay,
    T.FileName,
    T.SourceSystem,
    T.IngestionDate,
    NULL AS UpdatedDate,
    sha2(
        CONCAT_WS('|', 
            COALESCE(T.RideID, 'NULL'), 
            COALESCE(S.StationKey, 'NULL'), 
            COALESCE(ES.StationKey, 'NULL'), 
            COALESCE(R.RiderKey, 'NULL'), 
            COALESCE(B.BikeKey, 'NULL'), 
            COALESCE(D.Date_ID, 'NULL'), 
            COALESCE(TIMESTAMPDIFF(MINUTE, T.StartedAt, T.EndedAt), 'NULL'),
            COALESCE(T.SourceSystem, 'NULL')
        ), 
        256
    ) AS HASH_ID
FROM citibikes_silver.trips T
LEFT JOIN citibikes_gold.dimstation S ON T.StartStationId = S.StationID
LEFT JOIN citibikes_gold.dimstation ES ON T.EndStationId = ES.StationID
LEFT JOIN citibikes_gold.dimstationlocation SL ON S.StationID = SL.StationLocationID
LEFT JOIN citibikes_gold.dimrider R ON R.RiderType = T.MemberCasual
LEFT JOIN citibikes_gold.dimbike B ON B.BikeType = T.RideableType
LEFT JOIN dbo.dimdate D ON D.Date = CAST(T.StartedAt AS DATE)
WHERE NOT EXISTS (
    SELECT 1
    FROM citibikes_gold.facttrip tgt
    WHERE tgt.RideID = T.RideID
      AND tgt.StartStationKey = COALESCE(S.StationKey, -1)
      AND tgt.EndStationKey = COALESCE(ES.StationKey, -1)
      AND tgt.RiderKey = COALESCE(R.RiderKey, -1)
      AND tgt.BikeKey = COALESCE(B.BikeKey, -1)
      AND tgt.StartDateKey = COALESCE(D.Date_ID, -1)
      )
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a temporary view
facttrip_df.createOrReplaceTempView("vw_facttrip")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO citibikes_gold.facttrip AS tgt
# MAGIC USING vw_facttrip AS src
# MAGIC ON tgt.RideID = src.RideID
# MAGIC AND tgt.StartStationKey = src.StartStationKey
# MAGIC AND tgt.EndStationKey = src.EndStationKey
# MAGIC AND tgt.RiderKey = src.RiderKey
# MAGIC AND tgt.BikeKey = src.BikeKey
# MAGIC AND tgt.StartDateKey = src.StartDateKey
# MAGIC 
# MAGIC WHEN MATCHED AND (
# MAGIC         src.HASH_ID <> tgt.HASH_ID -- Detect changes based on the HASH_ID
# MAGIC         OR src.FileName <> tgt.FileName -- Ensure temporal accuracy
# MAGIC     ) THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.TripDurationMinutes = src.TripDurationMinutes,
# MAGIC     tgt.DistanceTraveled = src.DistanceTraveled,
# MAGIC     tgt.TimeOfDay = src.TimeOfDay,
# MAGIC     tgt.FileName = src.FileName,
# MAGIC     tgt.SourceSystem = src.SourceSystem,
# MAGIC     tgt.UpdatedDate = current_timestamp(),
# MAGIC     tgt.HASH_ID = src.HASH_ID
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     TripKey, RideID, StartStationKey, EndStationKey, RiderKey, BikeKey, StartDateKey, 
# MAGIC     TripDurationMinutes, DistanceTraveled, TimeOfDay, FileName, SourceSystem, InsertedDate, UpdatedDate, HASH_ID
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.TripKey, src.RideID, src.StartStationKey, src.EndStationKey, src.RiderKey, src.BikeKey, src.StartDateKey, 
# MAGIC     src.TripDurationMinutes, src.DistanceTraveled, src.TimeOfDay, src.FileName, src.SourceSystem, src.IngestionDate, NULL, src.HASH_ID
# MAGIC   );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE citibikes_gold.facttrip
# MAGIC (
# MAGIC     TripKey BIGINT NOT NULL, -- Surrogate Key
# MAGIC     RideID STRING NOT NULL, -- Primary Key
# MAGIC     StartStationKey BIGINT NOT NULL, -- Foreign Key to DimStation
# MAGIC     EndStationKey BIGINT NOT NULL, -- Foreign Key to DimStation
# MAGIC     RiderKey BIGINT NOT NULL, -- Foreign Key to DimRider
# MAGIC     BikeKey BIGINT NOT NULL, -- Foreign Key to DimBike
# MAGIC     StartDateKey BIGINT NOT NULL, -- Foreign Key to Date Dimension
# MAGIC    
# MAGIC     TripDurationMinutes DOUBLE NOT NULL, -- Trip duration in minutes
# MAGIC     DistanceTraveled DOUBLE NULL, -- Calculated distance using lat/long
# MAGIC     TimeOfDay STRING, -- Time of day classification
# MAGIC     FileName STRING NOT NULL,
# MAGIC     SourceSystem STRING, -- Source system for lineage
# MAGIC     InsertedDate TIMESTAMP NOT NULL, -- Date the record was inserted
# MAGIC     UpdatedDate TIMESTAMP, -- Date the record was last updated
# MAGIC     HASH_ID STRING NOT NULL -- Hash value for deduplication and tracking changes
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
