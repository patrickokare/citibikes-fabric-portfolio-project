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

# MARKDOWN ********************

# ### SELECT REQUIRED FILEDS & DEDUPLICATE

# CELL ********************

dimstationlocation_df = spark.sql("""
WITH ProcessedFiles AS (
    SELECT DISTINCT FileName 
    FROM citibikes_gold.dimstationlocation
),
-- Identify new files to process
NewFiles AS (
    SELECT DISTINCT FileName
    FROM citibikes_silver.trips
    WHERE FileName NOT IN (SELECT FileName FROM ProcessedFiles)
),
-- Filter trips from new files
FilteredTrips AS (
    SELECT 
        RideId,
        RideableType,
        StartedAt,
        EndedAt,
        StartStationName,
        StartStationId,
        EndStationName,
        EndStationId,
        StartLat,
        StartLng,
        EndLat,
        EndLng,
        MemberCasual,
        FileName,
        ProcessingTime,
        SourceSystem,
        IngestionDate
    FROM citibikes_silver.trips
    WHERE FileName IN (SELECT FileName FROM NewFiles)
),
-- Deduplicate dimstationlocation based on StationLocationID
DeduplicatedLocations AS (
    SELECT
        StationID,
        MIN(StartLatitude) AS StartLatitude,
        MIN(StartLongitude) AS StartLongitude,
        MIN(EndLatitude) AS EndLatitude,
        MIN(EndLongitude) AS EndLongitude,
        MIN(FileName) AS FileName,
        MIN(SourceSystem) AS SourceSystem,
        MIN(IngestionDate) AS InsertedDate,
        sha2(
            CONCAT_WS('|', 
                COALESCE(StationID, 'NULL'), 
                COALESCE(MIN(StartLatitude), 'NULL'),
                COALESCE(MIN(StartLongitude), 'NULL'),
                COALESCE(MIN(EndLatitude), 'NULL'),
                COALESCE(MIN(EndLongitude), 'NULL')
            ), 256
        ) AS HASH_ID
    FROM (
        SELECT 
            StartStationID AS StationID,
            StartLat AS StartLatitude,
            StartLng AS StartLongitude,
            EndLat AS EndLatitude,
            EndLng AS EndLongitude,
            FileName,
            SourceSystem,
            IngestionDate
        FROM FilteredTrips
        UNION ALL
        SELECT 
            EndStationID AS StationID,
            EndLat AS StartLatitude, -- Treat End as Start for consistency
            EndLng AS StartLongitude,
            StartLat AS EndLatitude, -- Treat Start as End for consistency
            StartLng AS EndLongitude,
            FileName,
            SourceSystem,
            IngestionDate
        FROM FilteredTrips
    ) CombinedStations
    GROUP BY StationID
)
SELECT
    ROW_NUMBER() OVER (ORDER BY StationID) + 
    COALESCE((SELECT MAX(LongKey) FROM citibikes_gold.dimstationlocation), 0) AS LongKey,
    StationID AS StationLocationID,
    StartLatitude,
    StartLongitude,
    EndLatitude,
    EndLongitude,
    FileName,
    SourceSystem,
    InsertedDate,
    NULL AS UpdatedDate,
    HASH_ID
FROM DeduplicatedLocations;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a temporary view
dimstationlocation_df.createOrReplaceTempView("vw_DimStationLocation")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Type 1 Data Loading

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO citibikes_gold.dimstationlocation AS tgt
# MAGIC USING vw_DimStationLocation AS src
# MAGIC ON tgt.StationLocationID = src.StationLocationID
# MAGIC WHEN MATCHED AND (
# MAGIC         src.HASH_ID <> tgt.HASH_ID -- Detect changes based on the HASH_ID
# MAGIC         OR src.FileName <> tgt.FileName -- Ensure temporal accuracy
# MAGIC     ) THEN
# MAGIC   UPDATE SET
# MAGIC     
# MAGIC     tgt.StartLatitude = src.StartLatitude,
# MAGIC     tgt.StartLongitude = src.StartLongitude,
# MAGIC     tgt.EndLatitude = src.EndLatitude,
# MAGIC     tgt.EndLongitude = src.EndLongitude,
# MAGIC     tgt.FileName = src.FileName,
# MAGIC     tgt.SourceSystem = src.SourceSystem,
# MAGIC     tgt.UpdatedDate = current_timestamp(),
# MAGIC     tgt.HASH_ID = src.HASH_ID
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     LongKey, StationLocationID, StartLatitude, StartLongitude, EndLatitude, EndLongitude, FileName, SourceSystem, InsertedDate, UpdatedDate, HASH_ID
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.LongKey, src.StationLocationID, src.StartLatitude, src.StartLongitude, src.EndLatitude, src.EndLongitude, src.FileName, src.SourceSystem, src.InsertedDate, NULL, src.HASH_ID
# MAGIC   );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE citibikes_gold.dimstationlocation (
# MAGIC         LongKey INT NOT NULL,
# MAGIC         StationLocationID STRING NOT NULL,
# MAGIC         StartLatitude DOUBLE,
# MAGIC         StartLongitude DOUBLE,
# MAGIC         EndLatitude DOUBLE,
# MAGIC         EndLongitude DOUBLE,
# MAGIC         FileName STRING NOT NULL,
# MAGIC 
# MAGIC         SourceSystem STRING, -- Source system for data lineage
# MAGIC         InsertedDate TIMESTAMP NOT NULL, -- Date the record was inserted
# MAGIC         UpdatedDate TIMESTAMP, -- Date the record was last updated
# MAGIC         HASH_ID STRING NOT NULL -- Hash value for deduplication and tracking changes
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
