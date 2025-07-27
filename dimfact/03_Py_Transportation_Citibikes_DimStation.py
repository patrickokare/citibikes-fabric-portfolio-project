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

dimstation_df = spark.sql("""
WITH ProcessedFiles AS (
    SELECT DISTINCT FileName 
    FROM citibikes_gold.dimstation
),
NewFiles AS (
    SELECT DISTINCT FileName
    FROM citibikes_silver.trips
    WHERE FileName IN (SELECT FileName FROM ProcessedFiles)
),
FilteredTrips AS (
    SELECT DISTINCT 
        StationID,
        StationName,
        FileName,
        SourceSystem,
        IngestionDate
    FROM (
        SELECT 
            StartStationID AS StationID,
            StartStationName AS StationName,
            FileName,
            SourceSystem,
            IngestionDate
        FROM citibikes_silver.trips
        WHERE FileName IN (SELECT FileName FROM NewFiles)
        
        UNION
        
        SELECT 
            EndStationID AS StationID,
            EndStationName AS StationName,
            FileName,
            SourceSystem,
            IngestionDate
        FROM citibikes_silver.trips
        WHERE FileName IN (SELECT FileName FROM NewFiles)
    )
),
DeduplicatedStations AS (
    SELECT
        StationID,
        StationName,
        MIN(FileName) AS FileName,
        MIN(SourceSystem) AS SourceSystem,
        MIN(IngestionDate) AS InsertedDate,
        sha2(CONCAT_WS('|', 
                COALESCE(StationID, 'NULL'),
                COALESCE(StationName, 'NULL')
            ), 
            256
        ) AS HASH_ID
    FROM FilteredTrips
    GROUP BY StationID, StationName
)
SELECT
    ROW_NUMBER() OVER (ORDER BY StationID) + 
    COALESCE((SELECT MAX(StationKey) FROM citibikes_gold.dimstation), 0) AS StationKey,
    S.StationID,
    S.StationName,
    COALESCE(SL.LongKey, -1) AS StationLocationKey,
    S.FileName,
    S.SourceSystem,
    S.InsertedDate,
    NULL AS UpdatedDate,
    S.HASH_ID
FROM DeduplicatedStations S
LEFT JOIN citibikes_gold.dimstationlocation SL ON S.StationID = SL.StationLocationID;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a temporary view
dimstation_df.createOrReplaceTempView("vw_DimStation")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO citibikes_gold.dimstation AS tgt
# MAGIC USING vw_DimStation AS src
# MAGIC ON tgt.StationID = src.StationID
# MAGIC WHEN MATCHED AND (
# MAGIC         src.HASH_ID <> tgt.HASH_ID -- Detect changes based on the HASH_ID
# MAGIC         OR src.FileName <> tgt.FileName -- Ensure temporal accuracy
# MAGIC     ) THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.StationName = src.StationName,
# MAGIC     tgt.StationLocationKey = src.StationLocationKey, -- Update FK if necessary
# MAGIC     tgt.FileName = src.FileName,
# MAGIC     tgt.SourceSystem = src.SourceSystem,
# MAGIC     tgt.UpdatedDate = current_timestamp(),
# MAGIC     tgt.HASH_ID = src.HASH_ID
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     StationKey, StationID, StationName, StationLocationKey, FileName, SourceSystem, InsertedDate, UpdatedDate, HASH_ID
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.StationKey, src.StationID, src.StationName, src.StationLocationKey, src.FileName, src.SourceSystem, src.InsertedDate, src.UpdatedDate, src.HASH_ID
# MAGIC   );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE citibikes_gold.dimstation (
# MAGIC 
# MAGIC 
# MAGIC    StationKey INT NOT NULL,
# MAGIC     StationID STRING NOT NULL,
# MAGIC     StationName STRING,
# MAGIC     StationLocationKey INT NOT NULL,
# MAGIC 
# MAGIC     FileName STRING,
# MAGIC 
# MAGIC     SourceSystem STRING, -- Source system for data lineage
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
