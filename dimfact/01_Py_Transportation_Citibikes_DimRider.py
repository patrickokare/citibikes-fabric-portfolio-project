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

deduplicated_riders_df = spark.sql("""
    WITH ProcessedFiles AS (
        SELECT DISTINCT FileName 
        FROM citibikes_gold.dimrider
    ),
    NewFiles AS (
        SELECT DISTINCT FileName
        FROM citibikes_silver.trips
        WHERE FileName NOT IN (SELECT FileName FROM ProcessedFiles)
    ),
    FilteredTrips AS (
        SELECT *
        FROM citibikes_silver.trips
        WHERE FileName IN (SELECT FileName FROM NewFiles)
    ),
    RankedRiders AS (
        SELECT 
            MemberCasual AS RiderType,
            FileName,
            SourceSystem,
            IngestionDate,
            ROW_NUMBER() OVER (PARTITION BY MemberCasual ORDER BY FileName DESC) AS Rank -- Rank based on FileName
        FROM FilteredTrips
    ),
    DeduplicatedRiders AS (
        SELECT 
            RiderType,
            FileName,
            SourceSystem,
            IngestionDate AS InsertedDate,
            sha2(CONCAT_WS('|', COALESCE(RiderType, 'NULL')), 256) AS HASH_ID
        FROM RankedRiders
        WHERE Rank = 1 -- Only take the latest file per RiderType
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY RiderType) + 
            COALESCE((SELECT MAX(RiderKey) FROM citibikes_gold.dimrider), 0) AS RiderKey, -- Generate Surrogate Key
        RiderType,
        FileName,
        SourceSystem,
        InsertedDate,
        NULL AS UpdatedDate,
        HASH_ID
    FROM DeduplicatedRiders;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deduplicated_riders_df.createOrReplaceTempView("vw_DimRider")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO citibikes_gold.dimrider AS tgt
# MAGIC USING vw_DimRider AS src
# MAGIC ON tgt.RiderType = src.RiderType
# MAGIC WHEN MATCHED AND (
# MAGIC     src.HASH_ID <> tgt.HASH_ID -- Check for changes using HASH_ID
# MAGIC     OR src.FileName <> tgt.FileName -- Check for new or updated file
# MAGIC ) THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.FileName = src.FileName,
# MAGIC     tgt.SourceSystem = src.SourceSystem,
# MAGIC     tgt.InsertedDate = src.InsertedDate,
# MAGIC     tgt.UpdatedDate = current_timestamp(),
# MAGIC     tgt.HASH_ID = src.HASH_ID
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     RiderKey, RiderType, FileName, SourceSystem, InsertedDate, UpdatedDate, HASH_ID
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.RiderKey, src.RiderType, src.FileName, src.SourceSystem, src.InsertedDate, NULL, src.HASH_ID
# MAGIC   );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Create the dimrider table in Delta format
# MAGIC CREATE TABLE citibikes_gold.dimrider (
# MAGIC     RiderKey INT NOT NULL, -- Surrogate key for riders
# MAGIC     RiderType STRING NOT NULL, -- Type of rider (e.g., casual, member)
# MAGIC     FileName STRING NOT NULL,
# MAGIC     SourceSystem STRING NOT NULL, -- Source system for data lineage
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
