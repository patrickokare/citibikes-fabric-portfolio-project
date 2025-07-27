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

dimbike_df = spark.sql("""
    WITH ProcessedFiles AS (
        SELECT DISTINCT FileName 
        FROM citibikes_gold.dimbike
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
    RankedBikes AS (
        SELECT 
            RideableType AS BikeType,
            FileName,
            SourceSystem,
            IngestionDate,
            ROW_NUMBER() OVER (PARTITION BY RideableType ORDER BY FileName DESC) AS Rank -- Rank based on FileName
        FROM FilteredTrips
    ),
    DeduplicatedBikes AS (
        SELECT 
            BikeType,
            FileName,
            SourceSystem,
            IngestionDate AS InsertedDate,
            sha2(CONCAT_WS('|', COALESCE(BikeType, 'NULL')), 256) AS HASH_ID
        FROM RankedBikes
        WHERE Rank = 1 -- Only take the latest file per BikeType
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY BikeType) + 
            COALESCE((SELECT MAX(BikeKey) FROM citibikes_gold.dimbike), 0) AS BikeKey, -- Generate Surrogate Key
        BikeType,
        FileName,
        SourceSystem,
        InsertedDate,
        NULL AS UpdatedDate,
        HASH_ID
    FROM DeduplicatedBikes;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a temporary view
dimbike_df.createOrReplaceTempView("vw_DimBike")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO citibikes_gold.dimbike AS tgt
# MAGIC USING vw_DimBike AS src
# MAGIC ON tgt.BikeType = src.BikeType
# MAGIC WHEN MATCHED AND (
# MAGIC         src.HASH_ID <> tgt.HASH_ID
# MAGIC          OR src.FileName <> tgt.FileName -- Check for new or updated file
# MAGIC     ) THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.BikeType = src.BikeType,
# MAGIC     tgt.FileName = src.FileName,
# MAGIC     tgt.SourceSystem = src.SourceSystem,
# MAGIC     tgt.UpdatedDate = current_timestamp(),
# MAGIC     tgt.HASH_ID = src.HASH_ID
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     BikeKey, BikeType, FileName, SourceSystem, InsertedDate, UpdatedDate, HASH_ID
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.BikeKey, src.BikeType, src.FileName ,src.SourceSystem, src.InsertedDate, src.UpdatedDate, src.HASH_ID
# MAGIC   );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC  CREATE TABLE citibikes_gold.dimbike
# MAGIC (
# MAGIC     BikeKey BIGINT NOT NULL, 
# MAGIC     BikeType STRING NOT NULL, 
# MAGIC     FileName STRING NOT NULL,
# MAGIC     SourceSystem STRING, 
# MAGIC      InsertedDate TIMESTAMP NOT NULL, 
# MAGIC      UpdatedDate TIMESTAMP, 
# MAGIC     HASH_ID STRING 
# MAGIC  )
# MAGIC USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
