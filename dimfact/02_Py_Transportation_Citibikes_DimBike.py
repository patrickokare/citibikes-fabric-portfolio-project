from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Step 1: Build the dimension dataframe
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
            ROW_NUMBER() OVER (PARTITION BY RideableType ORDER BY FileName DESC) AS Rank
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
        WHERE Rank = 1
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY BikeType) + 
            COALESCE((SELECT MAX(BikeKey) FROM citibikes_gold.dimbike), 0) AS BikeKey,
        BikeType,
        FileName,
        SourceSystem,
        InsertedDate,
        NULL AS UpdatedDate,
        HASH_ID
    FROM DeduplicatedBikes
""")

# Step 2: Register as temp view
dimbike_df.createOrReplaceTempView("vw_DimBike")

# Step 3: Merge into Gold dimension table
spark.sql("""
    MERGE INTO citibikes_gold.dimbike AS tgt
    USING vw_DimBike AS src
    ON tgt.BikeType = src.BikeType
    WHEN MATCHED AND (
        src.HASH_ID <> tgt.HASH_ID
        OR src.FileName <> tgt.FileName
    ) THEN
      UPDATE SET
        tgt.BikeType = src.BikeType,
        tgt.FileName = src.FileName,
        tgt.SourceSystem = src.SourceSystem,
        tgt.UpdatedDate = current_timestamp(),
        tgt.HASH_ID = src.HASH_ID
    WHEN NOT MATCHED THEN
      INSERT (
        BikeKey, BikeType, FileName, SourceSystem, InsertedDate, UpdatedDate, HASH_ID
      )
      VALUES (
        src.BikeKey, src.BikeType, src.FileName, src.SourceSystem, src.InsertedDate, src.UpdatedDate, src.HASH_ID
      )
""")

# Step 4 (Optional): Create table if not exists (usually handled in schema setup phase)
spark.sql("""
    CREATE TABLE IF NOT EXISTS citibikes_gold.dimbike (
        BikeKey BIGINT NOT NULL, 
        BikeType STRING NOT NULL, 
        FileName STRING NOT NULL,
        SourceSystem STRING, 
        InsertedDate TIMESTAMP NOT NULL, 
        UpdatedDate TIMESTAMP, 
        HASH_ID STRING 
    )
    USING DELTA
""")
