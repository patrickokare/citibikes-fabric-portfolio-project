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
            ROW_NUMBER() OVER (PARTITION BY MemberCasual ORDER BY FileName DESC) AS Rank
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
        WHERE Rank = 1
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY RiderType) + 
            COALESCE((SELECT MAX(RiderKey) FROM citibikes_gold.dimrider), 0) AS RiderKey,
        RiderType,
        FileName,
        SourceSystem,
        InsertedDate,
        NULL AS UpdatedDate,
        HASH_ID
    FROM DeduplicatedRiders;
""")

deduplicated_riders_df.createOrReplaceTempView("vw_DimRider")

MERGE INTO citibikes_gold.dimrider AS tgt
USING vw_DimRider AS src
ON tgt.RiderType = src.RiderType
WHEN MATCHED AND (
    src.HASH_ID <> tgt.HASH_ID
    OR src.FileName <> tgt.FileName
) THEN
  UPDATE SET
    tgt.FileName = src.FileName,
    tgt.SourceSystem = src.SourceSystem,
    tgt.InsertedDate = src.InsertedDate,
    tgt.UpdatedDate = current_timestamp(),
    tgt.HASH_ID = src.HASH_ID
WHEN NOT MATCHED THEN
  INSERT (
    RiderKey, RiderType, FileName, SourceSystem, InsertedDate, UpdatedDate, HASH_ID
  )
  VALUES (
    src.RiderKey, src.RiderType, src.FileName, src.SourceSystem, src.InsertedDate, NULL, src.HASH_ID
  );

CREATE TABLE IF NOT EXISTS citibikes_gold.dimrider (
    RiderKey INT NOT NULL,
    RiderType STRING NOT NULL,
    FileName STRING NOT NULL,
    SourceSystem STRING NOT NULL,
    InsertedDate TIMESTAMP NOT NULL,
    UpdatedDate TIMESTAMP,
    HASH_ID STRING NOT NULL
)
USING DELTA;
