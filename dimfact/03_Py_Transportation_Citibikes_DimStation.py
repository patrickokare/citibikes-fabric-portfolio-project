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
LEFT JOIN citibikes_gold.dimstationlocation SL ON S.StationID = SL.StationLocationID
""")

dimstation_df.createOrReplaceTempView("vw_DimStation")

MERGE INTO citibikes_gold.dimstation AS tgt
USING vw_DimStation AS src
ON tgt.StationID = src.StationID
WHEN MATCHED AND (
        src.HASH_ID <> tgt.HASH_ID
        OR src.FileName <> tgt.FileName
    ) THEN
  UPDATE SET
    tgt.StationName = src.StationName,
    tgt.StationLocationKey = src.StationLocationKey,
    tgt.FileName = src.FileName,
    tgt.SourceSystem = src.SourceSystem,
    tgt.UpdatedDate = current_timestamp(),
    tgt.HASH_ID = src.HASH_ID
WHEN NOT MATCHED THEN
  INSERT (
    StationKey, StationID, StationName, StationLocationKey, FileName, SourceSystem, InsertedDate, UpdatedDate, HASH_ID
  )
  VALUES (
    src.StationKey, src.StationID, src.StationName, src.StationLocationKey, src.FileName, src.SourceSystem, src.InsertedDate, src.UpdatedDate, src.HASH_ID
  )

CREATE TABLE IF NOT EXISTS citibikes_gold.dimstation (
    StationKey INT NOT NULL,
    StationID STRING NOT NULL,
    StationName STRING,
    StationLocationKey INT NOT NULL,
    FileName STRING,
    SourceSystem STRING,
    InsertedDate TIMESTAMP NOT NULL,
    UpdatedDate TIMESTAMP,
    HASH_ID STRING NOT NULL
)
USING DELTA
