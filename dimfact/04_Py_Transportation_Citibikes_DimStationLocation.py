# Author: Patrick Okare

dimstationlocation_df = spark.sql("""
WITH ProcessedFiles AS (
    SELECT DISTINCT FileName 
    FROM citibikes_gold.dimstationlocation
),
NewFiles AS (
    SELECT DISTINCT FileName
    FROM citibikes_silver.trips
    WHERE FileName NOT IN (SELECT FileName FROM ProcessedFiles)
),
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
            EndLat AS StartLatitude,
            EndLng AS StartLongitude,
            StartLat AS EndLatitude,
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
FROM DeduplicatedLocations
""")

dimstationlocation_df.createOrReplaceTempView("vw_DimStationLocation")


MERGE INTO citibikes_gold.dimstationlocation AS tgt
USING vw_DimStationLocation AS src
ON tgt.StationLocationID = src.StationLocationID
WHEN MATCHED AND (
        src.HASH_ID <> tgt.HASH_ID
        OR src.FileName <> tgt.FileName
    ) THEN
  UPDATE SET
    tgt.StartLatitude = src.StartLatitude,
    tgt.StartLongitude = src.StartLongitude,
    tgt.EndLatitude = src.EndLatitude,
    tgt.EndLongitude = src.EndLongitude,
    tgt.FileName = src.FileName,
    tgt.SourceSystem = src.SourceSystem,
    tgt.UpdatedDate = current_timestamp(),
    tgt.HASH_ID = src.HASH_ID
WHEN NOT MATCHED THEN
  INSERT (
    LongKey, StationLocationID, StartLatitude, StartLongitude, EndLatitude, EndLongitude, FileName, SourceSystem, InsertedDate, UpdatedDate, HASH_ID
  )
  VALUES (
    src.LongKey, src.StationLocationID, src.StartLatitude, src.StartLongitude, src.EndLatitude, src.EndLongitude, src.FileName, src.SourceSystem, src.InsertedDate, NULL, src.HASH_ID
  )


CREATE TABLE IF NOT EXISTS citibikes_gold.dimstationlocation (
    LongKey INT NOT NULL,
    StationLocationID STRING NOT NULL,
    StartLatitude DOUBLE,
    StartLongitude DOUBLE,
    EndLatitude DOUBLE,
    EndLongitude DOUBLE,
    FileName STRING NOT NULL,
    SourceSystem STRING,
    InsertedDate TIMESTAMP NOT NULL,
    UpdatedDate TIMESTAMP,
    HASH_ID STRING NOT NULL
)
USING DELTA
