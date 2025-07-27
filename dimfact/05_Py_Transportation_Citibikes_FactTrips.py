# Author: Patrick Okare

facttrip_df = spark.sql("""
SELECT 
    ROW_NUMBER() OVER (ORDER BY T.RideID) + 
        COALESCE((SELECT MAX(F.TripKey) FROM citibikes_gold.facttrip F), 0) AS TripKey,
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

facttrip_df.createOrReplaceTempView("vw_facttrip")


MERGE INTO citibikes_gold.facttrip AS tgt
USING vw_facttrip AS src
ON tgt.RideID = src.RideID
AND tgt.StartStationKey = src.StartStationKey
AND tgt.EndStationKey = src.EndStationKey
AND tgt.RiderKey = src.RiderKey
AND tgt.BikeKey = src.BikeKey
AND tgt.StartDateKey = src.StartDateKey

WHEN MATCHED AND (
        src.HASH_ID <> tgt.HASH_ID
        OR src.FileName <> tgt.FileName
    ) THEN
  UPDATE SET
    tgt.TripDurationMinutes = src.TripDurationMinutes,
    tgt.DistanceTraveled = src.DistanceTraveled,
    tgt.TimeOfDay = src.TimeOfDay,
    tgt.FileName = src.FileName,
    tgt.SourceSystem = src.SourceSystem,
    tgt.UpdatedDate = current_timestamp(),
    tgt.HASH_ID = src.HASH_ID

WHEN NOT MATCHED THEN
  INSERT (
    TripKey, RideID, StartStationKey, EndStationKey, RiderKey, BikeKey, StartDateKey, 
    TripDurationMinutes, DistanceTraveled, TimeOfDay, FileName, SourceSystem, InsertedDate, UpdatedDate, HASH_ID
  )
  VALUES (
    src.TripKey, src.RideID, src.StartStationKey, src.EndStationKey, src.RiderKey, src.BikeKey, src.StartDateKey, 
    src.TripDurationMinutes, src.DistanceTraveled, src.TimeOfDay, src.FileName, src.SourceSystem, src.IngestionDate, NULL, src.HASH_ID
  )

CREATE TABLE IF NOT EXISTS citibikes_gold.facttrip
(
    TripKey BIGINT NOT NULL,
    RideID STRING NOT NULL,
    StartStationKey BIGINT NOT NULL,
    EndStationKey BIGINT NOT NULL,
    RiderKey BIGINT NOT NULL,
    BikeKey BIGINT NOT NULL,
    StartDateKey BIGINT NOT NULL,

    TripDurationMinutes DOUBLE NOT NULL,
    DistanceTraveled DOUBLE NULL,
    TimeOfDay STRING,
    FileName STRING NOT NULL,
    SourceSystem STRING,
    InsertedDate TIMESTAMP NOT NULL,
    UpdatedDate TIMESTAMP,
    HASH_ID STRING NOT NULL
)
USING DELTA

