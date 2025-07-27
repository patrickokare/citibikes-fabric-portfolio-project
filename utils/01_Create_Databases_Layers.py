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

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS citibikes_bronze
# MAGIC LOCATION "Files/bronze/transportation/citibikes/github/"


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS citibikes_silver
# MAGIC LOCATION "Files/silver/transportation/citibikes/github/"

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS citibikes_gold
# MAGIC LOCATION "Files/gold/transportation/citibikes/"

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- DimRider Table
# MAGIC CREATE TABLE citibikes_gold.dimrider (
# MAGIC     RiderKey BIGINT NOT NULL, -- Surrogate Key
# MAGIC     RiderType STRING NOT NULL, -- Member or Casual
# MAGIC     SourceSystem STRING, -- Source system for data lineage
# MAGIC     InsertedDate TIMESTAMP, -- Date the record was inserted
# MAGIC     UpdatedDate TIMESTAMP, -- Date the record was last updated
# MAGIC     HASH_ID STRING NOT NULL
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- DimBike Table
# MAGIC CREATE TABLE citibikes_gold.dimbike (
# MAGIC     BikeKey BIGINT NOT NULL, -- Surrogate Key
# MAGIC     BikeType STRING NOT NULL, -- Type of bike
# MAGIC     SourceSystem STRING, -- Source system for data lineage
# MAGIC     InsertedDate TIMESTAMP, -- Date the record was inserted
# MAGIC     UpdatedDate TIMESTAMP, -- Date the record was last updated
# MAGIC      HASH_ID STRING NOT NULL
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- DimStation Table
# MAGIC CREATE TABLE citibikes_gold.dimstation (
# MAGIC     StationKey BIGINT NOT NULL, -- Surrogate Key
# MAGIC     StationID STRING NOT NULL, -- Station Identifier
# MAGIC     StationName STRING NOT NULL, -- Station Name
# MAGIC     Latitude DOUBLE, -- Latitude of the station
# MAGIC     Longitude DOUBLE, -- Longitude of the station
# MAGIC     SourceSystem STRING, -- Source system for data lineage
# MAGIC     InsertedDate TIMESTAMP, -- Date the record was inserted
# MAGIC     UpdatedDate TIMESTAMP,
# MAGIC     HASH_ID STRING NOT NULL
# MAGIC      )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- FactTrip Table
# MAGIC CREATE TABLE citibikes_gold.facttrip (
# MAGIC     TripKey BIGINT NOT NULL, -- Surrogate Key
# MAGIC     RideID STRING NOT NULL, -- Primary Key
# MAGIC     StartStationKey BIGINT NOT NULL, -- FK to DimStation
# MAGIC     EndStationKey BIGINT NOT NULL, -- FK to DimStation
# MAGIC     RiderKey BIGINT NOT NULL, -- FK to DimRider
# MAGIC     BikeKey BIGINT NOT NULL, -- FK to DimBike
# MAGIC     DateKey BIGINT NOT NULL, -- FK to DimDate
# MAGIC     TripDurationMinutes DOUBLE, -- Duration of the trip in minutes
# MAGIC     DistanceTraveled DOUBLE, -- Distance traveled (calculated from lat/long)
# MAGIC     TimeOfDay STRING, -- Time of day classification
# MAGIC     SourceSystem STRING, -- Source system for data lineage
# MAGIC     InsertedDate TIMESTAMP, -- Date the record was inserted
# MAGIC     UpdatedDate TIMESTAMP,
# MAGIC     HASH_ID STRING NOT NULL
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
