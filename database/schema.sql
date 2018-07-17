CREATE SCHEMA IF NOT EXISTS radars;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'direction') THEN
        CREATE TYPE direction AS ENUM('Norte', 'Sul', 'Leste', 'Oeste');
    END IF;
END
$$;



CREATE TABLE IF NOT EXISTS radars.equipments
(
"id"                                SERIAL PRIMARY KEY NOT NULL,
"equipment"                         VARCHAR(20),
"date_updated"                      DATE,
"latitude"                          float4,
"longitude"                         float4,
"direction"                         direction,
"bike_lane"                         BOOLEAN DEFAULT FALSE,
"bus_lane"                          BOOLEAN DEFAULT FALSE,
"parking_lane"                      BOOLEAN DEFAULT FALSE,
"number_lanes"                      INTEGER
);




CREATE TABLE IF NOT EXISTS radars.equipment_files
(
"id"                                SERIAL PRIMARY KEY NOT NULL,
"file_name"                         VARCHAR(100),
"pubdate"                           DATE,
"equipment"                         VARCHAR(20),
"date_created"                      TIMESTAMP
);


CREATE TABLE IF NOT EXISTS radars.flows
(
"id"                                SERIAL PRIMARY KEY NOT NULL,
"equipment_files_id"                BIGINT NOT NULL REFERENCES radars.equipment_files (id),
"direction"                         direction,
"time_range"                        VARCHAR(20),
"speed_00_10"                       INTEGER, /*through analysis we inferred that the date was timezone aware*/
"speed_11_20"                       INTEGER,
"speed_21_30"                       INTEGER,
"speed_31_40"                       INTEGER,
"speed_41_50"                       INTEGER,
"speed_51_60"                       INTEGER,
"speed_61_70"                       INTEGER,
"speed_71_80"                       INTEGER,
"speed_81_90"                       INTEGER,
"speed_91_100"                      INTEGER,
"speed_100_up"                      INTEGER,
"total"                             INTEGER
);
