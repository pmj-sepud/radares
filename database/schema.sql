CREATE SCHEMA IF NOT EXISTS radars;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'direction') THEN
        CREATE TYPE direction AS ENUM('Norte', 'Sul', 'Leste', 'Oeste');
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS radars.flows
(
"id"                                SERIAL PRIMARY KEY NOT NULL,
"pubdate"                           DATE, 
"equipment"                         VARCHAR(20),
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

CREATE UNIQUE INDEX IF NOT EXISTS "equip_date"
ON radars.flows USING btree (pubdate, equipment, direction, time_range);

CREATE TABLE IF NOT EXISTS radars.equipments
(
"id"                                SERIAL PRIMARY KEY NOT NULL,
"equipment"                         VARCHAR(20),
"pubdate"                           DATE,
"direction"                         direction,
"latitude"                          float4,
"longitude"                         float4,
"bike_lane"                         BOOLEAN,
"bus_lane"                          BOOLEAN,
"parking_lane"                      BOOLEAN,
"number_lanes"                      INTEGER
);