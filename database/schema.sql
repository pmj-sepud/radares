CREATE SCHEMA IF NOT EXISTS radars;

-- create the lambda role
CREATE ROLE sepud_admin LOGIN PASSWORD '5m2018_53pud3nv1r0nm3n7';

-- setup permissions for the lambda role
GRANT ALL ON SCHEMA radars TO sepud_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA radars GRANT ALL ON TABLES TO sepud_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA radars GRANT SELECT, USAGE ON SEQUENCES TO sepud_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA radars GRANT EXECUTE ON FUNCTIONS TO sepud_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA radars GRANT USAGE ON TYPES TO sepud_admin;


DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'direction') THEN
        CREATE TYPE direction AS ENUM('Norte', 'Sul', 'Leste', 'Oeste');
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS radars.equipment_files
(
"id"                                SERIAL PRIMARY KEY NOT NULL,
"file_name"                         VARCHAR(100),
"pubdate"                           DATE,
"equipment"                         VARCHAR(20),
"date_created"                      TIMESTAMP
);

CREATE UNIQUE INDEX "IDX_UNIQUE_pubdate_equipment"
ON radars.equipment_files USING btree
(pubdate, equipment);


CREATE TABLE IF NOT EXISTS radars.equipments
(
"id"                                SERIAL PRIMARY KEY NOT NULL,
"equipment"                         VARCHAR(20),
"date_updated"                      TIMESTAMP,
"address"							TEXT,
"latitude"                          float4,
"longitude"                         float4,
"direction"                         direction,
"bike_lane"                         BOOLEAN DEFAULT FALSE,
"bus_lane"                          BOOLEAN DEFAULT FALSE,
"parking_lane"                      BOOLEAN DEFAULT FALSE,
"number_lanes"                      INTEGER
);


CREATE TABLE IF NOT EXISTS radars.flows
(
"id"                                SERIAL PRIMARY KEY NOT NULL,
"equipment_files_id"                BIGINT NOT NULL REFERENCES radars.equipment_files (id),
"direction"                         direction,
"initial_time"                      TIME, /* future improvements: Storage this as TIME */
"end_time"                        	TIME, /* future improvements: Storage this as TIME */
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
