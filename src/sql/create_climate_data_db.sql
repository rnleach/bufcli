BEGIN;

CREATE TABLE IF NOT EXISTS locations (
    site        TEXT NOT NULL,
    model       TEXT NOT NULL,
    start_date  TEXT NOT NULL,
    latitude    NUM  NOT NULL,
    longitude   NUM  NOT NULL,
    elevation_m NUM  NOT NULL,
    UNIQUE(site, model, latitude, longitude, elevation_m));

CREATE INDEX IF NOT EXISTS locations_idx ON locations (site, model);

CREATE TABLE IF NOT EXISTS cli (
    site           TEXT NOT NULL,
    model          TEXT NOT NULL,

    valid_time     TEXT NOT NULL,
    year_lcl       INT  NOT NULL,
    month_lcl      INT  NOT NULL,
    day_lcl        INT  NOT NULL,
    hour_lcl       INT  NOT NULL,

    hdw            INT,

    blow_up_dt     REAL,
    blow_up_meters INT,

    dcape          INT,

    PRIMARY KEY (site, valid_time, model, year_lcl, month_lcl, day_lcl, hour_lcl));

PRAGMA cache_size=100000;

COMMIT;

