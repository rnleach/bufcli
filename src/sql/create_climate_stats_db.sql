
BEGIN;

CREATE TABLE IF NOT EXISTS deciles (
    site                   TEXT NOT NULL,
    model                  TEXT NOT NULL,

    day_of_year            INT  NOT NULL,
    hour_of_day            INT  NOT NULL,

    hdw_deciles            BLOB NOT NULL,
    blow_up_dt_deciles     BLOB NOT NULL,
    blow_up_meters_deciles BLOB NOT NULL,
    dcape_deciles          BLOB NOT NULL,

    PRIMARY KEY (site, model, day_of_year, hour_of_day));

COMMIT;

