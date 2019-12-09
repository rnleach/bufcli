
BEGIN;

CREATE TABLE IF NOT EXISTS deciles (
    site                   TEXT NOT NULL,
    model                  TEXT NOT NULL,

    day_of_year            INT  NOT NULL,
    hour_of_day            INT  NOT NULL,

    hdw                    BLOB NOT NULL,
    blow_up_dt             BLOB NOT NULL,
    blow_up_meters         BLOB NOT NULL,
    dcape                  BLOB NOT NULL,

    PRIMARY KEY (site, model, day_of_year, hour_of_day));

COMMIT;

