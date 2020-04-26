
BEGIN;

CREATE TABLE IF NOT EXISTS deciles (
    station_num       INT  NOT NULL,
    model             TEXT NOT NULL,

    day_of_year       INT  NOT NULL,
    hour_of_day       INT  NOT NULL,

    hdw               BLOB NOT NULL,
    el_blow_up_dt     BLOB NOT NULL,
    el_blow_up_meters BLOB NOT NULL,
    dcape             BLOB NOT NULL,

    PRIMARY KEY (station_num, model, day_of_year, hour_of_day));

COMMIT;

