SELECT
    day_of_year,
    hour_of_day,
    rowid,
FROM deciles
WHERE site = ?1 and model = ?2
