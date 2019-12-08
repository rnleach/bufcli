INSERT OR REPLACE INTO
deciles (
	site,
	model,

	day_of_year,
	hour_of_day,

	hdw_deciles,
	blow_up_dt_deciles,
	blow_up_meters_deciles,
	dcape_deciles
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);
