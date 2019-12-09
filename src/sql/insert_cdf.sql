INSERT OR REPLACE INTO
deciles (
	site,
	model,

	day_of_year,
	hour_of_day,

	hdw,
	blow_up_dt,
	blow_up_meters,
	dcape
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);
