INSERT OR REPLACE INTO
deciles (
	station_num,
	model,

	day_of_year,
	hour_of_day,

	hdw,
	el_blow_up_dt,
	el_blow_up_meters,
	dcape
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);
