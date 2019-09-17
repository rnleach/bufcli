INSERT OR REPLACE INTO
cli (
		site,
		model,

		valid_time,
		year_lcl,
		month_lcl,
		day_lcl,
		hour_lcl,

		hdw,
		blow_up_dt,
		blow_up_meters,

        dcape
	)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11);

