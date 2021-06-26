INSERT OR REPLACE INTO
cli (
		station_num,
		model,

		valid_time,
		year_lcl,
		month_lcl,
		day_lcl,
		hour_lcl,

		hdw,

		el_blow_up_dt,
		pft,

        dcape
	)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11);

