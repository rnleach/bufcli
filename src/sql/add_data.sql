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
		conv_t_def_c,
		dry_cape,
		wet_cape,
		cape_ratio,

		e0,
		de
	)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14);

