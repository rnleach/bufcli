use crate::stats_record::StatsRecord;
use bufkit_data::{Model, Site};
use chrono::{Datelike, Duration, FixedOffset, NaiveDateTime, TimeZone, Timelike};
use rusqlite::{params, types::ToSql, Connection, OpenFlags, Statement, NO_PARAMS};
use std::{
    error::Error,
    fs::create_dir,
    path::{Path, PathBuf},
};
use strum::AsStaticRef;

pub struct ClimoDB {
    conn: Connection,
}

impl ClimoDB {
    pub const CLIMO_DIR: &'static str = "climo";
    pub const CLIMO_DB: &'static str = "climo.db";

    pub fn path_to_climo_db(arch_root: &Path) -> PathBuf {
        arch_root.join(Self::CLIMO_DIR).join(Self::CLIMO_DB)
    }

    pub fn connect_or_create(arch_root: &Path) -> Result<Self, Box<dyn Error>> {
        let climo_path = arch_root.join(Self::CLIMO_DIR);
        if !climo_path.is_dir() {
            create_dir(&climo_path)?;
        }

        let db_file = climo_path.join(Self::CLIMO_DB);

        // Create and set up the database
        let conn = Connection::open_with_flags(
            db_file,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;

        // Create the database if it doesn't exist.
        conn.execute_batch(include_str!("sql/create.sql"))?;

        Ok(ClimoDB { conn })
    }
}

/// The struct creates and caches several prepared statements for adding data to the climo database.
pub struct ClimoBuilderInterface<'a, 'b: 'a> {
    climo_db: &'b ClimoDB,
    add_location_query: Statement<'a>,
    add_data_query: Statement<'a>,
    init_times_query: Statement<'a>,
    write_buffer: Vec<StatsRecord>,
}

impl<'a, 'b> ClimoBuilderInterface<'a, 'b> {
    const BUFSIZE: usize = 4096;

    pub fn initialize(climo_db: &'b ClimoDB) -> Result<Self, Box<dyn Error>> {
        let conn = &climo_db.conn;
        let add_location_query = conn.prepare(include_str!("sql/add_location.sql"))?;
        let add_data_query = conn.prepare(include_str!("sql/add_data.sql"))?;
        let init_times_query = conn.prepare(include_str!("sql/init_times.sql"))?;

        Ok(ClimoBuilderInterface {
            climo_db,
            add_location_query,
            add_data_query,
            init_times_query,
            write_buffer: Vec::with_capacity(ClimoBuilderInterface::BUFSIZE),
        })
    }

    #[inline]
    pub fn valid_times_for(
        &mut self,
        site: &Site,
        model: Model,
    ) -> Result<Vec<NaiveDateTime>, Box<dyn Error>> {
        let model_str = model.as_static();

        let valid_times: Result<Vec<NaiveDateTime>, _> = self
            .init_times_query
            .query_map(&[&site.id as &dyn ToSql, &model_str], |row| row.get(0))?
            .collect();
        let valid_times = valid_times?;

        Ok(valid_times)
    }

    #[inline]
    pub fn add(&mut self, record: StatsRecord) -> Result<(), Box<dyn Error>> {
        debug_assert!(self.write_buffer.len() <= ClimoBuilderInterface::BUFSIZE);
        self.write_buffer.push(record);

        if self.write_buffer.len() == ClimoBuilderInterface::BUFSIZE {
            self.flush()?;
        }

        Ok(())
    }

    #[inline]
    fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        use self::StatsRecord::*;

        self.climo_db.conn.execute("BEGIN TRANSACTION", NO_PARAMS)?;

        for record in self.write_buffer.drain(..) {
            match record {
                CliData {
                    site,
                    model,
                    valid_time,
                    hdw,
                    blow_up_dt,
                    blow_up_meters,
                } => {
                    let lcl_time = site
                        .time_zone
                        .unwrap_or_else(|| FixedOffset::west(0))
                        .from_utc_datetime(&valid_time);
                    let year_lcl = lcl_time.year();
                    let month_lcl = lcl_time.month();
                    let day_lcl = lcl_time.day();
                    let hour_lcl = lcl_time.hour();

                    self.add_data_query
                        .execute(&[
                            &site.id as &dyn ToSql,
                            &model.as_static(),
                            &valid_time as &dyn ToSql,
                            &year_lcl as &dyn ToSql,
                            &month_lcl as &dyn ToSql,
                            &day_lcl as &dyn ToSql,
                            &hour_lcl as &dyn ToSql,
                            &hdw as &dyn ToSql,
                            &blow_up_dt as &dyn ToSql,
                            &blow_up_meters as &dyn ToSql,
                        ])
                        .map(|_| ())?
                }
                Location {
                    site,
                    model,
                    valid_time,
                    lat,
                    lon,
                    elev_m,
                } => self
                    .add_location_query
                    .execute(&[
                        &site.id as &dyn ToSql,
                        &model.as_static(),
                        &valid_time as &dyn ToSql,
                        &lat as &dyn ToSql,
                        &lon as &dyn ToSql,
                        &elev_m as &dyn ToSql,
                    ])
                    .map(|_| ())?,
            }
        }

        self.climo_db
            .conn
            .execute("COMMIT TRANSACTION", NO_PARAMS)?;

        Ok(())
    }
}

impl<'a, 'b> Drop for ClimoBuilderInterface<'a, 'b> {
    fn drop(&mut self) {
        self.flush().unwrap();
        self.climo_db.conn.execute("VACUUM", NO_PARAMS).unwrap();
    }
}

/// This struct creates and caches several statements for querying the database.
pub struct ClimoQueryInterface<'a, 'b: 'a> {
    climo_db: &'b ClimoDB,
    hourly_decile_statements: [Option<Statement<'a>>; ClimoElement::NUM_VARIANTS],
}

/// Elements we can query for climo data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClimoElement {
    HDW,
    BlowUpDt,
    BlowUpHeight,
}

impl ClimoElement {
    const NUM_VARIANTS: usize = 3;

    fn into_index(self) -> usize {
        self as usize
    }

    fn into_column_name(self) -> &'static str {
        use ClimoElement::*;

        match self {
            HDW => "hdw",
            BlowUpDt => "blow_up_dt",
            BlowUpHeight => "blow_up_meters",
        }
    }
}

/// Struct for holding deciles data.
#[derive(Clone, Debug)]
pub struct HourlyDeciles {
    pub element: ClimoElement,
    pub valid_times: Vec<NaiveDateTime>,
    pub deciles: Vec<[f64; 11]>, // index 0 = min, index 10 = max, otherwise index*10 = percentile
}

impl<'a, 'b> ClimoQueryInterface<'a, 'b> {
    /// Initialize the interface.
    pub fn initialize(climo_db: &'b ClimoDB) -> Self {
        let hourly_decile_statements = [None, None, None];
        Self {
            climo_db,
            hourly_decile_statements,
        }
    }

    fn get_hourly_deciles_statement(
        &mut self,
        element: ClimoElement,
    ) -> Result<&mut Statement<'a>, Box<dyn Error>> {
        let opt = &mut self.hourly_decile_statements[element.into_index()];
        if opt.is_none() {
            *opt = Some(self.climo_db.conn.prepare(&format!(
                include_str!("sql/hourly_deciles.sql"),
                element.into_column_name()
            ))?);
        }

        Ok(opt.as_mut().unwrap())
    }

    /// Retrieve hourly deciles for the requested site and model.
    pub fn hourly_deciles(
        &mut self,
        site: &Site,
        model: &str,
        element: ClimoElement,
        start_time: NaiveDateTime,
        end_time: NaiveDateTime,
    ) -> Result<HourlyDeciles, Box<dyn Error>> {
        debug_assert!(end_time > start_time);

        // Need one week either side of the start and end to get all the data in the window.
        let early_start = start_time - Duration::days(7);
        let late_end = end_time + Duration::days(7);

        let in_window = make_window_func(early_start, late_end);

        let statement = self.get_hourly_deciles_statement(element)?;

        let mut data: Vec<(NaiveDateTime, f64)> = statement
            .query_map(params![site.id, model], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            // Filter out errors
            .filter_map(Result::ok)
            // Filter out data that is outside the range I care about
            .filter(|(valid_time, _val)| in_window(*valid_time))
            .collect();
        // Sort by value in ascending order.
        data.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        let data = data;

        let num_elements = ((end_time - start_time).num_hours() + 1) as usize;
        let mut deciles: Vec<[f64; 11]> = Vec::with_capacity(num_elements);
        let mut valid_times: Vec<NaiveDateTime> = Vec::with_capacity(num_elements);
        let mut curr_time = start_time;
        while curr_time <= end_time {
            let filter_start = curr_time - Duration::days(7);
            let filter_end = curr_time + Duration::days(7);
            let filter_hour = curr_time.hour();

            let in_window = make_window_func(filter_start, filter_end);

            let vals: Vec<f64> = data
                .iter()
                .filter(|(vt, _val)| vt.hour() == filter_hour)
                .filter(|(vt, _val)| in_window(*vt))
                .map(|(_vt, val)| *val)
                .collect();
            if vals.is_empty() {
                curr_time += Duration::hours(1);
                continue;
            }
            let max_idx: f32 = (vals.len() - 1) as f32;
            let percentile_idx =
                |percentile: f32| -> usize { (max_idx * percentile).round() as usize };

            let deciles_for_date = [
                vals[0],                    // minimum
                vals[percentile_idx(0.10)], // 10th percentile
                vals[percentile_idx(0.20)], // 20th percentile
                vals[percentile_idx(0.30)], // 30th percentile
                vals[percentile_idx(0.40)], // 40th percentile
                vals[vals.len() / 2],       // median
                vals[percentile_idx(0.60)], // 60th percentile
                vals[percentile_idx(0.70)], // 70th percentile
                vals[percentile_idx(0.80)], // 80th percentile
                vals[percentile_idx(0.90)], // 90th percentile
                vals[vals.len() - 1],       // maximum
            ];

            deciles.push(deciles_for_date);
            valid_times.push(curr_time);

            curr_time += Duration::hours(1);
        }

        if valid_times.is_empty() {
            return Err(Box::new(crate::BufcliError::new("No climate data")));
        }

        Ok(HourlyDeciles {
            element,
            deciles,
            valid_times,
        })
    }
}

fn make_window_func(start: NaiveDateTime, end: NaiveDateTime) -> impl Fn(NaiveDateTime) -> bool {
    let start_month = start.month();
    let start_day = start.day();
    let end_month = end.month();
    let end_day = end.day();

    move |vt: NaiveDateTime| -> bool {
        let vt_month = vt.month();
        let vt_day = vt.day();

        if start_month < end_month {
            (vt_month == start_month && vt_day >= start_day)
                || (vt_month == end_month && vt_day <= end_day)
                || (vt_month > start_month && vt_month < end_month)
        } else if start_month == end_month {
            vt_day >= start_day && vt_day <= end_day
        } else {
            // start_month > end_month, wrap around the year
            (vt_month == start_month && vt_day >= start_day)
                || (vt_month == end_month && vt_day <= end_day)
                || (vt_month > start_month || vt_month < end_month)
        }
    }
}
