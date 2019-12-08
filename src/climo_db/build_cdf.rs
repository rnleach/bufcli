//! Module to build and populate Cumulative Distribution Functions.

use super::ClimoDB;
use crate::{CumulativeDistribution, Deciles};
use bufkit_data::{Model, Site};
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, Timelike};
use rusqlite::{params, Statement, NO_PARAMS};
use std::error::Error;

pub struct ClimoCDFBuilderInterface<'a, 'b: 'a> {
    climo_db: &'b ClimoDB,
    add_cdf_query: Statement<'a>,
    all_data_query: Statement<'a>,
    buffer: Vec<Record>,
}

struct Record {
    site: Site,
    model: Model,
    day_of_year: u32,
    hour: u32,
    hdw_dist: Deciles,
    dt_dist: Deciles,
    meters_dist: Deciles,
    dcape_dist: Deciles,
}

/// A collection of data values. Each tuple is
/// (valid time, hdw, blow up dt, blow up meters, dcape)
pub type AllData = Vec<(NaiveDateTime, f64, f64, f64, f64)>;

const BUFSIZE: usize = 100;

impl<'a, 'b> ClimoCDFBuilderInterface<'a, 'b> {
    /// Initialize the interface.
    pub fn initialize(climo_db: &'b ClimoDB) -> Result<Self, Box<dyn Error>> {
        let stats_conn = &climo_db.stats_conn;
        let data_conn = &climo_db.conn;

        let add_cdf_query = stats_conn.prepare(include_str!("../sql/insert_cdf.sql"))?;
        let all_data_query =
            data_conn.prepare(include_str!("../sql/all_data_for_site_and_model.sql"))?;

        Ok(Self {
            climo_db,
            add_cdf_query,
            all_data_query,
            buffer: Vec::with_capacity(BUFSIZE),
        })
    }

    /// Load all the available for the site, model pair
    pub fn load_all_data(&mut self, site: &Site, model: Model) -> Result<AllData, Box<dyn Error>> {
        let data: Vec<(NaiveDateTime, f64, f64, f64, f64)> = self
            .all_data_query
            .query_map(params![site.id, model.as_static_str()], |row| {
                Ok((
                    row.get(0)?, // valid time
                    row.get(1)?, // hdw
                    row.get(2)?, // dt
                    row.get(3)?, // meters
                    row.get(4)?, // dcape
                ))
            })?
            .filter_map(Result::ok)
            //
            // ******** THIS STEP IS IMPORTANT ***********
            //
            .map(|(vt, hdw, mut dt, mut blow_up_meters, dcape)| {
                if blow_up_meters < 1000.0 {
                    dt = std::f64::INFINITY;
                    blow_up_meters = -std::f64::INFINITY;
                }

                (vt, hdw, dt, blow_up_meters, dcape)
            })
            .collect();

        Ok(data)
    }

    /// Add/Update `Deciles` in the database.
    pub fn add_to_db(
        &mut self,
        site: &Site,
        model: Model,
        day_of_year: u32,
        hour: u32,
        (hdw_dist, dt_dist, meters_dist, dcape_dist): (Deciles, Deciles, Deciles, Deciles),
    ) -> Result<(), Box<dyn Error>> {
        self.buffer.push(Record {
            site: site.clone(),
            model,
            day_of_year,
            hour,
            hdw_dist,
            dt_dist,
            meters_dist,
            dcape_dist,
        });

        if self.buffer.len() >= BUFSIZE {
            self.flush()?;
        }

        Ok(())
    }

    /// Create deciles.
    pub fn create_deciles(
        data: &[(NaiveDateTime, f64, f64, f64, f64)],
    ) -> Vec<(u32, u32, Deciles, Deciles, Deciles, Deciles)> {
        let non_leap_year: NaiveDate = NaiveDate::from_ymd(2019, 1, 1);
        let mut to_ret = vec![];

        for day_of_year in 1..=365 {
            // ignore leap year day 366
            let target_date = non_leap_year.with_ordinal(day_of_year).unwrap();
            let filter_start = target_date - Duration::days(7);
            let filter_end = target_date + Duration::days(7);
            let in_window = make_window_func(filter_start, filter_end);

            for hour in 0..24 {
                let mut hdw_vec = vec![];
                let mut blow_up_dt_vec = vec![];
                let mut blow_up_meters_vec = vec![];
                let mut dcape_vec = vec![];

                let data_iter = data
                    .iter()
                    .filter(|&(vt, _, _, _, _)| in_window(*vt))
                    .filter(|&(vt, _, _, _, _)| vt.hour() == hour)
                    .map(|&(_, hdw, dt, meters, dcape)| (hdw, dt, meters, dcape));

                for (hdw, dt, meters, dcape) in data_iter {
                    hdw_vec.push(hdw);
                    blow_up_dt_vec.push(dt);
                    blow_up_meters_vec.push(meters);
                    dcape_vec.push(dcape);
                }

                if hdw_vec.is_empty()
                    || blow_up_dt_vec.is_empty()
                    || blow_up_meters_vec.is_empty()
                    || dcape_vec.is_empty()
                {
                    continue;
                }

                let hdw_dist = CumulativeDistribution::new(hdw_vec).deciles();
                let dt_dist = CumulativeDistribution::new(blow_up_dt_vec).deciles();
                let meters_dist = CumulativeDistribution::new(blow_up_meters_vec).deciles();
                let dcape_dist = CumulativeDistribution::new(dcape_vec).deciles();

                to_ret.push((
                    day_of_year,
                    hour,
                    hdw_dist,
                    dt_dist,
                    meters_dist,
                    dcape_dist,
                ));
            }
        }

        to_ret
    }

    fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        self.climo_db
            .stats_conn
            .execute("BEGIN TRANSACTION", NO_PARAMS)?;

        for record in self.buffer.drain(..) {
            self.add_cdf_query.execute(params![
                record.site.id,
                record.model.as_static_str(),
                record.day_of_year,
                record.hour,
                record.hdw_dist.as_bytes()?,
                record.dt_dist.as_bytes()?,
                record.meters_dist.as_bytes()?,
                record.dcape_dist.as_bytes()?,
            ])?;
        }

        self.climo_db
            .stats_conn
            .execute("COMMIT TRANSACTION", NO_PARAMS)?;
        Ok(())
    }
}

impl<'a, 'b> Drop for ClimoCDFBuilderInterface<'a, 'b> {
    fn drop(&mut self) {
        self.flush().unwrap();
    }
}

fn make_window_func(start: NaiveDate, end: NaiveDate) -> impl Fn(NaiveDateTime) -> bool {
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
