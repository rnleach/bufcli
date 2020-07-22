use super::ClimoDB;
use super::StatsRecord;
use bufkit_data::{Model, SiteInfo};
use chrono::{Datelike, FixedOffset, NaiveDateTime, TimeZone, Timelike};
use rusqlite::{types::ToSql, Statement, NO_PARAMS};
use std::error::Error;

/// The struct creates and caches several prepared statements for adding data to the climo database.
pub struct ClimoPopulateInterface<'a, 'b: 'a> {
    climo_db: &'b ClimoDB,
    add_location_query: Statement<'a>,
    add_data_query: Statement<'a>,
    init_times_query: Statement<'a>,
    write_buffer: Vec<StatsRecord>,
}

impl<'a, 'b> ClimoPopulateInterface<'a, 'b> {
    const BUFSIZE: usize = 4096;

    pub fn initialize(climo_db: &'b ClimoDB) -> Result<Self, Box<dyn Error>> {
        let conn = &climo_db.conn;
        let add_location_query = conn.prepare(include_str!("add_location.sql"))?;
        let add_data_query = conn.prepare(include_str!("add_data.sql"))?;
        let init_times_query = conn.prepare(include_str!("init_times.sql"))?;

        Ok(ClimoPopulateInterface {
            climo_db,
            add_location_query,
            add_data_query,
            init_times_query,
            write_buffer: Vec::with_capacity(ClimoPopulateInterface::BUFSIZE),
        })
    }

    #[inline]
    pub fn valid_times_for(
        &mut self,
        site: &SiteInfo,
        model: Model,
    ) -> Result<Vec<NaiveDateTime>, Box<dyn Error>> {
        let model_str = model.as_static_str();
        let station_num: u32 = site.station_num.into();

        let valid_times: Result<Vec<NaiveDateTime>, _> = self
            .init_times_query
            .query_map(&[&station_num as &dyn ToSql, &model_str], |row| row.get(0))?
            .collect();
        let valid_times = valid_times?;

        Ok(valid_times)
    }

    #[inline]
    pub fn add(&mut self, record: StatsRecord) -> Result<(), Box<dyn Error>> {
        debug_assert!(self.write_buffer.len() <= ClimoPopulateInterface::BUFSIZE);
        self.write_buffer.push(record);

        if self.write_buffer.len() == ClimoPopulateInterface::BUFSIZE {
            self.flush()?;
        }

        Ok(())
    }

    #[inline]
    fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        use self::StatsRecord::*;

        self.climo_db.conn.execute("BEGIN TRANSACTION", NO_PARAMS)?;

        for record in self.write_buffer.drain(..) {
            if let Err(err) = {
                match record {
                    CliData {
                        site,
                        model,
                        valid_time,
                        hdw,
                        blow_up_dt,
                        blow_up_meters,
                        dcape,
                    } => {
                        let lcl_time = site
                            .time_zone
                            .unwrap_or_else(|| FixedOffset::west(0))
                            .from_utc_datetime(&valid_time);
                        let year_lcl = lcl_time.year();
                        let month_lcl = lcl_time.month();
                        let day_lcl = lcl_time.day();
                        let hour_lcl = lcl_time.hour();

                        let station_num: u32 = site.station_num.into();

                        self.add_data_query
                            .execute(&[
                                &station_num as &dyn ToSql,
                                &model.as_static_str(),
                                &valid_time as &dyn ToSql,
                                &year_lcl as &dyn ToSql,
                                &month_lcl as &dyn ToSql,
                                &day_lcl as &dyn ToSql,
                                &hour_lcl as &dyn ToSql,
                                &hdw as &dyn ToSql,
                                &blow_up_dt as &dyn ToSql,
                                &blow_up_meters as &dyn ToSql,
                                &dcape as &dyn ToSql,
                            ])
                            .map(|_| ())
                    }
                    Location {
                        site,
                        model,
                        lat,
                        lon,
                        elev_m,
                    } => {
                        // unwrap should be ok because we filtered out sites without a name
                        let name: String = site.name.unwrap();
                        self.add_location_query
                            .execute(&[
                                &Into::<u32>::into(site.station_num) as &dyn ToSql,
                                &name,
                                &model.as_static_str(),
                                &lat as &dyn ToSql,
                                &lon as &dyn ToSql,
                                &elev_m as &dyn ToSql,
                            ])
                            .map(|_| ())
                    }
                }
            } {
                eprintln!("Error adding data to database: {}", err);
                self.climo_db
                    .conn
                    .execute("COMMIT TRANSACTION", NO_PARAMS)?;
                return Err(err.into());
            }
        }

        self.climo_db
            .conn
            .execute("COMMIT TRANSACTION", NO_PARAMS)?;

        Ok(())
    }
}

impl<'a, 'b> Drop for ClimoPopulateInterface<'a, 'b> {
    fn drop(&mut self) {
        self.flush().unwrap();
        self.climo_db.conn.execute("VACUUM", NO_PARAMS).unwrap();
    }
}
