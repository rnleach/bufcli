use super::{ClimoDB, ClimoElement};
use crate::distributions::Deciles;
use bufkit_data::Site;
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime};
use rusqlite::{params, DatabaseName, Statement};
use std::error::Error;

/// This struct creates and caches several statements for querying the database.
pub struct ClimoQueryInterface<'a, 'b: 'a> {
    climo_db: &'b ClimoDB,
    deciles_statement: Statement<'a>,
}

impl<'a, 'b> ClimoQueryInterface<'a, 'b> {
    /// Initialize the interface.
    pub fn initialize(climo_db: &'b ClimoDB) -> Result<Self, Box<dyn Error>> {
        let deciles_statement = climo_db
            .stats_conn
            .prepare(include_str!("../sql/get_deciles.sql"))?;

        Ok(Self {
            climo_db,
            deciles_statement,
        })
    }

    /// Retrieve hourly `Deciles`s
    pub fn hourly_deciles(
        &mut self,
        site: &Site,
        model: &str,
        element: ClimoElement,
        start_time: NaiveDateTime,
        end_time: NaiveDateTime,
    ) -> Result<Vec<(NaiveDateTime, Deciles)>, Box<dyn Error>> {
        debug_assert!(end_time > start_time);
        debug_assert!(end_time - start_time < Duration::days(366));

        let start_year = start_time.year();
        let start_day_of_year = start_time.ordinal();
        let end_year = end_time.year();
        let end_day_of_year = end_time.ordinal();

        let local_db_conn = &self.climo_db.stats_conn;

        let data: Vec<(NaiveDateTime, Deciles)> = self
            .deciles_statement
            .query_map(params![site.id, model], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            // Filter out errors
            .filter_map(Result::ok)
            // Map the day of the year and hour of the day to the current date range data is being
            // selected for
            .map(|(day_of_year, hour, val)| {
                let year = if start_year == end_year || day_of_year >= start_day_of_year {
                    start_year
                } else if day_of_year <= end_day_of_year {
                    end_year
                } else {
                    unreachable!();
                };

                let valid_time = NaiveDate::from_yo(year, day_of_year).and_hms(hour, 0, 0);

                (valid_time, val)
            })
            // Filter out data that is outside the range I care about
            .filter(|(valid_time, _val): &(NaiveDateTime, _)| {
                *valid_time <= end_time && *valid_time >= start_time
            })
            // map the rowid to a decile
            .map(
                |(valid_time, rowid)| -> Result<(NaiveDateTime, Deciles), Box<dyn Error>> {
                    let blob = local_db_conn.blob_open(
                        DatabaseName::Main,
                        "deciles",
                        element.into_column_name(),
                        rowid,
                        true,
                    )?;

                    let deciles = Deciles::from_reader(blob)?;

                    Ok((valid_time, deciles))
                },
            )
            .filter_map(Result::ok)
            .collect();

        Ok(data)
    }
}
