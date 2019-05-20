use bufkit_data::{Model, Site};
use chrono::{Datelike, FixedOffset, NaiveDateTime, TimeZone, Timelike};
use rusqlite::types::ToSql;
use rusqlite::{Connection, OpenFlags, Statement, NO_PARAMS};
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

        //
        //  Create the locations
        //
        conn.execute(
            "CREATE TABLE IF NOT EXISTS locations (
                site        TEXT NOT NULL,
                model       TEXT NOT NULL,
                start_date  TEXT NOT NULL,
                latitude    NUM  NOT NULL,
                longitude   NUM  NOT NULL,
                elevation_m NUM  NOT NULL,
                UNIQUE(site, model, latitude, longitude, elevation_m)
            )",
            NO_PARAMS,
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS locations_idx ON locations (site, model)",
            NO_PARAMS,
        )?;

        //
        // Create the the climate table
        //
        conn.execute(
            "CREATE TABLE IF NOT EXISTS cli (
                site         TEXT NOT NULL,
                model        TEXT NOT NULL,

                valid_time   TEXT NOT NULL,
                year_lcl     INT  NOT NULL,
                month_lcl    INT  NOT NULL,
                day_lcl      INT  NOT NULL,
                hour_lcl     INT  NOT NULL,

                haines_high  INT,
                haines_mid   INT,
                haines_low   INT,
                hdw          INT,
                conv_t_def_c REAL,
                dry_cape     INT,
                wet_cape     INT,
                cape_ratio   REAL,
                ccl_agl_m    INT,
                el_asl_m     INT,

                mlcape       INT,
                mlcin        INT,
                dcape        INT,

                ml_sfc_t_c   INT,
                ml_sfc_rh    INT,
                sfc_wspd_kt  INT,
                sfc_wdir     INT,

                PRIMARY KEY (site, valid_time, model, year_lcl, month_lcl, day_lcl, hour_lcl)
            )",
            NO_PARAMS,
        )?;

        conn.execute("PRAGMA cache_size=100000", NO_PARAMS)?;

        Ok(ClimoDB { conn })
    }
}

#[derive(Clone, Debug)]
pub enum StatsRecord {
    CliData {
        site: Site,
        model: Model,
        valid_time: NaiveDateTime,

        hns_high: i32,
        hns_mid: i32,
        hns_low: i32,
        hdw: i32,
        conv_t_def: Option<f64>,
        dry_cape: Option<i32>,
        wet_cape: Option<i32>,
        cape_ratio: Option<f64>,
        ccl_agl_m: Option<i32>,
        el_asl_m: Option<i32>,

        mlcape: Option<i32>,
        mlcin: Option<i32>,
        dcape: Option<i32>,

        ml_sfc_t: Option<i32>,
        ml_sfc_rh: Option<i32>,
        sfc_wspd_kt: Option<i32>,
        sfc_wdir: Option<i32>,
    },
    Location {
        site: Site,
        model: Model,
        valid_time: NaiveDateTime,
        lat: f64,
        lon: f64,
        elev_m: f64,
    },
}

#[derive(Clone, Debug, Hash)]
struct StatsRecordKey<'a> {
    site_id: &'a str,
    model: Model,
    valid_time: NaiveDateTime,
}

/// The struct creates and caches several prepared statements.
pub struct ClimoDBInterface<'a, 'b: 'a> {
    climo_db: &'b ClimoDB,
    add_location_query: Statement<'a>,
    add_data_query: Statement<'a>,
    init_times_query: Statement<'a>,
    write_buffer: Vec<StatsRecord>,
}

impl<'a, 'b> ClimoDBInterface<'a, 'b> {
    const BUFSIZE: usize = 2048;
    pub fn initialize(climo_db: &'b ClimoDB) -> Result<Self, Box<dyn Error>> {
        let conn = &climo_db.conn;
        let add_location_query = conn.prepare(
            "
                INSERT OR IGNORE INTO 
                locations (site, model, start_date, latitude, longitude, elevation_m)
                VALUES(?1, ?2, ?3, ?4, ?5, ?6)
            ",
        )?;

        let add_data_query = conn.prepare(
            "
                INSERT OR REPLACE INTO
                cli (site, model, valid_time, year_lcl, month_lcl, day_lcl, hour_lcl, 
                    haines_high, haines_mid, haines_low, hdw, conv_t_def_c, dry_cape, wet_cape,
                    cape_ratio, ccl_agl_m, el_asl_m, mlcape, mlcin, dcape, ml_sfc_t_c, ml_sfc_rh,
                    sfc_wspd_kt, sfc_wdir)
                VALUES 
                (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18,
                 ?19, ?20, ?21, ?22, ?23, ?24)
            ",
        )?;

        let init_times_query = conn.prepare(
            "
                SELECT valid_time FROM 
                cli
                WHERE site = ?1 AND MODEL = ?2
            ",
        )?;

        Ok(ClimoDBInterface {
            climo_db,
            add_location_query,
            add_data_query,
            init_times_query,
            write_buffer: Vec::with_capacity(ClimoDBInterface::BUFSIZE),
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
            .query_map(&[&site.id as &ToSql, &model_str], |row| row.get(0))?
            .collect();
        let valid_times = valid_times?;

        Ok(valid_times)
    }

    #[inline]
    pub fn add(&mut self, record: StatsRecord) -> Result<(), Box<dyn Error>> {
        debug_assert!(self.write_buffer.len() <= ClimoDBInterface::BUFSIZE);
        self.write_buffer.push(record);

        if self.write_buffer.len() == ClimoDBInterface::BUFSIZE {
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
                    hns_low,
                    hns_mid,
                    hns_high,
                    hdw,
                    conv_t_def,
                    dry_cape,
                    wet_cape,
                    cape_ratio,
                    ccl_agl_m,
                    el_asl_m,
                    mlcape,
                    mlcin,
                    dcape,
                    ml_sfc_t,
                    ml_sfc_rh,
                    sfc_wspd_kt,
                    sfc_wdir,
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
                            &site.id as &ToSql,
                            &model.as_static(),
                            &valid_time as &ToSql,
                            &year_lcl as &ToSql,
                            &month_lcl as &ToSql,
                            &day_lcl as &ToSql,
                            &hour_lcl as &ToSql,
                            &hns_high as &ToSql,
                            &hns_mid as &ToSql,
                            &hns_low as &ToSql,
                            &hdw as &ToSql,
                            &conv_t_def as &ToSql,
                            &dry_cape as &ToSql,
                            &wet_cape as &ToSql,
                            &cape_ratio as &ToSql,
                            &ccl_agl_m as &ToSql,
                            &el_asl_m as &ToSql,
                            &mlcape as &ToSql,
                            &mlcin as &ToSql,
                            &dcape as &ToSql,
                            &ml_sfc_t as &ToSql,
                            &ml_sfc_rh as &ToSql,
                            &sfc_wspd_kt as &ToSql,
                            &sfc_wdir as &ToSql,
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
                        &site.id as &ToSql,
                        &model.as_static(),
                        &valid_time as &ToSql,
                        &lat as &ToSql,
                        &lon as &ToSql,
                        &elev_m as &ToSql,
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

impl<'a, 'b> Drop for ClimoDBInterface<'a, 'b> {
    fn drop(&mut self) {
        self.flush().unwrap()
    }
}
