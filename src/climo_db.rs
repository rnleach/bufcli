use rusqlite::{Connection, OpenFlags};
use std::{error::Error, fs::create_dir, path::Path};

pub struct ClimoDB {
    conn: Connection,
    stats_conn: Connection,
}

impl ClimoDB {
    pub const CLIMO_DIR: &'static str = "climo";
    pub const CLIMO_DB: &'static str = "climo.db";
    pub const CLIMO_STATS_DB: &'static str = "climo_stats.db";

    pub fn delete_climo_db(arch_root: &Path) -> Result<(), Box<dyn Error>> {
        let data_path = &arch_root.join(Self::CLIMO_DIR).join(Self::CLIMO_DB);
        let stats_path = &arch_root.join(Self::CLIMO_DIR).join(Self::CLIMO_STATS_DB);
        std::fs::remove_file(data_path)?;
        std::fs::remove_file(stats_path)?;
        Ok(())
    }

    pub fn connect_or_create(arch_root: &Path) -> Result<Self, Box<dyn Error>> {
        let climo_path = arch_root.join(Self::CLIMO_DIR);
        if !climo_path.is_dir() {
            create_dir(&climo_path)?;
        }

        let data_file = climo_path.join(Self::CLIMO_DB);
        let conn = Connection::open_with_flags(
            data_file,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;

        // Create the database if it doesn't exist.
        conn.execute_batch(include_str!("sql/create_climate_data_db.sql"))?;

        let stats_file = climo_path.join(Self::CLIMO_STATS_DB);
        let stats_conn = Connection::open_with_flags(
            stats_file,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;

        // Create the database if it doesn't exist.
        stats_conn.execute_batch(include_str!("sql/create_climate_stats_db.sql"))?;

        Ok(ClimoDB { conn, stats_conn })
    }
}

mod build_cdf;
pub use build_cdf::ClimoCDFBuilderInterface;

mod climo_element;
pub use climo_element::ClimoElement;

mod populate;
pub use populate::ClimoPopulateInterface;

mod query;
pub use query::ClimoQueryInterface;

mod stats_record;
pub use stats_record::StatsRecord;
