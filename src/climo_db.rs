use rusqlite::{Connection, OpenFlags};
use std::{error::Error, path::Path};

pub struct ClimoDB {
    conn: Connection,
}

impl ClimoDB {
    pub const CLIMO_DIR: &'static str = "climo";
    pub const CLIMO_DB: &'static str = "climo.db";

    pub fn delete_climo_db(arch_root: &Path) -> Result<(), Box<dyn Error>> {
        let data_path = &arch_root.join(Self::CLIMO_DIR).join(Self::CLIMO_DB);
        std::fs::remove_file(data_path)?;
        Ok(())
    }

    pub fn connect_or_create(arch_root: &Path) -> Result<Self, Box<dyn Error>> {
        let climo_path = arch_root.join(Self::CLIMO_DIR);
        if !climo_path.is_dir() {
            std::fs::create_dir(&climo_path)?;
        }

        let data_file = climo_path.join(Self::CLIMO_DB);
        let conn = Connection::open_with_flags(
            data_file,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;

        // Create the database if it doesn't exist.
        conn.execute_batch(include_str!("climo_db/create_climate_data_db.sql"))?;

        Ok(ClimoDB { conn })
    }
}

/// Elements we can query for climo data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClimoElement {
    HDW,
    BlowUpDt,
    BlowUpHeight,
    DCAPE,
}

mod populate;
pub use populate::ClimoPopulateInterface;

mod stats_record;
pub use stats_record::StatsRecord;
