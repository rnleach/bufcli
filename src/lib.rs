//! bufcli
//!
//! Library for interfacing with the climo portion of a
//! [bufkit-data](https://github.com/rnleach/bufkit-data) archive of
//! [bufkit](https://training.weather.gov/wdtd/tools/BUFKIT/) files.
//!

//
// Public API
//
pub use crate::{
    climo_db::{ClimoBuilderInterface, ClimoDB, ClimoElement, ClimoQueryInterface, HourlyDeciles},
    error::BufcliError,
    stats_record::StatsRecord,
};

//
// Private implementation.
//
mod climo_db;
mod error;
mod stats_record;
