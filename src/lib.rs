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
    climo_db::{
        ClimoCDFBuilderInterface, ClimoDB, ClimoElement, ClimoPopulateInterface,
        ClimoQueryInterface, StatsRecord,
    },
    distributions::{CumulativeDistribution, Deciles, Percentile},
    error::BufcliError,
};

//
// Private implementation.
//
mod climo_db;
mod distributions;
mod error;
