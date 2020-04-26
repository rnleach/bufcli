use serde::{Deserialize, Serialize};
use std::{convert::TryInto, error::Error, io::Read};

/// Represents the emperical CDF of a set of values
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CumulativeDistribution {
    sorted_values: Vec<f64>,
}

/// Pre-calculated deciles.
#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub struct Deciles {
    deciles: [f64; 11],
}

/// Percentile, which must be between 0 and 100 inclusive.
#[derive(Clone, Copy, Debug, Hash)]
pub struct Percentile(u32);

impl From<u32> for Percentile {
    fn from(val: u32) -> Self {
        assert!(
            val <= 100,
            "Invalid percentile, must be in 0 -> 100 range, inclusive."
        );
        Self(val)
    }
}

impl Into<f64> for Percentile {
    fn into(self) -> f64 {
        f64::from(self.0) / 100.0
    }
}

impl CumulativeDistribution {
    pub fn new(data: Vec<f64>) -> Self {
        let mut data = data;

        data.retain(|val| !val.is_nan());
        data.sort_unstable_by(|a: &f64, b: &f64| a.partial_cmp(b).unwrap());

        Self {
            sorted_values: data,
        }
    }

    /// User must ensure that data contains no NAN values and is sorted in ascending order already.
    pub unsafe fn presorted_new(data: Vec<f64>) -> Self {
        Self {
            sorted_values: data,
        }
    }

    pub fn percentile_of_value(&self, value: f64) -> Percentile {
        assert!(!value.is_nan());

        let index: usize = self
            .sorted_values
            .binary_search_by(|&probe| probe.partial_cmp(&value).unwrap())
            .unwrap_or_else(|err| err);

        Percentile(
            ((index * 100) / (self.sorted_values.len() - 1))
                .try_into()
                .unwrap(),
        )
    }

    pub fn value_at_percentile(&self, percentile: Percentile) -> f64 {
        let index: usize = (percentile.0 as usize) * (self.sorted_values.len() - 1) / 100;
        self.sorted_values[index]
    }

    pub fn deciles(&self) -> Deciles {
        let deciles: [f64; 11] = [
            self.value_at_percentile(Percentile(0)),
            self.value_at_percentile(Percentile(10)),
            self.value_at_percentile(Percentile(20)),
            self.value_at_percentile(Percentile(30)),
            self.value_at_percentile(Percentile(40)),
            self.value_at_percentile(Percentile(50)),
            self.value_at_percentile(Percentile(60)),
            self.value_at_percentile(Percentile(70)),
            self.value_at_percentile(Percentile(80)),
            self.value_at_percentile(Percentile(90)),
            self.value_at_percentile(Percentile(100)),
        ];

        Deciles { deciles }
    }
}

impl Deciles {

    // Serialize and deserialize Deciles for storing in a database. 

    pub(crate) fn as_bytes(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        bincode::serialize(self).map_err(Into::into)
    }

    pub(crate) fn from_reader<R: Read>(reader: R) -> Result<Self, Box<dyn Error>> {
        bincode::deserialize_from(reader).map_err(Into::into)
    }

    /// Retrieve the value of a percentile, which must be a decile.
    ///
    /// panics if percentile % 10 != 0
    pub fn value_at_percentile(&self, percentile: Percentile) -> f64 {
        let Percentile(pct) = percentile;

        if pct % 10 != 0 {
            panic!("Percentile must be a decile such that pct % 10 == 0");
        }

        let idx = (pct / 10) as usize;

        self.deciles[idx]
    }
}
