use crate::CmdLineArgs;
use std::error::Error;

// Capacity of bounded channels used in data and deciles modules.
const CAPACITY: usize = 16;

mod data;

pub(crate) fn build_climo(args: CmdLineArgs) -> Result<(), Box<dyn Error>> {
    data::build(args)?;

    Ok(())
}
