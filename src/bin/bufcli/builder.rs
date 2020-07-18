use crate::CmdLineArgs;
use bufkit_data::{Model, SiteInfo};
use std::{collections::HashSet, error::Error};

// Capacity of bounded channels used in data and deciles modules.
const CAPACITY: usize = 16;

mod data;
mod deciles;

pub(crate) fn build_climo(args: CmdLineArgs) -> Result<(), Box<dyn Error>> {
    let root = args.root.clone();

    let modified_pairs: HashSet<(SiteInfo, Model)> = data::build(args)?;
    deciles::build(modified_pairs, &root)?;

    Ok(())
}
