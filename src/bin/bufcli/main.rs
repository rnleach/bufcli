//! bufcli
//!
//! Generate ad hoc model climatologies from Bufkit soundings and store the intermediate data in the
//! archive. These can be queried later by other tools to provide context to any given analysis.
mod builder;

use bufkit_data::{Archive, BufkitDataErr, Model, SiteInfo};
use std::{error::Error, path::PathBuf, str::FromStr};
use strum::IntoEnumIterator;

fn main() {
    if let Err(e) = run() {
        println!("error: {}", e);

        let mut err = &*e;

        while let Some(cause) = err.source() {
            println!("caused by: {}", cause);
            err = cause;
        }

        ::std::process::exit(1);
    }
}

pub fn bail(msg: &str) -> ! {
    println!("{}", msg);
    ::std::process::exit(1);
}

fn run() -> Result<(), Box<dyn Error>> {
    let args = parse_args()?;

    match args.operation.as_ref() {
        "build" => builder::build_climo(args),
        "update" => builder::build_climo(args),
        "reset" => reset(args),
        _ => bail("Unknown operation."),
    }
}

#[derive(Debug)]
pub(crate) struct CmdLineArgs {
    root: PathBuf,
    site_model_pairs: Vec<(SiteInfo, Model)>,
    operation: String,
}

fn parse_args() -> Result<CmdLineArgs, Box<dyn Error>> {
    let app = clap::App::new("bufcli")
        .author("Ryan <rnleach@users.noreply.github.com>")
        .version(clap::crate_version!())
        .about("Model sounding climatology.")
        .arg(
            clap::Arg::with_name("sites")
                .multiple(true)
                .short("s")
                .long("sites")
                .takes_value(true)
                .help("Site identifiers (e.g. kord, katl, smn)."),
        )
        .arg(
            clap::Arg::with_name("models")
                .multiple(true)
                .short("m")
                .long("models")
                .takes_value(true)
                .possible_values(
                    &Model::iter()
                        .map(|val| val.as_static_str())
                        .collect::<Vec<&str>>(),
                )
                .help("Allowable models for this operation/program.")
                .long_help(concat!(
                    "Allowable models for this operation/program.",
                    " Default is to use all possible values."
                )),
        )
        .arg(
            clap::Arg::with_name("root")
                .short("r")
                .long("root")
                .takes_value(true)
                .help("Set the root of the archive.")
                .long_help(
                    "Set the root directory of the archive you are invoking this command for.",
                )
                .conflicts_with("create")
                .global(true),
        )
        .arg(
            clap::Arg::with_name("operation")
                .index(1)
                .takes_value(true)
                .required(true)
                .possible_values(&["build", "reset", "update"])
                .help("Build, update, or delete the climatology database.")
                .long_help(concat!(
                    "Either build, update, or reset the climate database. 'reset' deletes the",
                    " whole climate database and starts over fresh. Update will only add data for",
                    " dates not already in the database.",
                )),
        );

    let matches = app.get_matches();

    let root = matches
        .value_of("root")
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|hd| hd.join("bufkit")))
        .expect("Invalid root.");

    let arch = match Archive::connect(&root) {
        arch @ Ok(_) => arch,
        err @ Err(_) => {
            println!("Unable to connect to db, printing error and exiting.");
            err
        }
    }?;

    let mut models: Vec<Model> = matches
        .values_of("models")
        .into_iter()
        .flat_map(|model_iter| model_iter.map(Model::from_str))
        .filter_map(Result::ok)
        .collect();

    if models.is_empty() {
        models = vec![Model::GFS, Model::NAM, Model::NAM4KM];
    }

    let sites: Vec<String> = matches
        .values_of("sites")
        .into_iter()
        .flat_map(|site_iter| site_iter.map(ToOwned::to_owned))
        .collect();

    let site_model_pairs: Vec<(SiteInfo, Model)> = if sites.is_empty() {
        let mut site_model_pairs = vec![];
        let sites = arch.sites()?;
        for site in sites.into_iter() {
            if site.name.is_none() {
                println!("Skipping site with no name: {}", site);
                continue;
            }

            let site_models = arch.models(site.station_num)?;
            for model in site_models {
                if models.contains(&model) {
                    site_model_pairs.push((site.clone(), model));
                }
            }
        }
        site_model_pairs
    } else {
        let mut site_model_pairs = vec![];
        for model in models {
            for site in &sites {
                let site_stn_num = match arch.station_num_for_id(site, model) {
                    Ok(station_num) => station_num,
                    Err(BufkitDataErr::NotInIndex) => {
                        println!("Skipping site not in index: {}", site);
                        continue;
                    }
                    Err(err) => return Err(err.into()),
                };

                let site_info = match arch.site(site_stn_num) {
                    Some(site_info) => site_info,
                    None => continue,
                };

                if site_info.name.is_none() {
                    println!("Skipping site with no name: {}", site_info);
                    continue;
                }

                site_model_pairs.push((site_info, model));
            }
        }
        site_model_pairs
    };

    let operation: String = matches.value_of("operation").map(str::to_owned).unwrap();

    Ok(CmdLineArgs {
        root,
        site_model_pairs,
        operation,
    })
}

fn reset(args: CmdLineArgs) -> Result<(), Box<dyn Error>> {
    bufcli::ClimoDB::delete_climo_db(&args.root)
}
