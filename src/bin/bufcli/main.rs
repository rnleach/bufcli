//! bufcli
//!
//! Generate ad hoc model climatologies from Bufkit soundings and store the intermediate data in the
//! archive. These can be queried later by other tools to provide context to any given analysis.
mod builder;

use self::builder::build_climo;
use bufcli::ClimoDB;
use bufkit_data::{Archive, Model};
use clap::{crate_version, App, Arg};
use dirs::home_dir;
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
        "build" => build_climo(args),
        "update" => build_climo(args),
        "reset" => reset(args),
        _ => bail("Unknown operation."),
    }
}

#[derive(Debug)]
pub(crate) struct CmdLineArgs {
    root: PathBuf,
    sites: Vec<String>,
    models: Vec<Model>,
    operation: String,
}

fn parse_args() -> Result<CmdLineArgs, Box<dyn Error>> {
    let app = App::new("bufcli")
        .author("Ryan <rnleach@users.noreply.github.com>")
        .version(crate_version!())
        .about("Model sounding climatology.")
        .arg(
            Arg::with_name("sites")
                .multiple(true)
                .short("s")
                .long("sites")
                .takes_value(true)
                .help("Site identifiers (e.g. kord, katl, smn)."),
        )
        .arg(
            Arg::with_name("models")
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
            Arg::with_name("root")
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
            Arg::with_name("operation")
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
        .or_else(|| home_dir().map(|hd| hd.join("bufkit")))
        .expect("Invalid root.");
    let root_clone = root.clone();

    let arch = match Archive::connect(root) {
        arch @ Ok(_) => arch,
        err @ Err(_) => {
            println!("Unable to connect to db, printing error and exiting.");
            err
        }
    }?;

    let mut sites: Vec<String> = matches
        .values_of("sites")
        .into_iter()
        .flat_map(|site_iter| site_iter.map(ToOwned::to_owned))
        .collect();

    if sites.is_empty() {
        sites = arch.sites()?.into_iter().map(|site| site.id).collect();
    }

    let mut models: Vec<Model> = matches
        .values_of("models")
        .into_iter()
        .flat_map(|model_iter| model_iter.map(Model::from_str))
        .filter_map(Result::ok)
        .collect();

    if models.is_empty() {
        models = vec![Model::GFS, Model::NAM, Model::NAM4KM];
    }

    let operation: String = matches.value_of("operation").map(str::to_owned).unwrap();

    Ok(CmdLineArgs {
        root: root_clone,
        sites,
        models,
        operation,
    })
}

fn reset(args: CmdLineArgs) -> Result<(), Box<dyn Error>> {
    ClimoDB::delete_climo_db(&args.root)
}
