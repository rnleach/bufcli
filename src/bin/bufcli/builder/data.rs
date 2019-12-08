use super::CAPACITY;
use crate::CmdLineArgs;
use bufcli::{ClimoDB, ClimoPopulateInterface, StatsRecord};
use bufkit_data::{Archive, BufkitDataErr, Model, Site};
use chrono::NaiveDateTime;
use crossbeam_channel::{self as channel, Receiver, Sender};
use itertools::iproduct;
use pbr::ProgressBar;
use sounding_analysis::Sounding;
use sounding_bufkit::BufkitData;
use std::{
    collections::HashSet,
    error::Error,
    iter::FromIterator,
    path::Path,
    thread::{self, JoinHandle},
};
use threadpool;

pub(super) fn build(args: CmdLineArgs) -> Result<HashSet<(Site, Model)>, Box<dyn Error>> {
    use DataPopulateMsg::*;

    let root = &args.root.clone();

    // Channels for the main pipeline
    let (entry_point_snd, load_requests_rcv) = channel::bounded::<DataPopulateMsg>(CAPACITY);
    let (parse_requests_snd, parse_requests_rcv) = channel::bounded::<DataPopulateMsg>(CAPACITY);
    let (cli_requests_snd, cli_requests_rcv) = channel::bounded::<DataPopulateMsg>(CAPACITY);
    let (loc_requests_snd, loc_requests_rcv) = channel::bounded::<DataPopulateMsg>(CAPACITY);
    let (comp_notify_snd, comp_notify_rcv) = channel::bounded::<DataPopulateMsg>(CAPACITY);

    // Channel for adding stats to the climo database
    let (stats_snd, stats_rcv) = channel::bounded::<StatsRecord>(CAPACITY);

    // Hook everything together
    let stats_jh = start_stats_thread(root, stats_rcv)?;

    let (ep_jh, total_num) = start_entry_point_thread(args, entry_point_snd)?;
    let load_jh = start_load_thread(root, load_requests_rcv, parse_requests_snd)?;

    let parser_jh = start_parser_thread(parse_requests_rcv, cli_requests_snd)?;

    let cli_stats_jh =
        start_cli_stats_thread(cli_requests_rcv, loc_requests_snd, stats_snd.clone())?;

    let loc_stats_jh = start_location_stats_thread(loc_requests_rcv, comp_notify_snd, stats_snd)?;

    let mut site_model_pairs = HashSet::new();

    // Monitor progress and post updates here
    let mut pb = ProgressBar::new(total_num as u64);
    let arch = Archive::connect(root)?;
    for msg in comp_notify_rcv {
        match msg {
            PopulateCompleted { num, site, model } => {
                pb.set(num as u64);
                site_model_pairs.insert((site, model));
            }
            DataError {
                num,
                site,
                model,
                valid_time,
                msg,
            } => {
                print!("\u{001b}[300D\u{001b}[K");
                println!(
                    "Error parsing file, removing from archive: {} - {} - {}",
                    site.id, model, valid_time
                );
                println!("  {}", msg);
                pb.set(num as u64);
                if arch.file_exists(&site.id, model, &valid_time)? {
                    arch.remove(&site.id, model, &valid_time)?;
                }
            }
            _ => {
                print!("\u{001b}[300D\u{001b}[K");
                println!("Invalid message recieved in main thread: {:?}", msg);
            }
        }
    }

    pb.finish();

    println!("Waiting for populate service threads to shut down.");
    ep_jh.join().expect("Error joining entry point thread.")?;
    load_jh.join().expect("Error joining load thread.")?;
    parser_jh.join().expect("Error joining parsing thread.")?;
    cli_stats_jh
        .join()
        .expect("Error joining cli_stats thread.")?;
    loc_stats_jh
        .join()
        .expect("Error joining location thread.")?;
    stats_jh.join().expect("Error joining stats thread.")?;
    println!("Done populating climate data.");

    Ok(site_model_pairs)
}

// A join handle for the entry point thread, which returns and error with a string or nothing. It
// is also packaged with the total count of soundings that need to be possibly added to the
// archive index. Some (many) will likely already be there and can be skipped, but we need to
// check each and every one.
type EntryPointData = (JoinHandle<Result<(), String>>, i64);

fn start_entry_point_thread(
    args: CmdLineArgs,
    entry_point_snd: Sender<DataPopulateMsg>,
) -> Result<EntryPointData, Box<dyn Error>> {
    let arch = Archive::connect(&args.root)?;
    let sites = args
        .sites
        .iter()
        .map(|s| arch.site_info(s))
        .collect::<Result<Vec<Site>, BufkitDataErr>>()?;

    let mut total = 0;
    for (site, &model) in iproduct!(&sites, &args.models) {
        total += arch.count_init_times(&site.id, model)?;
    }

    let jh = thread::Builder::new().name("Generator".to_string()).spawn(
        move || -> Result<(), String> {
            let force_rebuild = args.operation == "build";

            let arch = Archive::connect(&args.root).map_err(|err| err.to_string())?;

            let climo_db = ClimoDB::connect_or_create(&args.root).map_err(|err| err.to_string())?;
            let mut climo_db =
                ClimoPopulateInterface::initialize(&climo_db).map_err(|err| err.to_string())?;

            let mut counter = 0;
            for (site, &model) in iproduct!(&sites, &args.models) {
                let init_times = arch
                    .init_times(&site.id, model)
                    .map_err(|err| err.to_string())?;
                let init_times: HashSet<NaiveDateTime> = HashSet::from_iter(init_times);

                let done_times = if !force_rebuild {
                    HashSet::from_iter(
                        climo_db
                            .valid_times_for(&site, model)
                            .map_err(|err| err.to_string())?,
                    )
                } else {
                    HashSet::new()
                };

                let mut small_counter = 0;
                for &init_time in init_times.difference(&done_times) {
                    counter += 1;
                    small_counter += 1;

                    let message = DataPopulateMsg::Load {
                        model,
                        init_time,
                        site: site.clone(),
                        num: counter,
                    };
                    entry_point_snd
                        .send(message)
                        .map_err(|err| err.to_string())?;
                }

                counter += init_times.len() - small_counter;
            }
            Ok(())
        },
    )?;

    Ok((jh, total))
}

fn start_load_thread(
    root: &Path,
    load_requests_rcv: Receiver<DataPopulateMsg>,
    parse_requests_snd: Sender<DataPopulateMsg>,
) -> Result<JoinHandle<Result<(), String>>, Box<dyn Error>> {
    let root = root.to_path_buf();

    let jh = thread::Builder::new()
        .name("FileLoader".to_string())
        .spawn(move || -> Result<(), String> {
            let arch = Archive::connect(root).map_err(|err| err.to_string())?;

            for load_req in load_requests_rcv {
                let message = match load_req {
                    DataPopulateMsg::Load {
                        num,
                        site,
                        model,
                        init_time,
                    } => match arch.retrieve(&site.id, model, init_time) {
                        Ok(data) => DataPopulateMsg::Parse {
                            num,
                            site,
                            model,
                            init_time,
                            data,
                        },
                        Err(err) => DataPopulateMsg::DataError {
                            num,
                            site,
                            model,
                            valid_time: init_time,
                            msg: err.to_string(),
                        },
                    },
                    message => message,
                };

                parse_requests_snd
                    .send(message)
                    .map_err(|err| err.to_string())?;
            }
            Ok(())
        })?;

    Ok(jh)
}

fn start_parser_thread(
    parse_requests: Receiver<DataPopulateMsg>,
    cli_requests: Sender<DataPopulateMsg>,
) -> Result<JoinHandle<Result<(), String>>, Box<dyn Error>> {
    let jh = thread::Builder::new()
        .name("SoundingParser".to_string())
        .spawn(move || -> Result<(), String> {
            for msg in parse_requests {
                if let DataPopulateMsg::Parse {
                    num,
                    site,
                    model,
                    init_time,
                    data,
                } = msg
                {
                    let bufkit_data = match BufkitData::init(&data, "") {
                        Ok(bufkit_data) => bufkit_data,
                        Err(err) => {
                            let message = DataPopulateMsg::DataError {
                                num,
                                site,
                                model,
                                valid_time: init_time,
                                msg: err.to_string(),
                            };
                            cli_requests.send(message).map_err(|err| err.to_string())?;
                            continue;
                        }
                    };

                    for (snd, _) in bufkit_data.into_iter().take_while(|(snd, _)| {
                        snd.lead_time()
                            .into_option()
                            .map(|lt| i64::from(lt) < model.hours_between_runs())
                            .unwrap_or(false)
                    }) {
                        if let Some(valid_time) = snd.valid_time() {
                            let message = DataPopulateMsg::CliData {
                                num,
                                site: site.clone(),
                                model,
                                valid_time,
                                snd: Box::new(snd),
                            };
                            cli_requests.send(message).map_err(|err| err.to_string())?;
                        } else {
                            let message = DataPopulateMsg::DataError {
                                num,
                                site: site.clone(),
                                model,
                                valid_time: init_time,
                                msg: "No valid time".to_string(),
                            };
                            cli_requests.send(message).map_err(|err| err.to_string())?;
                        }
                    }
                } else {
                    cli_requests.send(msg).map_err(|err| err.to_string())?;
                }
            }

            Ok(())
        })?;

    Ok(jh)
}

fn start_cli_stats_thread(
    cli_requests: Receiver<DataPopulateMsg>,
    location_requests: Sender<DataPopulateMsg>,
    climo_update_requests: Sender<StatsRecord>,
) -> Result<JoinHandle<Result<(), String>>, Box<dyn Error>> {
    let jh = thread::Builder::new()
        .name("CliStatsBuilder".to_string())
        .spawn(move || -> Result<(), String> {
            const POOL_SIZE: usize = 12;

            let pool = threadpool::Builder::new()
                .num_threads(POOL_SIZE)
                .thread_name("CliStatsCalc".to_string())
                .build();

            for _ in 0..POOL_SIZE {
                let local_cli_requests = cli_requests.clone();
                let local_location_requests = location_requests.clone();
                let local_update_requests = climo_update_requests.clone();

                pool.execute(move || {
                    for msg in local_cli_requests {
                        if let DataPopulateMsg::CliData {
                            num,
                            site,
                            model,
                            valid_time,
                            snd,
                        } = msg
                        {
                            {
                                let message = StatsRecord::create_cli_data(
                                    site.clone(),
                                    model,
                                    valid_time,
                                    &snd,
                                );
                                local_update_requests
                                    .send(message)
                                    .expect("Error sending message.");
                            }

                            let message = DataPopulateMsg::Location {
                                num,
                                site,
                                model,
                                valid_time,
                                snd,
                            };
                            local_location_requests
                                .send(message)
                                .expect("Error sending message.");
                        } else {
                            local_location_requests
                                .send(msg)
                                .expect("Error sending message.");
                        }
                    }
                });
            }

            pool.join();

            Ok(())
        })?;

    Ok(jh)
}

fn start_location_stats_thread(
    location_requests: Receiver<DataPopulateMsg>,
    completed_notification: Sender<DataPopulateMsg>,
    climo_update_requests: Sender<StatsRecord>,
) -> Result<JoinHandle<Result<(), String>>, Box<dyn Error>> {
    let jh = thread::Builder::new()
        .name("LocationUpdater".to_string())
        .spawn(move || -> Result<(), String> {
            for msg in location_requests {
                if let DataPopulateMsg::Location {
                    num,
                    site,
                    model,
                    valid_time,
                    snd,
                } = msg
                {
                    if snd
                        .lead_time()
                        .into_option()
                        .map(|lt| lt == 0)
                        .unwrap_or(true)
                    {
                        match StatsRecord::create_location_data(
                            site.clone(),
                            model,
                            valid_time,
                            &snd,
                        ) {
                            Ok(msg) => {
                                climo_update_requests
                                    .send(msg)
                                    .map_err(|err| err.to_string())?;

                                completed_notification
                                    .send(DataPopulateMsg::PopulateCompleted { num, site, model })
                                    .map_err(|err| err.to_string())?;
                            }
                            Err(site) => {
                                let message = DataPopulateMsg::DataError {
                                    num,
                                    site,
                                    model,
                                    valid_time,
                                    msg: "Missing location information".to_string(),
                                };
                                completed_notification
                                    .send(message)
                                    .map_err(|err| err.to_string())?;
                            }
                        }
                    }
                } else {
                    completed_notification
                        .send(msg)
                        .map_err(|err| err.to_string())?;
                }
            }

            Ok(())
        })?;

    Ok(jh)
}

fn start_stats_thread(
    root: &Path,
    stats_rcv: Receiver<StatsRecord>,
) -> Result<JoinHandle<Result<(), String>>, Box<dyn Error>> {
    let root = root.to_path_buf();

    let jh = thread::Builder::new()
        .name("ClimoWriter".to_string())
        .spawn(move || -> Result<(), String> {
            let climo_db = ClimoDB::connect_or_create(&root).map_err(|err| err.to_string())?;
            let mut climo_db =
                ClimoPopulateInterface::initialize(&climo_db).map_err(|err| err.to_string())?;

            for msg in stats_rcv {
                climo_db.add(msg).map_err(|err| err.to_string())?;
            }

            Ok(())
        })?;

    Ok(jh)
}

#[derive(Debug)]
enum DataPopulateMsg {
    Load {
        num: usize,
        site: Site,
        model: Model,
        init_time: NaiveDateTime,
    },
    Parse {
        num: usize,
        site: Site,
        model: Model,
        init_time: NaiveDateTime,
        data: String,
    },
    CliData {
        num: usize,
        site: Site,
        model: Model,
        valid_time: NaiveDateTime,
        snd: Box<Sounding>,
    },
    Location {
        num: usize,
        site: Site,
        model: Model,
        valid_time: NaiveDateTime,
        snd: Box<Sounding>,
    },
    PopulateCompleted {
        num: usize,
        site: Site,
        model: Model,
    },
    DataError {
        num: usize,
        site: Site,
        model: Model,
        valid_time: NaiveDateTime,
        msg: String,
    },
}
