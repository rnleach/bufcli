use crate::CmdLineArgs;
use bufcli::{ClimoBuilderInterface, ClimoDB, StatsRecord};
use bufkit_data::{Archive, BufkitDataErr, Model, Site};
use chrono::NaiveDateTime;
use crossbeam_channel::{self as channel, Receiver, Sender};
use itertools::iproduct;
use pbr::ProgressBar;
use sounding_analysis::Analysis;
use sounding_bufkit::BufkitData;
use std::{
    collections::HashSet,
    error::Error,
    iter::FromIterator,
    path::Path,
    thread::{self, JoinHandle},
};
use threadpool;

const CAPACITY: usize = 16;

pub(crate) fn build_climo(args: CmdLineArgs) -> Result<(), Box<dyn Error>> {
    use self::PipelineMessage::*;

    let root = &args.root.clone();

    // Channels for the main pipeline
    let (entry_point_snd, load_requests_rcv) = channel::bounded::<PipelineMessage>(CAPACITY);
    let (parse_requests_snd, parse_requests_rcv) = channel::bounded::<PipelineMessage>(CAPACITY);
    let (cli_requests_snd, cli_requests_rcv) = channel::bounded::<PipelineMessage>(CAPACITY);
    let (loc_requests_snd, loc_requests_rcv) = channel::bounded::<PipelineMessage>(CAPACITY);
    let (comp_notify_snd, comp_notify_rcv) = channel::bounded::<PipelineMessage>(CAPACITY);

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

    // Monitor progress and post updates here
    let mut pb = ProgressBar::new(total_num as u64);
    let arch = Archive::connect(root)?;
    for msg in comp_notify_rcv {
        match msg {
            Completed { num } => {
                pb.set(num as u64);
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

    println!("Waiting for service threads to shut down.");
    ep_jh.join().expect("Error joining thread.")?;
    load_jh.join().expect("Error joining thread.")?;
    parser_jh.join().expect("Error joining thread.")?;
    cli_stats_jh.join().expect("Error joining thread.")?;
    loc_stats_jh.join().expect("Error joining thread.")?;
    stats_jh.join().expect("Error joining thread.")?;
    println!("Done.");

    Ok(())
}

#[allow(clippy::type_complexity)]
fn start_entry_point_thread(
    args: CmdLineArgs,
    entry_point_snd: Sender<PipelineMessage>,
) -> Result<(JoinHandle<Result<(), String>>, i64), Box<dyn Error>> {
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
                ClimoBuilderInterface::initialize(&climo_db).map_err(|err| err.to_string())?;

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

                    let message = PipelineMessage::Load {
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
    load_requests_rcv: Receiver<PipelineMessage>,
    parse_requests_snd: Sender<PipelineMessage>,
) -> Result<JoinHandle<Result<(), String>>, Box<dyn Error>> {
    let root = root.to_path_buf();

    let jh = thread::Builder::new()
        .name("FileLoader".to_string())
        .spawn(move || -> Result<(), String> {
            let arch = Archive::connect(root).map_err(|err| err.to_string())?;

            for load_req in load_requests_rcv {
                let message = match load_req {
                    PipelineMessage::Load {
                        num,
                        site,
                        model,
                        init_time,
                    } => match arch.retrieve(&site.id, model, init_time) {
                        Ok(data) => PipelineMessage::Parse {
                            num,
                            site,
                            model,
                            init_time,
                            data,
                        },
                        Err(err) => PipelineMessage::DataError {
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
    parse_requests: Receiver<PipelineMessage>,
    cli_requests: Sender<PipelineMessage>,
) -> Result<JoinHandle<Result<(), String>>, Box<dyn Error>> {
    let jh = thread::Builder::new()
        .name("SoundingParser".to_string())
        .spawn(move || -> Result<(), String> {
            for msg in parse_requests {
                if let PipelineMessage::Parse {
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
                            let message = PipelineMessage::DataError {
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

                    for anal in bufkit_data.into_iter().take_while(|anal| {
                        anal.sounding()
                            .lead_time()
                            .into_option()
                            .and_then(|lt| Some(i64::from(lt) < model.hours_between_runs()))
                            .unwrap_or(false)
                    }) {
                        if let Some(valid_time) = anal.sounding().valid_time() {
                            let message = PipelineMessage::CliData {
                                num,
                                site: site.clone(),
                                model,
                                valid_time,
                                anal: Box::new(anal),
                            };
                            cli_requests.send(message).map_err(|err| err.to_string())?;
                        } else {
                            let message = PipelineMessage::DataError {
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
    cli_requests: Receiver<PipelineMessage>,
    location_requests: Sender<PipelineMessage>,
    climo_update_requests: Sender<StatsRecord>,
) -> Result<JoinHandle<Result<(), String>>, Box<dyn Error>> {
    let jh = thread::Builder::new()
        .name("CliDataStatsCalc".to_string())
        .spawn(move || -> Result<(), String> {
            const POOL_SIZE: usize = 2;

            let pool = threadpool::Builder::new()
                .num_threads(POOL_SIZE)
                .thread_name("CliDataStatsCalc_".to_string())
                .build();

            for _ in 0..POOL_SIZE {
                let local_cli_requests = cli_requests.clone();
                let local_location_requests = location_requests.clone();
                let local_update_requests = climo_update_requests.clone();

                pool.execute(move || {
                    for msg in local_cli_requests {
                        if let PipelineMessage::CliData {
                            num,
                            site,
                            model,
                            valid_time,
                            anal,
                        } = msg
                        {
                            {
                                let message = StatsRecord::create_cli_data(
                                    site.clone(),
                                    model,
                                    valid_time,
                                    &anal,
                                );
                                local_update_requests
                                    .send(message)
                                    .expect("Error sending message.");
                            }

                            let message = PipelineMessage::Location {
                                num,
                                site,
                                model,
                                valid_time,
                                anal,
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
    location_requests: Receiver<PipelineMessage>,
    completed_notification: Sender<PipelineMessage>,
    climo_update_requests: Sender<StatsRecord>,
) -> Result<JoinHandle<Result<(), String>>, Box<dyn Error>> {
    let jh = thread::Builder::new()
        .name("LocationUpdater".to_string())
        .spawn(move || -> Result<(), String> {
            for msg in location_requests {
                if let PipelineMessage::Location {
                    num,
                    site,
                    model,
                    valid_time,
                    anal,
                } = msg
                {
                    if anal
                        .sounding()
                        .lead_time()
                        .into_option()
                        .map(|lt| lt == 0)
                        .unwrap_or(true)
                    {
                        match StatsRecord::create_location_data(site, model, valid_time, &anal) {
                            Ok(msg) => {
                                climo_update_requests
                                    .send(msg)
                                    .map_err(|err| err.to_string())?;

                                completed_notification
                                    .send(PipelineMessage::Completed { num })
                                    .map_err(|err| err.to_string())?;
                            }
                            Err(site) => {
                                let message = PipelineMessage::DataError {
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
                ClimoBuilderInterface::initialize(&climo_db).map_err(|err| err.to_string())?;

            for msg in stats_rcv {
                climo_db.add(msg).map_err(|err| err.to_string())?;
            }

            Ok(())
        })?;

    Ok(jh)
}

#[derive(Debug)]
enum PipelineMessage {
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
        anal: Box<Analysis>,
    },
    Location {
        num: usize,
        site: Site,
        model: Model,
        valid_time: NaiveDateTime,
        anal: Box<Analysis>,
    },
    Completed {
        num: usize,
    },
    DataError {
        num: usize,
        site: Site,
        model: Model,
        valid_time: NaiveDateTime,
        msg: String,
    },
}
