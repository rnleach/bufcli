use crate::CmdLineArgs;
use bufcli::{ClimoDB, ClimoPopulateInterface, StatsRecord};
use bufkit_data::{Archive, Model, SiteInfo};
use chrono::NaiveDateTime;
use crossbeam_channel::{self as channel, Receiver, Sender};
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

// Capacity of bounded channels used in data module.
const CAPACITY: usize = 256;

pub(crate) fn build_climo(args: CmdLineArgs) -> Result<(), Box<dyn Error>> {
    use DataPopulateMsg::*;

    let root = args.root.clone();

    // Channels for the main pipeline
    let (entry_point_snd, load_requests_rcv) = channel::bounded::<DataPopulateMsg>(CAPACITY);
    let (parse_requests_snd, parse_requests_rcv) = channel::bounded::<DataPopulateMsg>(CAPACITY);
    let (cli_requests_snd, cli_requests_rcv) = channel::bounded::<DataPopulateMsg>(CAPACITY);
    let (loc_requests_snd, loc_requests_rcv) = channel::bounded::<DataPopulateMsg>(CAPACITY);
    let (comp_notify_snd, comp_notify_rcv) = channel::bounded::<DataPopulateMsg>(CAPACITY);

    // Channel for adding stats to the climo database
    let (stats_snd, stats_rcv) = channel::bounded::<StatsRecord>(CAPACITY);

    // Hook everything together
    let stats_jh = start_stats_thread(&root, stats_rcv, comp_notify_snd.clone())?;
    let total_num = start_entry_point_thread(args, entry_point_snd)?;
    start_load_thread(&root, load_requests_rcv, parse_requests_snd)?;
    start_parser_thread(parse_requests_rcv, cli_requests_snd)?;
    start_cli_stats_thread(cli_requests_rcv, loc_requests_snd, stats_snd.clone())?;
    start_location_stats_thread(loc_requests_rcv, comp_notify_snd, stats_snd)?;

    // Monitor progress and post updates here
    let mut pb = ProgressBar::new(total_num as u64);
    let arch = Archive::connect(&root)?;
    let mut num_terminates = 0;
    for msg in comp_notify_rcv {
        match msg {
            PopulateCompleted { num } => {
                pb.set(num as u64);
            }
            TerminateThread => {
                num_terminates += 1;

                if num_terminates >= 2 {
                    // Signal that the stats thread and location stats thread are done, 
                    // so everything else must also be done.
                    pb.finish();
                }
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
                    site.station_num, model, valid_time
                );
                println!("  {}", msg);
                pb.set(num as u64);
                if arch.file_exists(site.station_num, model, valid_time)? {
                    arch.remove(site.station_num, model, valid_time)?;
                }
            }
            _ => {
                print!("\u{001b}[300D\u{001b}[K");
                println!("Invalid message recieved in main thread: {:?}", msg);
            }
        }
    }

    // Let stats drop implementation release the database and commit all changes.
    stats_jh.join().unwrap();

    Ok(())
}

macro_rules! assign_or_bail {
    ($res:expr, $channel:ident) => {
        match $res {
            Ok(val) => val,
            Err(err) => {
                $channel
                    .send(DataPopulateMsg::ThreadError(err.to_string()))
                    .unwrap_or_else(|err| {
                        eprintln!("Broken channel, returning from thread with error: {}", err)
                    });
                return;
            }
        }
    };
    ($res:expr, $channel:ident, $msg:expr) => {
        match $res {
            Ok(val) => val,
            Err(err) => {
                $channel
                    .send(DataPopulateMsg::ThreadError(err.to_string() + $msg))
                    .unwrap_or_else(|err| {
                        eprintln!("Broken channel, returning from thread with error: {}", err)
                    });
                return;
            }
        }
    };
}

macro_rules! send_or_bail {
    ($msg:ident, $channel:ident) => {
        match $channel.send($msg) {
            Ok(()) => {}
            Err(err) => {
                eprintln!("Broken channel with error: {}", err);
                return;
            }
        }
    };
}

fn start_entry_point_thread(
    args: CmdLineArgs,
    entry_point_snd: Sender<DataPopulateMsg>,
) -> Result<u64, Box<dyn Error>> {
    let arch = Archive::connect(&args.root)?;

    let mut total = 0;
    for (site_info, model) in args.site_model_pairs.iter() {
        total += arch.count(site_info.station_num, *model)? as u64;
    }

    thread::Builder::new()
        .name("Generator".to_string())
        .spawn(move || {
            let force_rebuild = args.operation == "build";

            let arch = assign_or_bail!(
                Archive::connect(&args.root),
                entry_point_snd,
                " error connecting to archive"
            );
            let climo_db = assign_or_bail!(
                ClimoDB::connect_or_create(&args.root),
                entry_point_snd,
                " error connecting to climo db"
            );
            let mut climo_db = assign_or_bail!(
                ClimoPopulateInterface::initialize(&climo_db),
                entry_point_snd,
                " error connecting to ClimoPopulateInterface"
            );

            let mut counter = 0;
            for (site, model) in args.site_model_pairs.into_iter() {
                let init_times = assign_or_bail!(
                    arch.inventory(site.station_num, model),
                    entry_point_snd,
                    " error retrieving init_times"
                );
                let init_times: HashSet<NaiveDateTime> = HashSet::from_iter(init_times);

                let done_times = if !force_rebuild {
                    let iter = assign_or_bail!(
                        climo_db.valid_times_for(&site, model),
                        entry_point_snd,
                        " error retriving done_times"
                    );
                    HashSet::from_iter(iter)
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

                    send_or_bail!(message, entry_point_snd);
                }

                counter += init_times.len() - small_counter;
            }
        })?;

    Ok(total)
}

fn start_load_thread(
    root: &Path,
    load_requests_rcv: Receiver<DataPopulateMsg>,
    parse_requests_snd: Sender<DataPopulateMsg>,
) -> Result<(), Box<dyn Error>> {
    let root = root.to_path_buf();

    thread::Builder::new()
        .name("FileLoader".to_string())
        .spawn(move || {
            let arch = assign_or_bail!(
                Archive::connect(&root),
                parse_requests_snd,
                " error connecting in FileLoader"
            );

            for load_req in load_requests_rcv {
                let message = match load_req {
                    DataPopulateMsg::Load {
                        num,
                        site,
                        model,
                        init_time,
                    } => match arch.retrieve(site.station_num, model, init_time) {
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

                send_or_bail!(message, parse_requests_snd);
            }
        })?;

    Ok(())
}

fn start_parser_thread(
    parse_requests: Receiver<DataPopulateMsg>,
    cli_requests: Sender<DataPopulateMsg>,
) -> Result<(), Box<dyn Error>> {
    thread::Builder::new()
        .name("SoundingParser".to_string())
        .spawn(move || {
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
                            send_or_bail!(message, cli_requests);
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
                            send_or_bail!(message, cli_requests);
                        } else {
                            let message = DataPopulateMsg::DataError {
                                num,
                                site: site.clone(),
                                model,
                                valid_time: init_time,
                                msg: "No valid time".to_string(),
                            };

                            send_or_bail!(message, cli_requests);
                        }
                    }
                } else {
                    send_or_bail!(msg, cli_requests);
                }
            }
        })?;

    Ok(())
}

fn start_cli_stats_thread(
    cli_requests: Receiver<DataPopulateMsg>,
    location_requests: Sender<DataPopulateMsg>,
    climo_update_requests: Sender<StatsRecord>,
) -> Result<(), Box<dyn Error>> {
    thread::Builder::new()
        .name("CliStatsBuilder".to_string())
        .spawn(move || {
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
                                send_or_bail!(message, local_update_requests);
                            }

                            let message = DataPopulateMsg::Location {
                                num,
                                site,
                                model,
                                valid_time,
                                snd,
                            };
                            send_or_bail!(message, local_location_requests);
                        } else {
                            send_or_bail!(msg, local_location_requests);
                        }
                    }
                });
            }

            pool.join();
        })?;

    Ok(())
}

fn start_location_stats_thread(
    location_requests: Receiver<DataPopulateMsg>,
    completed_notification: Sender<DataPopulateMsg>,
    climo_update_requests: Sender<StatsRecord>,
) -> Result<(), Box<dyn Error>> {
    thread::Builder::new()
        .name("LocationUpdater".to_string())
        .spawn(move || {
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
                        match StatsRecord::create_location_data(site.clone(), model, &snd) {
                            Ok(msg) => {
                                send_or_bail!(msg, climo_update_requests);

                                let message = DataPopulateMsg::PopulateCompleted { num };
                                send_or_bail!(message, completed_notification);
                            }
                            Err(site) => {
                                let message = DataPopulateMsg::DataError {
                                    num,
                                    site,
                                    model,
                                    valid_time,
                                    msg: "Missing location information".to_string(),
                                };
                                send_or_bail!(message, completed_notification);
                            }
                        }
                    }
                } else {
                    send_or_bail!(msg, completed_notification);
                }
            }

            completed_notification
                .send(DataPopulateMsg::TerminateThread)
                .expect("Error sending terminate thread.");
        })?;

    Ok(())
}

fn start_stats_thread(
    root: &Path,
    stats_rcv: Receiver<StatsRecord>,
    comp_notify_snd: Sender<DataPopulateMsg>,
) -> Result<JoinHandle<()>, Box<dyn Error + 'static>> {
    let root = root.to_path_buf();

    let jh = thread::Builder::new()
        .name("ClimoWriter".to_string())
        .spawn(move || {
            let climo_db = assign_or_bail!(ClimoDB::connect_or_create(&root), comp_notify_snd);
            let mut climo_db = assign_or_bail!(
                ClimoPopulateInterface::initialize(&climo_db),
                comp_notify_snd
            );

            for msg in stats_rcv {
                assign_or_bail!(climo_db.add(msg), comp_notify_snd);
            }

            comp_notify_snd
                .send(DataPopulateMsg::TerminateThread)
                .expect("Error sending terminate thread.");
        })?;

    Ok(jh)
}

#[derive(Debug)]
enum DataPopulateMsg {
    Load {
        num: usize,
        site: SiteInfo,
        model: Model,
        init_time: NaiveDateTime,
    },
    Parse {
        num: usize,
        site: SiteInfo,
        model: Model,
        init_time: NaiveDateTime,
        data: String,
    },
    CliData {
        num: usize,
        site: SiteInfo,
        model: Model,
        valid_time: NaiveDateTime,
        snd: Box<Sounding>,
    },
    Location {
        num: usize,
        site: SiteInfo,
        model: Model,
        valid_time: NaiveDateTime,
        snd: Box<Sounding>,
    },
    PopulateCompleted {
        num: usize,
    },
    DataError {
        num: usize,
        site: SiteInfo,
        model: Model,
        valid_time: NaiveDateTime,
        msg: String,
    },
    ThreadError(String),
    TerminateThread,
}
