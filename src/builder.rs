use super::{climo_db::StatsRecord, CmdLineArgs};
use crate::climo_db::{ClimoDB, ClimoDBInterface};
use bufkit_data::{Archive, BufkitDataErr, Model, Site};
use chrono::NaiveDateTime;
use crossbeam_channel::{self as channel, Receiver, Sender};
use itertools::iproduct;
use metfor::{rh, Knots, Quantity, WindSpdDir};
use pbr::ProgressBar;
use sounding_analysis::{
    convective_parcel, dcape, haines_high, haines_low, haines_mid, hot_dry_windy, lift_parcel,
    mixed_layer_parcel, partition_cape, Analysis, Parcel,
};
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
    let (ep_jh, total_num) = start_entry_point_thread(args, entry_point_snd)?;
    let load_jh = start_load_thread(root, load_requests_rcv, parse_requests_snd)?;

    let parser_jh = start_parser_thread(parse_requests_rcv, cli_requests_snd)?;

    let cli_stats_jh =
        start_cli_stats_thread(cli_requests_rcv, loc_requests_snd, stats_snd.clone())?;

    let loc_stats_jh = start_location_stats_thread(loc_requests_rcv, comp_notify_snd, stats_snd)?;

    let stats_jh = start_stats_thread(root, stats_rcv)?;

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
                ClimoDBInterface::initialize(&climo_db).map_err(|err| err.to_string())?;

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
                                let snd = anal.sounding();

                                let hns_low = i32::from(haines_low(snd).unwrap_or(0));
                                let hns_mid = i32::from(haines_mid(snd).unwrap_or(0));
                                let hns_high = i32::from(haines_high(snd).unwrap_or(0));

                                let hdw = match hot_dry_windy(snd) {
                                    Ok(hdw) => hdw as i32,
                                    Err(err) => {
                                        let message = PipelineMessage::DataError {
                                            num,
                                            site,
                                            model,
                                            valid_time,
                                            msg: err.to_string(),
                                        };
                                        local_location_requests
                                            .send(message)
                                            .expect("Error sending message.");
                                        continue;
                                    }
                                };

                                let ml_parcel: Option<Parcel> = mixed_layer_parcel(snd).ok();

                                let (mlcape, mlcin, ml_sfc_t, ml_sfc_rh) = {
                                    if let Some(parcel) = ml_parcel {
                                        let ml_sfc_t = Some(parcel.temperature.unpack() as i32);
                                        let ml_sfc_rh = rh(parcel.temperature, parcel.dew_point)
                                            .map(|rh| (rh * 100.0) as i32);

                                        if let Ok(anal) = lift_parcel(parcel, snd) {
                                            let cape = anal.cape().map(|cape| cape.unpack() as i32);
                                            let cin = anal.cin().map(|cin| cin.unpack() as i32);
                                            (cape, cin, ml_sfc_t, ml_sfc_rh)
                                        } else {
                                            (None, None, ml_sfc_t, ml_sfc_rh)
                                        }
                                    } else {
                                        (None, None, None, None)
                                    }
                                };

                                let (
                                    conv_t_def,
                                    dry_cape,
                                    wet_cape,
                                    cape_ratio,
                                    ccl_agl_m,
                                    el_asl_m,
                                ) = {
                                    if let Ok(parcel) = convective_parcel(snd) {
                                        let conv_t_def = ml_parcel.map(|ml_pcl| {
                                            (parcel.temperature - ml_pcl.temperature).unpack()
                                        });

                                        let parcel_anal = lift_parcel(parcel, snd).ok();

                                        let (dry, wet) = parcel_anal
                                            .as_ref()
                                            .and_then(|profile_anal| {
                                                partition_cape(&profile_anal)
                                                    .ok()
                                                    .map(|(dry, wet)| (Some(dry), Some(wet)))
                                            })
                                            .unwrap_or((None, None));

                                        let cape_ratio =
                                            dry.and_then(|dry| wet.map(|wet| wet / dry));
                                        let dry = dry.map(|dry| dry.unpack().round() as i32);
                                        let wet = wet.map(|wet| wet.unpack().round() as i32);

                                        let (ccl_agl_m, el_asl_m) = parcel_anal
                                            .as_ref()
                                            .and_then(|profile_anal| {
                                                let ccl = profile_anal
                                                    .lcl_height_agl()
                                                    .map(|ccl| ccl.unpack().round() as i32);
                                                let el = profile_anal
                                                    .el_height_asl()
                                                    .map(|el| el.unpack().round() as i32);

                                                Some((ccl, el))
                                            })
                                            .unwrap_or((None, None));

                                        (conv_t_def, dry, wet, cape_ratio, ccl_agl_m, el_asl_m)
                                    } else {
                                        (None, None, None, None, None, None)
                                    }
                                };

                                let dcp = dcape(snd)
                                    .ok()
                                    .map(|(_, dcp, _)| dcp.unpack().round() as i32);

                                let (sfc_wspd_kt, sfc_wdir) = snd
                                    .sfc_wind()
                                    .into_option()
                                    .map(|wind| {
                                        let WindSpdDir {
                                            speed: Knots(spd),
                                            direction: dir,
                                        } = wind;
                                        (Some(spd.round() as i32), Some(dir.round() as i32))
                                    })
                                    .unwrap_or_else(|| (None, None));

                                let message = StatsRecord::CliData {
                                    site: site.clone(),
                                    model,
                                    valid_time,
                                    hns_low,
                                    hns_mid,
                                    hns_high,
                                    hdw,
                                    conv_t_def,
                                    dry_cape,
                                    wet_cape,
                                    cape_ratio,
                                    ccl_agl_m,
                                    el_asl_m,
                                    mlcape,
                                    mlcin,
                                    dcape: dcp,
                                    ml_sfc_t,
                                    ml_sfc_rh,
                                    sfc_wspd_kt,
                                    sfc_wdir,
                                };
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
                        let snd = anal.sounding();

                        let location = snd.station_info().location();
                        let elevation = snd
                            .station_info()
                            .elevation()
                            .into_option()
                            .map(Quantity::unpack);

                        if let (Some(elev_m), Some((lat, lon))) = (elevation, location) {
                            let message = StatsRecord::Location {
                                site,
                                model,
                                valid_time,
                                lat,
                                lon,
                                elev_m,
                            };
                            climo_update_requests
                                .send(message)
                                .map_err(|err| err.to_string())?;
                        } else {
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

                    let message = PipelineMessage::Completed { num };
                    completed_notification
                        .send(message)
                        .map_err(|err| err.to_string())?;
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
                ClimoDBInterface::initialize(&climo_db).map_err(|err| err.to_string())?;

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
