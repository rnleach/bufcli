use super::CAPACITY;
use bufcli::{ClimoCDFBuilderInterface, ClimoDB, Deciles};
use bufkit_data::{Model, Site};
use chrono::NaiveDateTime;
use crossbeam_channel::{self as channel, Sender};
use pbr::ProgressBar;
use std::{
    collections::HashSet,
    error::Error,
    path::Path,
    thread::{self},
};
use threadpool;

pub(super) fn build(
    site_model_pairs: HashSet<(Site, Model)>,
    archive_root: &Path,
) -> Result<(), Box<dyn Error>> {
    println!("Updating distributions.");
    let total_num = site_model_pairs.len();
    let (to_main, from_cdf_builder) = channel::bounded::<DecilesBuilderMsg>(CAPACITY);
    start_deciles_builder(site_model_pairs, to_main, archive_root)?;

    let mut pb = ProgressBar::new(total_num as u64);
    for msg in from_cdf_builder {
        match msg {
            DecilesBuilderMsg::CDFStatsCompleted => {}
            other => println!("{:?}", other),
        }

        pb.inc();
    }
    pb.finish();
    println!("Done updating distributions.");

    Ok(())
}

macro_rules! assign_or_bail {
    ($res:expr, $channel:ident) => {
        match $res {
            Ok(val) => val,
            Err(err) => {
                $channel
                    .send(DecilesBuilderMsg::ThreadError(err.to_string()))
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

fn start_deciles_builder(
    set: HashSet<(Site, Model)>,
    to_main: Sender<DecilesBuilderMsg>,
    root: &Path,
) -> Result<(), Box<dyn Error>> {
    let (data_snd, cdf_builder_receiver) = channel::bounded::<DecilesBuilderMsg>(CAPACITY);
    let (db_add_send, db_add_receiver) = channel::bounded::<DecilesBuilderMsg>(CAPACITY);

    let local_root = root.to_path_buf();
    let local_to_main = to_main.clone();
    thread::Builder::new()
        .name("CDFMaster".into())
        .spawn(move || -> () {
            const POOL_SIZE: usize = 12;

            let to_main = local_to_main;

            // Make a pool of threads that take a vec and build a CDF distribution out of them
            let pool = threadpool::Builder::new()
                .num_threads(POOL_SIZE)
                .thread_name("DecilesBuilder".to_string())
                .build();

            for _ in 0..POOL_SIZE {
                let local_cdf_builder_receiver = cdf_builder_receiver.clone();
                let local_db_add_send = db_add_send.clone();

                pool.execute(move || {
                    for msg in local_cdf_builder_receiver {
                        if let DecilesBuilderMsg::HourlyCDFData { site, model, data } = msg {
                            let cdfs = ClimoCDFBuilderInterface::create_deciles(&data);
                            let msg = DecilesBuilderMsg::CDFDBAdd { site, model, cdfs };

                            local_db_add_send.send(msg).unwrap();
                        } else {
                            // Just forward the message and we'll deal with it later.
                            local_db_add_send.send(msg).unwrap();
                        }
                    }
                });
            }
            drop(cdf_builder_receiver);
            drop(db_add_send);

            // Load the vec of data for each site/model pair, and send it to the pool
            let climo_db = assign_or_bail!(ClimoDB::connect_or_create(&local_root), to_main);
            let mut climo_db =
                assign_or_bail!(ClimoCDFBuilderInterface::initialize(&climo_db), to_main);
            for (site, model) in set.into_iter() {
                let data = assign_or_bail!(climo_db.load_all_data(&site, model), to_main);
                let msg = DecilesBuilderMsg::HourlyCDFData { site, model, data };
                send_or_bail!(msg, data_snd);
            }
            drop(data_snd);

            pool.join();
        })?;

    // Make a thread the receives completed CDFS and inserts them in the db
    let local_root = root.to_path_buf();
    thread::Builder::new()
        .name("CDFDbAdder".into())
        .spawn(move || -> () {
            let climo_db = assign_or_bail!(ClimoDB::connect_or_create(&local_root), to_main);
            let mut climo_db =
                assign_or_bail!(ClimoCDFBuilderInterface::initialize(&climo_db), to_main);

            for msg in db_add_receiver {
                if let DecilesBuilderMsg::CDFDBAdd { site, model, cdfs } = msg {
                    for (day_of_year, hour, hdw, dt, meters, dcape) in cdfs.into_iter() {
                        let _ = assign_or_bail!(
                            climo_db.add_to_db(
                                &site,
                                model,
                                day_of_year,
                                hour,
                                (hdw, dt, meters, dcape)
                            ),
                            to_main
                        );
                    }
                    let msg = DecilesBuilderMsg::CDFStatsCompleted;
                    send_or_bail!(msg, to_main);
                } else {
                    // Just forward the message and we'll deal with it later.
                    send_or_bail!(msg, to_main);
                }
            }
        })?;

    Ok(())
}

#[derive(Debug)]
enum DecilesBuilderMsg {
    HourlyCDFData {
        site: Site,
        model: Model,
        data: Vec<(NaiveDateTime, f64, f64, f64, f64)>,
    },
    CDFDBAdd {
        site: Site,
        model: Model,
        cdfs: Vec<(
            u32, // day of year
            u32, // hour
            Deciles,
            Deciles,
            Deciles,
            Deciles,
        )>,
    },
    ThreadError(String),
    CDFStatsCompleted,
}
