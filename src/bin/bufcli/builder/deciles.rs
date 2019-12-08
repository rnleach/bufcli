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
    thread::{self, JoinHandle},
};
use threadpool;

pub(super) fn build(
    site_model_pairs: HashSet<(Site, Model)>,
    archive_root: &Path,
) -> Result<(), Box<dyn Error>> {
    println!("Updating distributions.");
    let total_num = site_model_pairs.len();
    let (to_main, from_cdf_builder) = channel::bounded::<DecilesBuilderMsg>(CAPACITY);
    let (jh_calc, jh_db_add) = start_deciles_builder(site_model_pairs, to_main, archive_root)?;

    let mut pb = ProgressBar::new(total_num as u64);
    for msg in from_cdf_builder {
        match msg {
            DecilesBuilderMsg::CDFStatsCompleted => {}
            other => println!("{:?}", other),
        }

        pb.inc();
    }
    println!("Done updating distributions.");

    println!("Waiting for CDF service threads to shut down.");
    jh_calc
        .join()
        .expect("Error joining CDF calculation threads.")?;
    jh_db_add
        .join()
        .expect("Error joining CDF db add thread.")?;

    Ok(())
}

// This is a pair of join handles for two threads started to build CDFs. The first one is the
// thread that loads the vecs and dispatches their calculations to a thread pool, which that
// thread manages so we don't need to worry about here. That thread pool dispatches requests to add
// Deciles to the climo_stats database, which is the second thread that is returned here. The
// caller of start_deciles_builder needs to wait for both of these threads to finish.
type DecilesBuilderHandles = (
    JoinHandle<Result<(), String>>,
    JoinHandle<Result<(), String>>,
);

fn start_deciles_builder(
    set: HashSet<(Site, Model)>,
    to_main: Sender<DecilesBuilderMsg>,
    root: &Path,
) -> Result<DecilesBuilderHandles, Box<dyn Error>> {
    let (data_snd, cdf_builder_receiver) = channel::bounded::<DecilesBuilderMsg>(CAPACITY);
    let (db_add_send, db_add_receiver) = channel::bounded::<DecilesBuilderMsg>(CAPACITY);

    let local_root = root.to_path_buf();
    let jh_producer =
        thread::Builder::new()
            .name("CDFMaster".into())
            .spawn(move || -> Result<(), String> {
                const POOL_SIZE: usize = 12;

                // Make a pool of threads that take a vec and build a CDF distribution out of them
                let pool = threadpool::Builder::new()
                    .num_threads(POOL_SIZE)
                    .thread_name("CDFBuilder".to_string())
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
                let climo_db =
                    ClimoDB::connect_or_create(&local_root).map_err(|err| err.to_string())?;
                let mut climo_db = ClimoCDFBuilderInterface::initialize(&climo_db)
                    .map_err(|err| err.to_string())?;
                for (site, model) in set.into_iter() {
                    let data = climo_db
                        .load_all_data(&site, model)
                        .map_err(|err| err.to_string())?;

                    let msg = DecilesBuilderMsg::HourlyCDFData { site, model, data };

                    data_snd.send(msg).map_err(|err| err.to_string())?;
                }
                drop(data_snd);

                pool.join();

                Ok(())
            })?;

    // Make a thread the receives completed CDFS and inserts them in the db
    let local_root = root.to_path_buf();
    let jh_cdf_db_adder =
        thread::Builder::new()
            .name("CDFDbAdder".into())
            .spawn(move || -> Result<(), String> {
                let climo_db =
                    ClimoDB::connect_or_create(&local_root).map_err(|err| err.to_string())?;
                let mut climo_db = ClimoCDFBuilderInterface::initialize(&climo_db)
                    .map_err(|err| err.to_string())?;

                climo_db
                    .begin_transaction()
                    .map_err(|err| err.to_string())?;

                for msg in db_add_receiver {
                    if let DecilesBuilderMsg::CDFDBAdd { site, model, cdfs } = msg {
                        for (day_of_year, hour, hdw, dt, meters, dcape) in cdfs.into_iter() {
                            climo_db
                                .add_to_db(
                                    &site,
                                    model,
                                    day_of_year,
                                    hour,
                                    (hdw, dt, meters, dcape),
                                )
                                .map_err(|err| err.to_string())?;
                        }
                        let msg = DecilesBuilderMsg::CDFStatsCompleted;
                        to_main.send(msg).map_err(|err| err.to_string())?;
                    } else {
                        // Just forward the message and we'll deal with it later.
                        to_main.send(msg).map_err(|err| err.to_string())?;
                    }
                }

                climo_db
                    .commit_transaction()
                    .map_err(|err| err.to_string())?;

                Ok(())
            })?;

    Ok((jh_producer, jh_cdf_db_adder))
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
    CDFStatsCompleted,
}
