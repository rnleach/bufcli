use bufkit_data::{Model, Site};
use chrono::NaiveDateTime;
use metfor::Quantity;
use sounding_analysis::{
    convective_parcel, convective_parcel_initiation_energetics, hot_dry_windy, lift_parcel,
    partition_cape, Analysis,
};

#[derive(Clone, Debug)]
pub enum StatsRecord {
    CliData {
        site: Site,
        model: Model,
        valid_time: NaiveDateTime,

        hdw: Option<i32>,
        conv_t_def: Option<f64>,
        dry_cape: Option<i32>,
        wet_cape: Option<i32>,
        cape_ratio: Option<f64>,

        e0: Option<i32>,
        de: Option<i32>,
    },
    Location {
        site: Site,
        model: Model,
        valid_time: NaiveDateTime,
        lat: f64,
        lon: f64,
        elev_m: f64,
    },
}

impl StatsRecord {
    pub fn create_cli_data(
        site: Site,
        model: Model,
        init_time: NaiveDateTime,
        anal: &Analysis,
    ) -> Self {
        let snd = anal.sounding();

        let hdw = hot_dry_windy(snd).ok().map(|hdw| hdw as i32);

        let (conv_t_def, e0, de) = match convective_parcel_initiation_energetics(snd).ok() {
            Some((_, conv_t_def, e0, de)) => (
                Some(conv_t_def.unpack()),
                Some(e0.unpack() as i32),
                Some(de.unpack() as i32),
            ),
            None => (None, None, None),
        };

        let (dry_cape, wet_cape, cape_ratio) = convective_parcel(snd)
            .and_then(|pcl| lift_parcel(pcl, snd))
            .and_then(|pcl_anal| partition_cape(&pcl_anal))
            .map(|(dry, wet)| {
                (
                    Some(dry.unpack() as i32),
                    Some(wet.unpack() as i32),
                    Some(wet / dry),
                )
            })
            .unwrap_or((None, None, None));

        StatsRecord::CliData {
            site,
            model,
            valid_time: init_time,
            hdw,
            conv_t_def,
            dry_cape,
            wet_cape,
            cape_ratio,
            e0,
            de,
        }
    }

    /// If I was unable create a location instance, return the site so I can use it without
    /// having to preemptively clones
    pub fn create_location_data(
        site: Site,
        model: Model,
        valid_time: NaiveDateTime,
        anal: &Analysis,
    ) -> Result<Self, Site> {
        let info = anal.sounding().station_info();

        let location_data = info.location().and_then(|(lat, lon)| {
            info.elevation()
                .into_option()
                .map(|elev_m| (lat, lon, elev_m.unpack()))
        });

        match location_data {
            Some((lat, lon, elev_m)) => Ok(StatsRecord::Location {
                site,
                model,
                valid_time,
                lat,
                lon,
                elev_m,
            }),
            None => Err(site),
        }
    }
}
