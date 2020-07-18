use bufkit_data::{Model, SiteInfo};
use chrono::NaiveDateTime;
use metfor::Quantity;
use sounding_analysis::{
    dcape,
    experimental::fire::{blow_up, BlowUpAnalysis},
    hot_dry_windy, Sounding,
};

#[derive(Clone, Debug)]
pub enum StatsRecord {
    CliData {
        site: SiteInfo,
        model: Model,
        valid_time: NaiveDateTime,

        hdw: Option<i32>,
        blow_up_dt: Option<f64>,
        blow_up_meters: Option<i32>,

        dcape: Option<i32>,
    },
    Location {
        site: SiteInfo,
        model: Model,
        valid_time: NaiveDateTime,
        lat: f64,
        lon: f64,
        elev_m: f64,
    },
}

impl StatsRecord {
    pub fn create_cli_data(
        site: SiteInfo,
        model: Model,
        init_time: NaiveDateTime,
        snd: &Sounding,
    ) -> Self {
        let hdw = hot_dry_windy(snd).ok().map(|hdw| hdw as i32);
        let (blow_up_dt, blow_up_meters): (Option<f64>, Option<i32>) = match blow_up(snd, None) {
            Err(_) => (None, None),
            Ok(BlowUpAnalysis {
                delta_t_lmib,
                delta_z_lmib,
                ..
            }) => (
                Some(delta_t_lmib.unpack()),
                Some(delta_z_lmib.unpack()).map(|h| h as i32),
            ),
        };
        let dcape = dcape(snd).ok().map(|anal| anal.1.unpack() as i32);

        StatsRecord::CliData {
            site,
            model,
            valid_time: init_time,
            hdw,
            blow_up_dt,
            blow_up_meters,
            dcape,
        }
    }

    /// If I was unable create a location instance, return the site so I can use it without
    /// having to preemptively clone
    pub fn create_location_data(
        site: SiteInfo,
        model: Model,
        valid_time: NaiveDateTime,
        snd: &Sounding,
    ) -> Result<Self, SiteInfo> {
        let info = snd.station_info();

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
