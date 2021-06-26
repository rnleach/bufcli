use bufkit_data::{Model, SiteInfo};
use chrono::NaiveDateTime;
use metfor::Quantity;
use sounding_analysis::{experimental::fire::BlowUpAnalysis, Sounding};

#[derive(Clone, Debug)]
pub enum StatsRecord {
    CliData {
        site: SiteInfo,
        model: Model,
        valid_time: NaiveDateTime,

        hdw: Option<i32>,
        blow_up_dt: Option<f64>,
        pft: Option<i32>,

        dcape: Option<i32>,
    },
    Location {
        site: SiteInfo,
        model: Model,
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
        let hdw = sounding_analysis::hot_dry_windy(snd)
            .ok()
            .map(|hdw| hdw as i32);

        let bua = sounding_analysis::experimental::fire::blow_up(snd, None);

        let blow_up_dt: Option<f64> = match bua {
            Err(_) => None,
            Ok(BlowUpAnalysis { delta_t_el, .. }) => Some(delta_t_el.unpack()),
        };

        let pft: Option<i32> = sounding_analysis::pft(snd, 15.0)
            .map(|pft| pft.unpack() as i32)
            .ok();

        let dcape = sounding_analysis::dcape(snd)
            .ok()
            .map(|anal| anal.1.unpack() as i32);

        StatsRecord::CliData {
            site,
            model,
            valid_time: init_time,
            hdw,
            blow_up_dt,
            pft,
            dcape,
        }
    }

    /// If I was unable create a location instance, return the site so I can use it without
    /// having to preemptively clone
    pub fn create_location_data(
        site: SiteInfo,
        model: Model,
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
                lat,
                lon,
                elev_m,
            }),
            None => Err(site),
        }
    }
}
