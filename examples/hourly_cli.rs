use bufcli::{ClimoDB, ClimoElement, ClimoQueryInterface, CumulativeDistribution, Percentile};
use bufkit_data::Site;
use chrono::{NaiveDate, NaiveDateTime};
use std::{
    error::Error,
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

const ARCHIVE_ROOT: &str = "/home/ryan/bufkit/";

fn main() -> Result<(), Box<dyn Error>> {
    let pth = Path::new(ARCHIVE_ROOT);
    let climo_db = ClimoDB::connect_or_create(pth)?;
    let mut climo_db = ClimoQueryInterface::initialize(&climo_db);

    let site = Site {
        id: "KMSO".to_owned(),
        name: None,
        notes: None,
        state: None,
        auto_download: false,
        time_zone: None,
    };
    let model = "nam4km";
    let element = ClimoElement::HDW;
    let start_time = NaiveDate::from_ymd(2017, 8, 28).and_hms(12, 0, 0);
    let end_time = NaiveDate::from_ymd(2017, 9, 5).and_hms(12, 0, 0);

    let cdfs: Vec<(NaiveDateTime, CumulativeDistribution)> =
        climo_db.hourly_cdfs(&site, model, element, start_time, end_time)?;

    // Output into a file
    let mut output = BufWriter::new(File::create("hourly_deciles.txt")?);
    writeln!(output, "# {}, {}, {:?}", site.id, model, element)?;
    writeln!(
        output,
        "valid_time min 10th 20th 30th 40th median 60th 70th 80th 90th max"
    )?;

    for (vt, cdf) in cdfs {
        writeln!(
            output,
            "{} {} {} {} {} {} {} {} {} {} {} {}",
            vt.format("%Y-%m-%d-%H"),
            cdf.value_at_percentile(Percentile::from(0)),
            cdf.value_at_percentile(Percentile::from(10)),
            cdf.value_at_percentile(Percentile::from(20)),
            cdf.value_at_percentile(Percentile::from(30)),
            cdf.value_at_percentile(Percentile::from(40)),
            cdf.value_at_percentile(Percentile::from(50)),
            cdf.value_at_percentile(Percentile::from(60)),
            cdf.value_at_percentile(Percentile::from(70)),
            cdf.value_at_percentile(Percentile::from(80)),
            cdf.value_at_percentile(Percentile::from(90)),
            cdf.value_at_percentile(Percentile::from(100)),
        )?;
    }

    Ok(())
}
