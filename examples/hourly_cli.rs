use bufcli::{ClimoDB, ClimoElement, ClimoQueryInterface, HourlyDeciles};
use bufkit_data::Site;
use chrono::NaiveDate;
use itertools::izip;
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

    let HourlyDeciles {
        element,
        valid_times,
        deciles,
    } = climo_db.hourly_deciles(&site, model, element, start_time, end_time)?;

    // Output into a file
    let mut output = BufWriter::new(File::create("hourly_deciles.txt")?);
    writeln!(output, "# {}, {}, {:?}", site.id, model, element)?;
    writeln!(
        output,
        "valid_time min 10th 20th 30th 40th median 60th 70th 80th 90th max"
    )?;

    for (vt, deciles) in izip!(valid_times, deciles) {
        writeln!(
            output,
            "{} {} {} {} {} {} {} {} {} {} {} {}",
            vt.format("%Y-%m-%d-%H"),
            deciles[0],
            deciles[1],
            deciles[2],
            deciles[3],
            deciles[4],
            deciles[5],
            deciles[6],
            deciles[7],
            deciles[8],
            deciles[9],
            deciles[10],
        )?;
    }

    Ok(())
}
