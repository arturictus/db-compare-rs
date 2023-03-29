use chrono::NaiveDateTime;

use crate::database::{self, RequestBuilder};
use crate::diff::IO;
use crate::{Config, DBResultTypes};

use super::par_run;

pub fn run(config: &Config) -> Result<(), postgres::Error> {
    let query = RequestBuilder::new(config).column(column());
    let db1_tables = database::tables_with_column(query.build_master())?.to_s();
    for table in db1_tables {
        compare_table(config, &table)?;
    }
    Ok(())
}

fn column() -> String {
    "updated_at".to_string()
}

fn compare_table(config: &Config, table: &str) -> Result<(), postgres::Error> {
    let builder = RequestBuilder::new(config)
        .table(table)
        .column(column())
        .until(config.rows_until);
    let mut last_date_time: Option<NaiveDateTime> = Some(config.rows_until);
    while last_date_time.is_some() {
        let builder = builder.clone().until(last_date_time.unwrap());
        let (records1, records2) = par_run(builder, database::full_row_ordered_by_until)?;

        let mut diff_io = config.diff_io.borrow_mut();
        let header = format!(
            "====== `{table}` compare rows where `{}` is < '{:?}' ======",
            column(),
            last_date_time.unwrap()
        );
        last_date_time = get_last_date_time(&records1, last_date_time);
        if !records1.is_empty() && !records2.is_empty() {
            diff_io.write((header, records1, records2));
        }
    }
    Ok(())
}

fn get_last_date_time(
    records: &DBResultTypes,
    prev: Option<NaiveDateTime>,
) -> Option<NaiveDateTime> {
    let records = records.to_h();
    let mut last_date_time = None;
    if let Some(value) = records.last() {
        let date = value[&column()].as_str().unwrap();
        if let Ok(date) = NaiveDateTime::parse_from_str(date, "%Y-%m-%dT%H:%M:%S") {
            if let Some(prev_date) = prev {
                if date == prev_date {
                    return NaiveDateTime::from_timestamp_opt(prev_date.timestamp() - 1, 0);
                }
            }
            last_date_time = Some(date);
        }
    }
    last_date_time
}
