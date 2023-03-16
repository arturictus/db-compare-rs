use chrono::offset::Utc;
use chrono::NaiveDateTime;

use crate::database::{self, RequestBuilder};
use crate::diff::IO;
use crate::Config;

use super::{last_created_records, par_run};

pub fn run(config: &Config) -> Result<(), postgres::Error> {
    let query = RequestBuilder::new(config).column(column());
    let db1_tables = database::tables_with_column(query.build_master())?;
    for table in db1_tables {
        compare_table(config, &table)?;
    }
    Ok(())
}

fn column() -> String {
    "updated_at".to_string()
}

// fn compare_rows(config: &Config, table: &str) -> Result<(), postgres::Error> {
//     let builder = RequestBuilder::new(config)
//         .table(table)
//         .column(column())
//         // date +%s
//         .until(
//             config
//                 .rows_until
//                 .ok_or("`until` required to run UpdatedAtsUntil job")
//                 .unwrap(),
//         );
//     println!("{:#?}", config);
//     let (records1, records2) = par_run(builder, database::full_row_ordered_by_until)?;
//     let mut diff_io = config.diff_io.borrow_mut();
//     diff_io.write((format!("====== `{table}` all columns"), records1, records2));
//     Ok(())
// }

fn compare_table(config: &Config, table: &str) -> Result<(), postgres::Error> {
    let mut last_created_at: Option<NaiveDateTime> = if let Some(until) = &config.rows_until {
        Some(NaiveDateTime::from_timestamp_opt(until.to_owned(), 0).unwrap())
    } else {
        Some(NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap())
    };
    let builder = RequestBuilder::new(config)
        .table(table)
        .column(column())
        // date +%s
        .until(last_created_at.unwrap().timestamp());
    while let Some(_) = last_created_at {
        let builder = builder.clone().until(last_created_at.unwrap().timestamp());
        let (records1, records2) = par_run(builder, database::full_row_ordered_by_until)?;

        // dbg!(&records1);
        let mut diff_io = config.diff_io.borrow_mut();
        diff_io.write((
            format!(
                "====== `{table}` compare rows where updated_at is <= {:?}",
                last_created_at
            ),
            records1.clone(),
            records2,
        ));
        last_created_at = get_last_created_at(&records1, last_created_at);
    }
    Ok(())
}

fn get_last_created_at(
    records: &Vec<String>,
    prev: Option<NaiveDateTime>,
) -> Option<NaiveDateTime> {
    // TODO: opimize using only the last record from the previous query
    let mut last_created_at = None;
    if let Some(last) = records.last() {
        let value: serde_json::Value = serde_json::from_str(&last).unwrap();
        let date = value[&column()].as_str().unwrap();
        let date = NaiveDateTime::parse_from_str(date, "%Y-%m-%dT%H:%M:%S").unwrap();
        if let Some(prev_date) = prev {
            if date == prev_date {
                return None;
                // return NaiveDateTime::from_timestamp_opt(prev_date.timestamp() - 1, 0);
            }
        }
        last_created_at = Some(date);
    }
    last_created_at
}
