use chrono::NaiveDateTime;

use crate::database::{self, RequestBuilder};
use crate::diff::IO;
use crate::{Config, DBResultTypes, JsonMap};
use std::collections::BTreeMap;

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

        println!("records1: {:?}", records1.to_h());
        println!("records2: {:?}", records2.to_h());

        let RowSelector {
            matches: (result_a, result_b),
            missing,
        } = only_matching_ids(&records1, &records2);

        let mut diff_io = config.diff_io.borrow_mut();
        let header = format!(
            "====== `{table}` compare rows where `{}` is < '{:?}' ======",
            column(),
            last_date_time.unwrap()
        );
        last_date_time = get_last_date_time(&records1, last_date_time);
        println!("last_date_time: {:?}", last_date_time);
        // only_matching_ids(&records1, &records2);
        diff_io.write((header, result_a, result_b));

        diff_io.write((
            format!("|== `{table}` missing rows"),
            missing,
            DBResultTypes::Empty,
        ));
    }
    Ok(())
}
#[derive(Debug)]
struct RowSelector {
    matches: (DBResultTypes, DBResultTypes),
    missing: DBResultTypes,
}
fn only_matching_ids(a: &DBResultTypes, b: &DBResultTypes) -> RowSelector {
    println!("a: {:?}", a.to_h());
    println!("b: {:?}", b.to_h());
    let btree: BTreeMap<u64, JsonMap> =
        b.to_h().into_iter().fold(BTreeMap::new(), |mut acc, data| {
            acc.insert(data.get("id").unwrap().as_u64().unwrap(), data);
            acc
        });
    let mut missing: Vec<JsonMap> = Vec::new();
    let acc: Vec<(JsonMap, JsonMap)> = a.to_h().into_iter().fold(Vec::new(), |mut acc, data| {
        let id = data.get("id").unwrap().as_u64().unwrap();
        if let Some(value) = btree.get(&id) {
            acc.push((data, value.clone()));
        } else {
            missing.push(data);
        }
        acc
    });
    let mut a_result: Vec<JsonMap> = vec![];
    let mut b_result: Vec<JsonMap> = vec![];
    for e in acc {
        a_result.push(e.0);
        b_result.push(e.1);
    }
    let r = RowSelector {
        matches: (DBResultTypes::Map(a_result), DBResultTypes::Map(b_result)),
        missing: DBResultTypes::Map(missing),
    };
    println!("r: {:?}", &r);
    r
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

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::{from_str, Map, Value};

    #[test]
    fn test_only_matching_ids() {
        let a = gen_data(1);
        let b = gen_data(2);
        let c = gen_data(3);
        let d = gen_data(4);

        let RowSelector {
            matches: (result_a, result_b),
            missing,
        } = only_matching_ids(
            &DBResultTypes::Map(vec![a.clone(), b.clone(), c.clone()]),
            &DBResultTypes::Map(vec![a.clone(), b.clone(), d.clone()]),
        );
        assert_eq!(result_a.to_h(), vec![a.clone(), b.clone()]);
        assert_eq!(result_b.to_h(), vec![a.clone(), b.clone()]);
        assert_eq!(missing.to_h(), vec![c.clone()]);
    }

    fn gen_data(id: u64) -> Map<String, Value> {
        let data = format!(r#"{{"id": {id},"name": "John_{id}"}}"#);
        let mut v: Value = from_str(&data).unwrap();
        let val = v.as_object_mut().unwrap();
        let mut m = Map::new();
        m.append(val);
        m
    }
}
