use postgres::Error as PgError;
mod repo;
mod request;
use chrono::prelude::*;
pub use repo::{ping_db, updated_ids_after_cutoff, RepoResult};

pub use request::{Request, RequestBuilder};
use std::time::Instant;

pub type JsonMap = serde_json::Map<String, serde_json::Value>;
#[derive(Debug, Clone)]
pub enum DBResultType {
    Strings(Vec<String>),
    JsonMaps(Vec<JsonMap>),
    GroupedRows(Vec<(JsonMap, JsonMap)>),
    Ids(Vec<u32>),
    Empty,
}

impl DBResultType {
    pub fn to_s(&self) -> Vec<String> {
        match self {
            Self::Strings(v) => v.clone(),
            Self::Empty => vec![],
            Self::GroupedRows(_) => panic!("not a string: {:?}", self),
            _ => panic!("not a string: {:?}", self),
        }
    }
    pub fn to_h(&self) -> Vec<JsonMap> {
        match self {
            Self::JsonMaps(v) => v.clone(),
            Self::Empty => vec![],
            Self::GroupedRows(_) => panic!("not a hash: {:?}", self),
            _ => panic!("not a Map: {:?}", self),
        }
    }
    pub fn to_gr(&self) -> Vec<(JsonMap, JsonMap)> {
        match self {
            Self::GroupedRows(v) => v.clone(),
            _ => panic!("not a Map: {:?}", self),
        }
    }
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Empty => true,
            Self::JsonMaps(e) => e.is_empty(),
            Self::Strings(e) => e.is_empty(),
            Self::GroupedRows(e) => e.is_empty(),
            Self::Ids(e) => e.is_empty(),
        }
    }

    // TODO: Work in progress
    // pub fn unwrap<T>(self) -> Vec<T> {
    //     match self {
    //         Self::JsonMaps(e) => e,
    //         Self::Strings(e) => e,
    //         Self::GroupedRows(e) => e,
    //         Self::Ids(e) => e,
    //         Self::Empty => panic!("Empty"),
    //     }
    // }

    pub fn exclude_ids(self, ids: &[u32]) -> Self {
        match self {
            Self::JsonMaps(e) => {
                let new_data = e
                    .into_iter()
                    .filter(|e| !ids.contains(&e["id"].as_u64().unwrap().try_into().unwrap()))
                    .collect();
                Self::JsonMaps(new_data)
            }
            Self::GroupedRows(e) => {
                let new_data = e
                    .into_iter()
                    .filter(|(a, _b)| !ids.contains(&a["id"].as_u64().unwrap().try_into().unwrap()))
                    .collect();
                Self::GroupedRows(new_data)
            }
            _ => panic!("not a Map: {:?}", self),
        }
    }

    // TODO: Work in progress
    // pub fn into_iter(&self) -> impl Iterator<Item = impl Sized + '_> {
    //     match self {
    //         Self::JsonMaps(e) => e.iter(),
    //         Self::GroupedRows(e) => e.iter(),
    //         _ => panic!("not a Map: {:?}", self),
    //     }
    // }

    pub fn m_into_iter(&self) -> impl Iterator<Item = &JsonMap> {
        match self {
            Self::JsonMaps(e) => e.iter(),
            _ => panic!("not a Map: {:?}", self),
        }
    }
    pub fn gr_into_iter(&self) -> impl Iterator<Item = &(JsonMap, JsonMap)> {
        match self {
            Self::GroupedRows(e) => e.iter(),
            _ => panic!("not a Map: {:?}", self),
        }
    }
    pub fn s_into_iter(&self) -> impl Iterator<Item = &String> {
        match self {
            Self::Strings(e) => e.iter(),
            _ => panic!("not a String: {:?}", self),
        }
    }
}
pub type DBsResults = (String, DBResultType, DBResultType);

pub fn get_sequences(r: Request) -> Result<Vec<(std::string::String, u32)>, PgError> {
    duration::<Vec<(String, u32)>>(
        format!("Getting sequences from {}", r.db.name()),
        r,
        repo::get_sequences,
    )
}
pub fn get_greatest_id_from(r: Request) -> Result<u32, PgError> {
    let table = r.table.as_ref().unwrap();
    duration::<u32>(
        format!("Greatest id from `{table}` in {}", r.db.name()),
        r,
        repo::get_greatest_id_from,
    )
}

pub fn get_row_by_id_range(r: Request) -> RepoResult {
    let table = r.table.clone().unwrap();
    let lower_bound = r.bounds.unwrap().0;
    let upper_bound = r.bounds.unwrap().1;
    let db = r.db.name();
    duration::<DBResultType>(
        format!("`{table}` rows with ids from `{lower_bound}` to `{upper_bound}` in {db}"),
        r,
        repo::get_row_by_id_range,
    )
}
pub fn count_for(r: Request) -> Result<u32, PgError> {
    duration::<u32>(
        format!(
            "count from {} in {}",
            r.table.as_ref().unwrap(),
            r.db.name()
        ),
        r,
        repo::count_for,
    )
}

pub fn all_tables(r: Request) -> RepoResult {
    duration::<DBResultType>(
        format!("Getting all tables for {}", r.db.name()),
        r,
        repo::all_tables,
    )
}

pub fn tables_with_column(r: Request) -> RepoResult {
    let column = r.column.as_ref().unwrap();
    duration::<DBResultType>(
        format!(
            "Getting all tables with column {} in {}",
            column,
            r.db.name()
        ),
        r,
        repo::tables_with_column,
    )
}
#[allow(dead_code)]
pub fn id_and_column_value(r: Request) -> RepoResult {
    let column = r.column.as_ref().unwrap();
    let table = r.table.as_ref().unwrap();
    let db = r.db.name();
    duration::<DBResultType>(
        format!("Getting `id` and values from column `{column}` from table {table} in {db}"),
        r,
        repo::id_and_column_value,
    )
}

pub fn full_row_ordered_by(r: Request) -> Result<DBResultType, PgError> {
    let table = r.table.as_ref().unwrap();
    duration::<DBResultType>(
        format!("Getting rows from table {table} in {}", r.db.name()),
        r,
        repo::full_row_ordered_by,
    )
}

// Result<Vec<serde_json::Map<String, Value>>, PgError>
pub fn full_row_ordered_by_until(r: Request) -> Result<DBResultType, PgError> {
    let table = r.table.as_ref().unwrap();
    duration::<DBResultType>(
        format!("Getting rows from table {table} in {}", r.db.name()),
        r,
        repo::full_row_ordered_by_until,
    )
}

fn duration<T>(
    message: String,
    p: Request,
    fun: fn(Request) -> Result<T, PgError>,
) -> Result<T, PgError> {
    println!("[{} UTC] START: {message}", Utc::now().format("%F %X"));
    let start = Instant::now();
    let output = fun(p);
    let duration = start.elapsed();

    println!("=> {message} took: {duration:?}");
    output
}

#[cfg(test)]
mod test {
    use super::*;

    use serde_json::{from_str, Map, Value};
    #[test]
    fn test_db_result_types_exclude_ids() {
        let data = vec![gen_json_map(1), gen_json_map(2), gen_json_map(3)];
        let db_result = DBResultType::JsonMaps(data.clone());
        let ids = vec![1, 2];
        let result = if let DBResultType::JsonMaps(result) = db_result.exclude_ids(&ids) {
            result
        } else {
            todo!()
        };
        let expected = vec![gen_json_map(3)];
        assert_eq!(result, expected);
        let db_result = DBResultType::GroupedRows(vec![
            (data[0].clone(), data[0].clone()),
            (data[1].clone(), data[1].clone()),
            (data[2].clone(), data[2].clone()),
        ]);
        let ids = vec![1, 2];
        let result = if let DBResultType::GroupedRows(result) = db_result.exclude_ids(&ids) {
            result
        } else {
            todo!()
        };
        let expected = vec![(gen_json_map(3), gen_json_map(3))];
        assert_eq!(result, expected);
    }

    fn gen_json_map(id: u64) -> JsonMap {
        let data = format!(r#"{{"id": {id},"name": "John_{id}"}}"#);
        let mut v: Value = from_str(&data).unwrap();
        let val = v.as_object_mut().unwrap();
        let mut m = Map::new();
        m.append(val);
        m
    }
}
