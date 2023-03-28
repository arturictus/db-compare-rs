use postgres::Error as PgError;
mod repo;
mod request;
use chrono::prelude::*;
pub use repo::{ping_db, AResult};
use serde_json::Value;

pub use request::{Request, RequestBuilder};
use std::time::Instant;

use crate::DBResultTypes;

use self::request::DB;

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

pub fn get_row_by_id_range(r: Request) -> AResult {
    let table = r.table.clone().unwrap();
    let lower_bound = r.bounds.unwrap().0;
    let upper_bound = r.bounds.unwrap().1;
    let db = r.db.name();
    duration::<DBResultTypes>(
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

pub fn all_tables(r: Request) -> AResult {
    duration::<DBResultTypes>(
        format!("Getting all tables for {}", r.db.name()),
        r,
        repo::all_tables,
    )
}

pub fn tables_with_column(r: Request) -> AResult {
    let column = r.column.as_ref().unwrap();
    duration::<DBResultTypes>(
        format!(
            "Getting all tables with column {} in {}",
            column,
            r.db.name()
        ),
        r,
        repo::tables_with_column,
    )
}

pub fn id_and_column_value(r: Request) -> AResult {
    let column = r.column.as_ref().unwrap();
    let table = r.table.as_ref().unwrap();
    let db = r.db.name();
    duration::<DBResultTypes>(
        format!("Getting `id` and values from column `{column}` from table {table} in {db}"),
        r,
        repo::id_and_column_value,
    )
}

pub fn full_row_ordered_by(r: Request) -> Result<DBResultTypes, PgError> {
    let table = r.table.as_ref().unwrap();
    duration::<DBResultTypes>(
        format!("Getting rows from table {table} in {}", r.db.name()),
        r,
        repo::full_row_ordered_by,
    )
}

// Result<Vec<serde_json::Map<String, Value>>, PgError>
pub fn full_row_ordered_by_until(r: Request) -> Result<DBResultTypes, PgError> {
    let table = r.table.as_ref().unwrap();
    duration::<DBResultTypes>(
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
