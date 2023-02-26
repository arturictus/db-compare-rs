use postgres::Error as PgError;
mod repo;
mod request;
use chrono::prelude::*;
pub use repo::ping_db;
use request::DBRequest;
pub use request::RequestBuilder;
use std::time::Instant;

pub fn get_sequences(r: DBRequest) -> Result<Vec<(std::string::String, u32)>, PgError> {
    duration::<Vec<(String, u32)>>(
        format!("Getting sequences from {}", r.db.name()),
        r,
        repo::get_sequences,
    )
}
pub fn get_greatest_id_from(r: DBRequest) -> Result<u32, PgError> {
    let table = r.table.as_ref().unwrap();
    duration::<u32>(
        format!("Greatest id from `{table}` in {}", r.db.name()),
        r,
        repo::get_greatest_id_from,
    )
}

pub fn get_row_by_id_range(r: DBRequest) -> Result<Vec<String>, PgError> {
    duration::<Vec<String>>(
        format!(
            "`{}` rows with ids from `{}` to `{}` in {}",
            r.table.clone().unwrap(),
            r.bounds.unwrap().0,
            r.bounds.unwrap().1,
            r.db.name()
        ),
        r,
        repo::get_row_by_id_range,
    )
}
pub fn count_for(r: DBRequest) -> Result<u32, PgError> {
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

pub fn all_tables(r: DBRequest) -> Result<Vec<String>, PgError> {
    duration::<Vec<String>>(
        format!("Getting all tables for {}", r.db.name()),
        r,
        repo::all_tables,
    )
}

pub fn tables_with_column(r: DBRequest) -> Result<Vec<String>, PgError> {
    let column = r.column.as_ref().unwrap();
    duration::<Vec<String>>(
        format!(
            "Getting all tables with column {} in {}",
            column,
            r.db.name()
        ),
        r,
        repo::tables_with_column,
    )
}

pub fn id_and_column_value(r: DBRequest) -> Result<Vec<String>, PgError> {
    duration::<Vec<String>>(
        format!(
            "Getting `id` and values from column `{}` from table {} in {}",
            r.column.as_ref().unwrap(),
            r.table.as_ref().unwrap(),
            r.db.name()
        ),
        r,
        repo::id_and_column_value,
    )
}

pub fn full_row_ordered_by(r: DBRequest) -> Result<Vec<String>, PgError> {
    let table = r.table.as_ref().unwrap();
    duration::<Vec<String>>(
        format!("Getting rows from table {table} in {}", r.db.name()),
        r,
        repo::full_row_ordered_by,
    )
}

fn duration<T>(
    message: String,
    p: DBRequest,
    fun: fn(DBRequest) -> Result<T, PgError>,
) -> Result<T, PgError> {
    println!("[{} UTC] START: {message}", Utc::now().format("%F %X"));
    let start = Instant::now();
    let output = fun(p);
    let duration = start.elapsed();

    println!("=> {message} took: {duration:?}");
    output
}
