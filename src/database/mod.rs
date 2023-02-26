use postgres::Error as PgError;
mod query;
mod repo;
use chrono::prelude::*;
use query::DBRequest;
pub use query::RequestBuilder;
pub use repo::ping_db;
use std::time::Instant;

pub fn get_sequences(q: DBRequest) -> Result<Vec<(std::string::String, u32)>, PgError> {
    duration::<Vec<(String, u32)>>(
        format!("Getting sequences from {}", q.db.name()),
        q,
        repo::get_sequences,
    )
}
pub fn get_greatest_id_from(q: DBRequest) -> Result<u32, PgError> {
    let table = q.table.as_ref().unwrap();
    duration::<u32>(
        format!("Greatest id from `{table}` in {}", q.db.name()),
        q,
        repo::get_greatest_id_from,
    )
}

pub fn get_row_by_id_range(query: DBRequest) -> Result<Vec<String>, PgError> {
    duration::<Vec<String>>(
        format!(
            "`{}` rows with ids from `{}` to `{}` in {}",
            query.table.clone().unwrap(),
            query.bounds.unwrap().0,
            query.bounds.unwrap().1,
            query.db.name()
        ),
        query,
        repo::get_row_by_id_range,
    )
}
pub fn count_for(query: DBRequest) -> Result<u32, PgError> {
    duration::<u32>(
        format!(
            "count from {} in {}",
            query.table.as_ref().unwrap(),
            query.db.name()
        ),
        query,
        repo::count_for,
    )
}

pub fn all_tables(q: DBRequest) -> Result<Vec<String>, PgError> {
    duration::<Vec<String>>(
        format!("Getting all tables for {}", q.db.name()),
        q,
        repo::all_tables,
    )
}

pub fn tables_with_column(q: DBRequest) -> Result<Vec<String>, PgError> {
    let column = q.column.as_ref().unwrap();
    duration::<Vec<String>>(
        format!(
            "Getting all tables with column {} in {}",
            column,
            q.db.name()
        ),
        q,
        repo::tables_with_column,
    )
}

pub fn id_and_column_value(q: DBRequest) -> Result<Vec<String>, PgError> {
    duration::<Vec<String>>(
        format!(
            "Getting `id` and values from column `{}` from table {} in {}",
            q.column.as_ref().unwrap(),
            q.table.as_ref().unwrap(),
            q.db.name()
        ),
        q,
        repo::id_and_column_value,
    )
}

pub fn full_row_ordered_by(q: DBRequest) -> Result<Vec<String>, PgError> {
    let table = q.table.as_ref().unwrap();
    duration::<Vec<String>>(
        format!("Getting rows from table {table} in {}", q.db.name()),
        q,
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
