use crate::Config;
use postgres::Error as PgError;
mod query;
mod repo;
use chrono::prelude::*;
pub use query::{DBQuery, QueryBuilder};
pub use repo::ping_db;
use std::time::Instant;

#[derive(Clone, Copy, Debug)]
pub enum DBSelector {
    MasterDB,
    ReplicaDB,
}

impl DBSelector {
    pub fn name(&self) -> String {
        match self {
            Self::MasterDB => "DB1".to_string(),
            Self::ReplicaDB => "DB2".to_string(),
        }
    }

    pub fn url<'main>(&self, config: &'main Config) -> &'main String {
        match self {
            Self::MasterDB => &config.db1,
            Self::ReplicaDB => &config.db2,
        }
    }
}
struct Query<'a> {
    config: &'a Config,
    db_url: &'a str,
    table: Option<&'a str>,
    column: Option<String>,
    bounds: Option<(u32, u32)>,
}

pub fn get_sequences(
    config: &Config,
    db: DBSelector,
) -> Result<Vec<(std::string::String, u32)>, PgError> {
    duration::<Vec<(String, u32)>>(
        format!("Getting sequences from {}", db.name()),
        Query {
            config,
            db_url: db.url(config),
            table: None,
            column: None,
            bounds: None,
        },
        |params| repo::get_sequences(params.config, params.db_url),
    )
}
pub fn get_greatest_id_from(config: &Config, db: DBSelector, table: &str) -> Result<u32, PgError> {
    duration::<u32>(
        format!("Greatest id from `{table}` in {}", db.name()),
        Query {
            config,
            db_url: db.url(config),
            table: Some(table),
            column: None,
            bounds: None,
        },
        |params| repo::get_greatest_id_from(params.config, params.db_url, params.table.unwrap()),
    )
}

pub fn get_row_by_id_range(query: DBQuery) -> Result<Vec<String>, PgError> {
    new_duration::<Vec<String>>(
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
pub fn count_for(query: DBQuery) -> Result<u32, PgError> {
    new_duration::<u32>(
        format!(
            "count from {} in {}",
            query.table.as_ref().unwrap(),
            query.db.name()
        ),
        query,
        repo::count_for,
    )
}

pub fn all_tables(config: &Config, db: DBSelector) -> Result<Vec<String>, PgError> {
    duration::<Vec<String>>(
        format!("Getting all tables for {}", db.name()),
        Query {
            config,
            db_url: db.url(config),
            table: None,
            column: None,
            bounds: None,
        },
        |params| repo::all_tables(params.config, params.db_url),
    )
}

pub fn tables_with_column(
    config: &Config,
    db: DBSelector,
    column: String,
) -> Result<Vec<String>, PgError> {
    duration::<Vec<String>>(
        format!("Getting all tables with column {} in {}", column, db.name()),
        Query {
            config,
            db_url: db.url(config),
            column: Some(column),
            table: None,
            bounds: None,
        },
        |params| repo::tables_with_column(params.config, params.db_url, params.column.unwrap()),
    )
}

pub fn id_and_column_value(q: DBQuery) -> Result<Vec<String>, PgError> {
    new_duration::<Vec<String>>(
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

pub fn full_row_ordered_by(q: DBQuery) -> Result<Vec<String>, PgError> {
    let table = q.table.as_ref().unwrap();
    new_duration::<Vec<String>>(
        format!("Getting rows from table {table} in {}", q.db.name()),
        q,
        repo::full_row_ordered_by,
    )
}

fn duration<T>(
    message: String,
    p: Query,
    fun: fn(Query) -> Result<T, PgError>,
) -> Result<T, PgError> {
    println!("[{} UTC] START: {message}", Utc::now().format("%F %X"));
    let start = Instant::now();
    let output = fun(p);
    let duration = start.elapsed();

    println!("=> took: {duration:?}");
    output
}
fn new_duration<T>(
    message: String,
    p: DBQuery,
    fun: fn(DBQuery) -> Result<T, PgError>,
) -> Result<T, PgError> {
    println!("[{} UTC] START: {message}", Utc::now().format("%F %X"));
    let start = Instant::now();
    let output = fun(p);
    let duration = start.elapsed();

    println!("=> took: {duration:?}");
    output
}
