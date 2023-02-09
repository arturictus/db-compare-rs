use crate::Config;
use postgres::Error as PgError;
mod repo;
use chrono::prelude::*;
pub use repo::ping_db;
use std::time::Instant;

struct Query<'a> {
    config: &'a Config<'a>,
    db_url: &'a str,
    table: Option<&'a str>,
    column: Option<String>,
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

pub fn count_for(config: &Config, db_url: &str, table: &str) -> Result<u32, PgError> {
    duration::<u32>(
        format!(
            "count from {} in {}",
            table,
            config.db_url_shortener(db_url)
        ),
        Query {
            config,
            db_url,
            table: Some(table),
            column: None,
        },
        |params| repo::count_for(params.config, params.db_url, params.table.unwrap()),
    )
}

pub fn all_tables(config: &Config, db_url: &str) -> Result<Vec<String>, PgError> {
    duration::<Vec<String>>(
        format!("Getting all tables for {}", config.db_url_shortener(db_url)),
        Query {
            config,
            db_url,
            table: None,
            column: None,
        },
        |params| repo::all_tables(params.config, params.db_url),
    )
}

pub fn tables_with_column(
    config: &Config,
    db_url: &str,
    column: String,
) -> Result<Vec<String>, PgError> {
    duration::<Vec<String>>(
        format!(
            "Getting all tables with column {} in {}",
            column,
            config.db_url_shortener(db_url)
        ),
        Query {
            config,
            db_url,
            table: None,
            column: Some(column),
        },
        |params| repo::tables_with_column(params.config, params.db_url, params.column.unwrap()),
    )
}

pub fn id_and_column_value(
    config: &Config,
    db_url: &str,
    table: &str,
    column: String,
) -> Result<Vec<String>, PgError> {
    duration::<Vec<String>>(
        format!(
            "Getting `id` and values from column `{}` from table {} in {}",
            column,
            table,
            config.db_url_shortener(db_url)
        ),
        Query {
            config,
            db_url,
            table: Some(table),
            column: Some(column),
        },
        |params| {
            repo::id_and_column_value(
                params.config,
                params.db_url,
                params.table.unwrap(),
                params.column.unwrap(),
            )
        },
    )
}

pub fn full_row_ordered_by(
    config: &Config,
    db_url: &str,
    table: &str,
    column: String,
) -> Result<Vec<String>, PgError> {
    duration::<Vec<String>>(
        format!(
            "Getting rows from table {} in {}",
            table,
            config.db_url_shortener(db_url)
        ),
        Query {
            config,
            db_url,
            table: Some(table),
            column: Some(column),
        },
        |params| {
            repo::full_row_ordered_by(
                params.config,
                params.db_url,
                params.table.unwrap(),
                params.column.unwrap(),
            )
        },
    )
}
