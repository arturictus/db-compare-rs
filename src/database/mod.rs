use crate::{db_url_shortener, Args};
use postgres::{Error, TransactionBuilder};
mod repo;
pub use repo::ping_db;
use std::time::Instant;

struct Query<'a> {
    args: &'a Args,
    db_url: &'a str,
    table: Option<&'a str>,
    column: Option<String>,
}

fn duration<T>(message: String, p: Query, fun: fn(Query) -> Result<T, Error>) -> Result<T, Error> {
    let start = Instant::now();
    let output = fun(p);
    let duration = start.elapsed();

    println!("{} (took: {:?})", message, duration);
    output
}

pub fn count_for(args: &Args, db_url: &str, table: &str) -> Result<u32, Error> {
    duration::<u32>(
        format!(
            "== RESULT: count from {} in {}",
            table,
            db_url_shortener(args, db_url)
        ),
        Query {
            args,
            db_url,
            table: Some(table),
            column: None,
        },
        |params| repo::count_for(params.args, params.db_url, params.table.unwrap()),
    )
}

pub fn all_tables(args: &Args, db_url: &str) -> Result<Vec<String>, Error> {
    duration::<Vec<String>>(
        format!(
            "== QUERY: Getting all tables for {}",
            db_url_shortener(args, db_url)
        ),
        Query {
            args,
            db_url,
            table: None,
            column: None,
        },
        |params| repo::all_tables(params.args, params.db_url),
    )
}

pub fn tables_with_column(args: &Args, db_url: &str, column: String) -> Result<Vec<String>, Error> {
    duration::<Vec<String>>(
        format!(
            "== QUERY: Getting all tables with column {} in {}",
            column,
            db_url_shortener(args, db_url)
        ),
        Query {
            args,
            db_url,
            table: None,
            column: Some(column),
        },
        |params| repo::tables_with_column(params.args, params.db_url, params.column.unwrap()),
    )
}

pub fn id_and_column_value(
    args: &Args,
    db_url: &str,
    table: &str,
    column: String,
) -> Result<Vec<String>, Error> {
    duration::<Vec<String>>(
        format!(
            "== QUERY: Getting `id` and values from column `{}` from table {} in {}",
            column,
            table,
            db_url_shortener(args, db_url)
        ),
        Query {
            args,
            db_url,
            table: Some(table),
            column: Some(column),
        },
        |params| {
            repo::id_and_column_value(
                params.args,
                params.db_url,
                params.table.unwrap(),
                params.column.unwrap(),
            )
        },
    )
}

pub fn full_row_ordered_by(
    args: &Args,
    db_url: &str,
    table: &str,
    column: String,
) -> Result<Vec<String>, Error> {
    duration::<Vec<String>>(
        format!(
            "== QUERY: Getting rows from table {} in {}",
            table,
            db_url_shortener(args, db_url)
        ),
        Query {
            args,
            db_url,
            table: Some(table),
            column: Some(column),
        },
        |params| {
            repo::full_row_ordered_by(
                params.args,
                params.db_url,
                params.table.unwrap(),
                params.column.unwrap(),
            )
        },
    )
}
