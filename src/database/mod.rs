use crate::db_url_shortener;
use crate::Args;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres::{Client, Error, NoTls, SimpleQueryMessage};
use postgres_openssl::MakeTlsConnector;
use std::time::{Duration, Instant};

struct Query<'a> {
    args: &'a Args,
    db_url: &'a str,
    table: Option<&'a str>,
    column: Option<String>,
}

fn duration<T>(title: String, p: Query, fun: fn(Query) -> Result<T, Error>) -> Result<T, Error> {
    let start = Instant::now();
    let output = fun(p);
    let duration = start.elapsed();

    println!("{} (took: {:?})", title, duration);
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
        |params| do_count_for(params.args, params.db_url, params.table.unwrap()),
    )
}

fn do_count_for(args: &Args, db_url: &str, table: &str) -> Result<u32, Error> {
    let mut client = connect(args, db_url)?;
    let mut output: u32 = 0;
    for row in client.simple_query(&format!("SELECT COUNT(*) FROM {};", table)) {
        for data in row {
            match data {
                SimpleQueryMessage::Row(result) => {
                    output = result.get(0).unwrap_or("0").parse::<u32>().unwrap();
                }

                _ => (),
            }
        }
    }
    Ok(output)
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
        |params| do_all_tables(params.args, params.db_url),
    )
}

fn do_all_tables(args: &Args, db_url: &str) -> Result<Vec<String>, Error> {
    let mut client = connect(args, db_url)?;
    let mut tables = Vec::new();
    for row in client.query("SELECT table_name FROM information_schema.tables;", &[])? {
        let table_name: Option<String> = row.get(0);
        tables.push(table_name.unwrap());
    }
    Ok(tables)
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
        |params| do_tables_with_column(params.args, params.db_url, params.column.unwrap()),
    )
}

fn do_tables_with_column(args: &Args, db_url: &str, column: String) -> Result<Vec<String>, Error> {
    let mut client = connect(args, db_url)?;
    let mut tables: Vec<String> = Vec::new();
    for row in client.query(
        "select t.table_name
      from information_schema.tables t
      inner join information_schema.columns c on c.table_name = t.table_name
                                      and c.table_schema = t.table_schema
      where c.column_name = $1
            and t.table_schema not in ('information_schema', 'pg_catalog')
            and t.table_type = 'BASE TABLE'
      order by t.table_schema;",
        &[&column],
    )? {
        let data: Option<String> = row.get(0);
        tables.push(data.unwrap())
    }
    Ok(tables)
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
            do_id_and_column_value(
                params.args,
                params.db_url,
                params.table.unwrap(),
                params.column.unwrap(),
            )
        },
    )
}

fn do_id_and_column_value(
    args: &Args,
    db_url: &str,
    table: &str,
    column: String,
) -> Result<Vec<String>, Error> {
    let mut client = connect(args, db_url)?;

    let mut records = Vec::new();
    for row in client.simple_query(&format!(
        "SELECT id, {} FROM {} ORDER BY {} LIMIT {};",
        column, table, column, args.limit
    )) {
        for data in row {
            match data {
                SimpleQueryMessage::Row(result) => {
                    records.push(format!(
                        "{} : {}",
                        result.get(0).unwrap_or("0").parse::<u32>().unwrap(),
                        result.get(1).unwrap_or("0")
                    ));
                }

                _ => (),
            }
        }
    }
    Ok(records)
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
            do_full_row_ordered_by(
                params.args,
                params.db_url,
                params.table.unwrap(),
                params.column.unwrap(),
            )
        },
    )
}
fn do_full_row_ordered_by(
    args: &Args,
    db_url: &str,
    table: &str,
    column: String,
) -> Result<Vec<String>, Error> {
    use serde_json::Value;
    let mut records = Vec::new();
    let mut client = connect(args, db_url)?;
    for row in client.simple_query(&format!(
        "WITH
          cte AS
          (
              SELECT
                  *,
                  ROW_NUMBER() OVER (ORDER BY {} DESC) AS rn
              FROM
                  {}
          )
      SELECT
          JSON_AGG(cte.* ORDER BY {} DESC) FILTER (WHERE rn <= {}) AS data
      FROM
          cte;",
        column, table, column, args.limit
    )) {
        for data in row {
            match data {
                SimpleQueryMessage::Row(result) => {
                    let data = result.get(0).unwrap_or("[]");
                    let list: Vec<Value> = serde_json::from_str(data).unwrap();

                    for e in list {
                        records.push(serde_json::to_string(&e).unwrap())
                    }
                }

                _ => (),
            }
        }
    }
    Ok(records)
}

pub fn ping_db(args: &Args, db_url: &str) -> Result<(), postgres::Error> {
    let mut client = connect(args, db_url)?;
    println!("Ping {} -> 10", db_url_shortener(args, db_url));
    let result = client
        .query_one("select 10", &[])
        .expect("failed to execute select 10 to postgres");
    let value: i32 = result.get(0);
    println!("Pong {} -> {}", db_url_shortener(args, db_url), value);
    Ok(())
}

pub fn connect(args: &Args, db_url: &str) -> Result<Client, postgres::Error> {
    match args.tls {
        Some(false) => Client::connect(db_url, NoTls),
        _ => {
            let mut builder = SslConnector::builder(SslMethod::tls())
                .expect("unable to create sslconnector builder");
            builder.set_verify(SslVerifyMode::NONE);
            let connector = MakeTlsConnector::new(builder.build());
            Client::connect(db_url, connector)
        }
    }
}
