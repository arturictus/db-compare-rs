use crate::Args;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres::{Client, Error, NoTls, SimpleQueryMessage};
use postgres_openssl::MakeTlsConnector;

pub fn all_tables(args: &Args, db_url: &str) -> Result<Vec<String>, Error> {
    println!("== QUERY: Getting all tables");
    let mut client = connect(args, db_url)?;
    let mut tables = Vec::new();
    for row in client.query("SELECT table_name FROM information_schema.tables;", &[])? {
        let table_name: Option<String> = row.get(0);
        tables.push(table_name.unwrap());
    }
    Ok(tables)
}

pub fn tables_with_column(args: &Args, db_url: &str, column: String) -> Result<Vec<String>, Error> {
    println!("== QUERY: Getting all tables with column {}", column);
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
    println!(
        "== QUERY: Getting `id` and values from column `{}` from table {}",
        column, table
    );
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
    println!("== QUERY: Getting rows from table {}", table);
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

pub fn ping_db(args: &Args) -> Result<(), postgres::Error> {
    let mut client = connect(args, &args.db1)?;
    println!("Ping -> 10");
    let result = client
        .query_one("select 10", &[])
        .expect("failed to execute select 10 to postgres");
    let value: i32 = result.get(0);
    println!("Pong -> {}", value);
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
