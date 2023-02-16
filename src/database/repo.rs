use crate::database::DBSelector;
use crate::Config;
// use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres::{Client, Error as PgError, NoTls, SimpleQueryMessage};
// use postgres_openssl::MakeTlsConnector;
use futures::TryStreamExt;
use sqlx::{postgres::PgPoolOptions, Error as SqlxError, Pool, Postgres, Row};

pub async fn get_greatest_id_from(
    config: &Config,
    db: DBSelector,
    table: &str,
) -> Result<u32, SqlxError> {
    let mut conn = db.client(config);
    let mut output: u32 = 0;
    let rows =
        sqlx::query(&format!("SELECT id FROM {table} ORDER BY id DESC LIMIT 1;")).fetch(&conn);
    while let Some(row) = rows.try_next().await? {
        // map the row into a user-defined domain type
        output = row.try_get("id").unwrap().parse::<u32>().unwrap();
    }
    // if let Ok(row) = {
    //     for data in row {
    //         if let SimpleQueryMessage::Row(result) = data {
    //             output = result.get(0).unwrap_or("0").parse::<u32>().unwrap();
    //         }
    //     }
    // }
    Ok(output)
}
pub fn get_row_by_id_range(
    config: &Config,
    db: DBSelector,
    table: &str,
    lower_bound: u32,
    upper_bound: u32,
) -> Result<Vec<String>, PgError> {
    use serde_json::Value;
    let mut client = connect(config, db_url)?;
    let column = "id".to_string();
    let limit = config.args.limit;
    let mut records: Vec<String> = Vec::new();
    let the_q = format!(
        "WITH
        cte AS
        (
            SELECT
                *,
                ROW_NUMBER() OVER (ORDER BY {column} DESC) AS rn
            FROM
                {table}
            WHERE
               (id > {lower_bound}) AND (id <= {upper_bound})
        )
    SELECT
        JSON_AGG(cte.* ORDER BY {column} DESC) FILTER (WHERE rn <= {limit}) AS data
    FROM
        cte;"
    );

    if let Ok(rows) = client.simple_query(&the_q) {
        for data in rows {
            if let SimpleQueryMessage::Row(result) = data {
                let value = result.get(0).unwrap_or("[]");
                let list: Vec<Value> = serde_json::from_str(value).unwrap();

                for e in list {
                    records.push(serde_json::to_string(&e).unwrap())
                }
            }
        }
    }

    Ok(records)
}

pub fn count_for(config: &Config, db: DBSelector, table: &str) -> Result<u32, PgError> {
    let mut client = connect(config, db_url)?;
    let mut output: u32 = 0;
    if let Ok(rows) = client.simple_query(&format!("SELECT COUNT(*) FROM {table};")) {
        for data in rows {
            if let SimpleQueryMessage::Row(result) = data {
                output = result.get(0).unwrap_or("0").parse::<u32>().unwrap();
            }
        }
    }
    Ok(output)
}

fn connect(config: &Config, db: DBSelector) -> Result<Client, PgError> {
    if config.args.no_tls {
        Client::connect(db_url, NoTls)
    } else {
        let mut builder =
            SslConnector::builder(SslMethod::tls()).expect("unable to create sslconnector builder");
        builder.set_verify(SslVerifyMode::NONE);
        let connector = MakeTlsConnector::new(builder.build());
        Client::connect(db_url, connector)
    }
}

pub fn all_tables(config: &Config, db: DBSelector) -> Result<Vec<String>, PgError> {
    let mut client = connect(config, db_url)?;
    let mut tables = Vec::new();
    for row in client.query("SELECT table_name FROM information_schema.tables;", &[])? {
        let table_name: Option<String> = row.get(0);
        tables.push(table_name.unwrap());
    }
    tables = white_listed_tables(config, tables);
    tables.sort();
    Ok(tables)
}

pub fn tables_with_column(
    config: &Config,
    db: DBSelector,
    column: String,
) -> Result<Vec<String>, PgError> {
    let mut client = connect(config, db_url)?;
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
    Ok(white_listed_tables(config, tables))
}

pub fn id_and_column_value(
    config: &Config,
    db: DBSelector,
    table: &str,
    column: String,
) -> Result<Vec<String>, PgError> {
    let mut client = connect(config, db_url)?;

    let mut records = Vec::new();
    if let Ok(rows) = client.simple_query(&format!(
        "SELECT id, {column} FROM {table} ORDER BY {column} LIMIT {};",
        config.args.limit
    )) {
        for data in rows {
            if let SimpleQueryMessage::Row(result) = data {
                records.push(format!(
                    "{} : {}",
                    result.get(0).unwrap_or("0").parse::<u32>().unwrap(),
                    result.get(1).unwrap_or("0")
                ));
            }
        }
    }
    Ok(records)
}

fn white_listed_tables(config: &Config, tables: Vec<String>) -> Vec<String> {
    if let Some(whitelisted) = &config.white_listed_tables {
        tables
            .into_iter()
            .filter(|t| whitelisted.contains(t))
            .collect()
    } else {
        tables
    }
}

pub fn full_row_ordered_by(
    config: &Config,
    db: DBSelector,
    table: &str,
    column: String,
) -> Result<Vec<String>, PgError> {
    use serde_json::Value;
    let mut records = Vec::new();
    let mut client = connect(config, db_url)?;
    if let Ok(rows) = client.simple_query(&format!(
        "WITH
        cte AS
        (
            SELECT
                *,
                ROW_NUMBER() OVER (ORDER BY {column} DESC) AS rn
            FROM
                {table}
        )
    SELECT
        JSON_AGG(cte.* ORDER BY {column} DESC) FILTER (WHERE rn <= {}) AS data
    FROM
        cte;",
        config.args.limit
    )) {
        for data in rows {
            if let SimpleQueryMessage::Row(result) = data {
                let value = result.get(0).unwrap_or("[]");
                let list: Vec<Value> = serde_json::from_str(value).unwrap();

                for e in list {
                    records.push(serde_json::to_string(&e).unwrap())
                }
            }
        }
    }
    Ok(records)
}

pub fn ping_db(config: &Config, db: DBSelector) -> Result<(), PgError> {
    let mut client = connect(config, db.url(config))?;
    println!("Ping 10 -> {}", db.name());
    let result = client
        .query_one("select 10", &[])
        .expect("failed to execute select 10 to postgres");
    let value: i32 = result.get(0);
    println!("Pong {value} <- {}", db.url(config));
    Ok(())
}
