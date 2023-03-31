use crate::database::{DBResultTypes, Request};
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres::{Client, Error as PgError, NoTls, SimpleQueryMessage};
use postgres_openssl::MakeTlsConnector;
use serde_json::Value;

pub type AResult = Result<DBResultTypes, PgError>;
pub fn get_sequences(q: Request) -> Result<Vec<(String, u32)>, PgError> {
    let mut client = connect(&q)?;
    let mut records: Vec<(String, u32)> = Vec::new();
    let q = "SELECT sequencename AS sequence,last_value FROM pg_sequences ORDER BY sequencename;";
    if let Ok(rows) = client.simple_query(q) {
        for data in rows {
            if let SimpleQueryMessage::Row(result) = data {
                records.push((
                    result.get(0).unwrap_or("error").into(),
                    result.get(1).unwrap_or("0").parse::<u32>().unwrap(),
                ));
            }
        }
    }
    Ok(records)
}
pub fn get_greatest_id_from(q: Request) -> Result<u32, PgError> {
    let mut client = connect(&q)?;
    let table = q.table.unwrap();
    let mut output: u32 = 0;
    if let Ok(row) =
        client.simple_query(&format!("SELECT id FROM {table} ORDER BY id DESC LIMIT 1;"))
    {
        for data in row {
            if let SimpleQueryMessage::Row(result) = data {
                output = result.get(0).unwrap_or("0").parse::<u32>().unwrap();
            }
        }
    }
    Ok(output)
}
pub fn get_row_by_id_range(q: Request) -> AResult {
    let mut client = connect(&q)?;
    let column = "id".to_string();
    let limit = q.config.limit;
    let table = q.table.unwrap();
    let (lower_bound, upper_bound) = q.bounds.unwrap();
    let query = format!(
        "WITH
        cte AS
        (
            SELECT
                *,
                ROW_NUMBER() OVER (ORDER BY {column} DESC) AS db_compare_row_number
            FROM
                {table}
            WHERE
               (id > {lower_bound}) AND (id <= {upper_bound})
        )
    SELECT
        JSON_AGG(cte.* ORDER BY {column} DESC) FILTER (WHERE db_compare_row_number <= {limit}) AS data
    FROM
        cte;"
    );

    let records = collect_records_with_rn(&query, &mut client)?;
    Ok(DBResultTypes::Map(records))
}

pub fn count_for(query: Request) -> Result<u32, PgError> {
    let mut client = connect(&query)?;
    let mut output: u32 = 0;
    if let Ok(rows) =
        client.simple_query(&format!("SELECT COUNT(*) FROM {};", query.table.unwrap()))
    {
        for data in rows {
            if let SimpleQueryMessage::Row(result) = data {
                output = result.get(0).unwrap_or("0").parse::<u32>().unwrap();
            }
        }
    }
    Ok(output)
}

fn connect(query: &Request) -> Result<Client, PgError> {
    if query.config.tls {
        let mut builder =
            SslConnector::builder(SslMethod::tls()).expect("unable to create sslconnector builder");
        builder.set_verify(SslVerifyMode::NONE);
        let connector = MakeTlsConnector::new(builder.build());
        Client::connect(&query.db.url(), connector)
    } else {
        Client::connect(&query.db.url(), NoTls)
    }
}

pub fn all_tables(q: Request) -> AResult {
    let mut client = connect(&q)?;
    let mut tables = Vec::new();
    for row in client.query("SELECT table_name FROM information_schema.tables;", &[])? {
        let table_name: Option<String> = row.get(0);
        tables.push(table_name.unwrap());
    }
    tables = white_listed_tables(q, tables);
    tables.sort();
    Ok(DBResultTypes::String(tables))
}

pub fn tables_with_column(q: Request) -> AResult {
    let mut client = connect(&q)?;
    let mut tables: Vec<String> = Vec::new();
    let column = q.column.as_ref().unwrap();
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
    Ok(DBResultTypes::String(white_listed_tables(q, tables)))
}

pub fn id_and_column_value(q: Request) -> AResult {
    let mut client = connect(&q)?;
    let column = q.column.unwrap();
    let table = q.table.unwrap();
    let mut records = Vec::new();
    if let Ok(rows) = client.simple_query(&format!(
        "SELECT id, {column} FROM {table} ORDER BY {column} LIMIT {};",
        q.config.limit
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
    Ok(DBResultTypes::String(records))
}

fn white_listed_tables(q: Request, tables: Vec<String>) -> Vec<String> {
    if let Some(whitelisted) = q.config.white_listed_tables {
        tables
            .into_iter()
            .filter(|t| whitelisted.contains(t))
            .collect()
    } else {
        tables
    }
}

pub fn full_row_ordered_by(q: Request) -> AResult {
    let mut client = connect(&q)?;
    let column = q.column.unwrap();
    let table = q.table.unwrap();
    let limit = q.config.limit;
    let query = format!(
        "WITH
        cte AS
        (
            SELECT
                *,
                ROW_NUMBER() OVER (ORDER BY {column} DESC) AS db_compare_row_number
            FROM
                {table}
        )
    SELECT
        JSON_AGG(cte.* ORDER BY {column} DESC) FILTER (WHERE db_compare_row_number <= {limit}) AS data
    FROM
        cte;"
    );
    let records = collect_records_with_rn(&query, &mut client)?;

    Ok(DBResultTypes::Map(records))
}
pub fn full_row_ordered_by_until(q: Request) -> AResult {
    let mut client = connect(&q)?;
    let column = q.column.unwrap();

    let table = q.table.unwrap();
    let limit = q.config.limit;
    let until = q.tm_cutoff;
    let until = until.format("%Y-%m-%d %H:%M:%S").to_string();
    let q = format!(
        "WITH
        cte AS
        (
            SELECT
                *,
                ROW_NUMBER() OVER (ORDER BY {column} DESC) AS db_compare_row_number
            FROM
                {table}
            WHERE
                {column} < '{until}'
        )
    SELECT
        JSON_AGG(cte.* ORDER BY {column} DESC) FILTER (WHERE db_compare_row_number <= {limit}) AS data
    FROM
        cte;"
    );
    let records = collect_records_with_rn(&q, &mut client)?;
    Ok(DBResultTypes::Map(records))
}

pub fn ping_db(q: Request) -> Result<(), PgError> {
    let mut client = connect(&q)?;
    println!("Ping 10 -> {}", q.db.name());
    let result = client
        .query_one("select 10", &[])
        .expect("failed to execute select 10 to postgres");
    let value: i32 = result.get(0);
    println!("Pong {value} <- {}", q.db.name());
    Ok(())
}

fn collect_records_with_rn(
    query: &str,
    client: &mut Client,
) -> Result<Vec<serde_json::Map<String, Value>>, PgError> {
    let mut records = Vec::new();
    let rows = client.simple_query(query)?;

    for data in rows {
        if let SimpleQueryMessage::Row(result) = data {
            let value = result.get(0).unwrap_or("[]");
            let list: Vec<serde_json::Map<String, Value>> = serde_json::from_str(value).unwrap();

            for mut e in list {
                e.remove("db_compare_row_number");
                records.push(e)
            }
        }
    }
    Ok(records)
}

pub fn updated_ids_after_cutoff(q: Request) -> AResult {
    let mut client = connect(&q)?;
    let table = q.table.unwrap();
    let cutoff = q.tm_cutoff;
    let cutoff = cutoff.format("%Y-%m-%d %H:%M:%S").to_string();
    let query = format!("SELECT json_agg(id) FROM {table} WHERE updated_at > '{cutoff}';");
    let rows = client.simple_query(&query)?;

    let mut records = Vec::new();

    for data in rows {
        if let SimpleQueryMessage::Row(result) = data {
            records.push(result.get(0).unwrap_or("0").parse::<u32>().unwrap());
        }
    }

    Ok(DBResultTypes::Ids(records))
}
