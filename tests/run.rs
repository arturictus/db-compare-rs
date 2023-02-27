use anyhow::Result;
use db_compare::*;

static DBHOST: &str = "postgresql://postgres:postgres@127.0.0.1";
static DB1: &str = "db_compare_test_db1";
static DB2: &str = "db_compare_test_db2";

fn default_args() -> Args {
    Args {
        db1: Some(format!("{DBHOST}/{DB1}").to_string()),
        db2: Some(format!("{DBHOST}/{DB2}").to_string()),
        limit: 1,
        no_tls: false,
        all_columns_sample_size: None,
        diff_file: None,
        tables_file: None,
        config: Some("./tests/fixtures/run-test-config.yml".to_string()),
    }
}
#[test]
fn integration_test() {
    let db1 = format!("{DBHOST}/{DB1}");
    let db2 = format!("{DBHOST}/{DB1}");
    drop_all(&db1).unwrap();
    drop_all(&db2).unwrap();

    around(|| db_compare::run(default_args()));
}

fn around(fun: fn() -> Result<(), postgres::Error>) {
    let db1 = format!("{DBHOST}/{DB1}");
    let db2 = format!("{DBHOST}/{DB1}");
    setup_tables(&db1).unwrap();
    setup_tables(&db2).unwrap();
    let r = fun();
    drop_all(&db1).unwrap();
    drop_all(&db2).unwrap();
    r.unwrap();
}

use postgres::{Client, Error, NoTls};

fn drop_all(db: &str) -> Result<(), Error> {
    let mut client = Client::connect(DBHOST, NoTls).unwrap();
    let db_name = db.split("/").into_iter().last().unwrap();
    client.batch_execute(&format!("DROP database {db_name}"))
}
fn setup_tables(db: &str) -> Result<(), Error> {
    let mut client = Client::connect(DBHOST, NoTls)?;
    let db_name = db.split("/").into_iter().last().unwrap();
    client.batch_execute(&format!("CREATE DATABASE {db_name}"))?;

    let mut client = Client::connect(db, NoTls)?;
    client.batch_execute(
        "
        CREATE TABLE IF NOT EXISTS author (
            id              SERIAL PRIMARY KEY,
            name            VARCHAR NOT NULL,
            country         VARCHAR NOT NULL
            )
    ",
    )?;

    client.batch_execute(
        "
        CREATE TABLE IF NOT EXISTS book  (
            id              SERIAL PRIMARY KEY,
            title           VARCHAR NOT NULL,
            author_id       INTEGER NOT NULL REFERENCES author
            )
    ",
    )?;

    Ok(())
}
