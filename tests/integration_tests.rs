mod common;



use common::{TestRunner, DB};
use db_compare::Job;

use db_compare::*;

fn default_args() -> Args {
    Args {
        db1: Some(DB::A.url()),
        db2: Some(DB::B.url()),
        diff_format: None,
        limit: 1,
        no_tls: false,
        all_columns_sample_size: None,
        diff_file: None,
        tables_file: None,
        config: None,
        rows_until: None,
    }
}
fn default_config(jobs: Vec<Job>) -> Config {
    Config {
        jobs,
        white_listed_tables: Some(vec!["users".to_string(), "messages".to_string()]),
        ..Config::new(&default_args())
    }
}
#[test]
fn test_counters() {
    let config = default_config(vec![Job::Counters]);

    TestRunner::new(&config).run("db1 has one record more than db2");
}
#[test]
fn test_updated_ats() {
    let config = default_config(vec![Job::UpdatedAts]);

    TestRunner::new(&config).run("db1 has one record more than db2");
}
#[test]
fn test_created_ats() {
    let config = default_config(vec![Job::CreatedAts]);

    TestRunner::new(&config).run("db1 has one record more than db2");
}

#[test]
fn test_all_columns() {
    let config = default_config(vec![Job::AllColumns]);

    TestRunner::new(&config).run("db1 has one record more than db2");
}
#[test]
fn test_sequences() {
    let config = default_config(vec![Job::Sequences]);
    TestRunner::new(&config).run("db1 has one record more than db2");
}
#[test]
fn test_updated_ats_until() {
    let mut config = default_config(vec![Job::UpdatedAtsUntil]);

    config.limit = 2;

    TestRunner::new(&config).run("db1 has more records than db2");
}

#[test]
fn test_updated_ats_until_limit_1() {
    let mut config = default_config(vec![Job::UpdatedAtsUntil]);

    config.limit = 1;
    TestRunner::new(&config).run("db1 has more records than db2 limit 1");
}
#[test]
fn test_updated_ats_until_limit_2() {
    let mut config = default_config(vec![Job::UpdatedAtsUntil]);

    config.limit = 2;

    TestRunner::new(&config).run("db1 has more records than db2 limit 2");
}
#[test]
fn test_updated_ats_until_limit_5() {
    let mut config = default_config(vec![Job::UpdatedAtsUntil]);

    config.limit = 5;

    TestRunner::new(&config).run("db1 has more records than db2 limit 5");
}
