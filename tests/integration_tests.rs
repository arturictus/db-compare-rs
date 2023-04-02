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
        tables: None,
        config: None,
        jobs: None,
        tm_cutoff: None,
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

    TestRunner::new(&config).run("test_counters");
}
#[test]
fn test_updated_ats() {
    let config = default_config(vec![Job::UpdatedAts]);

    TestRunner::new(&config).run("updated_ats");
}
#[test]
fn test_created_ats() {
    let config = default_config(vec![Job::CreatedAts]);

    TestRunner::new(&config).run("created_ats");
}

#[test]
fn test_all_columns() {
    let config = default_config(vec![Job::ByID]);

    TestRunner::new(&config).run("all columns");
}

#[test]
fn test_all_columns_limit_5() {
    let mut config = default_config(vec![Job::ByID]);
    config.limit = 5;
    TestRunner::new(&config).run("all columns limit 5");
}

#[test]
fn test_sequences() {
    let config = default_config(vec![Job::Sequences]);
    TestRunner::new(&config).run("sequences");
}
#[test]
fn test_updated_ats_until() {
    let mut config = default_config(vec![Job::UpdatedAtsUntil]);

    config.limit = 2;

    TestRunner::new(&config).run("updated_ats_until_limit_2");
}

#[test]
fn test_updated_ats_until_limit_1() {
    let mut config = default_config(vec![Job::UpdatedAtsUntil]);

    config.limit = 1;
    TestRunner::new(&config).run("updated_ats_until_limit_1");
}

#[test]
fn test_updated_ats_until_limit_5() {
    let mut config = default_config(vec![Job::UpdatedAtsUntil]);

    config.limit = 5;

    TestRunner::new(&config).run("updated_ats_until_limit_5");
}
#[test]
fn test_all_columns_excluding_replica_updated_ats() {
    let config = default_config(vec![Job::ByIDExcludingReplicaUpdatedAts]);

    TestRunner::new(&config).run("all_columns_excluding_replica_updated_ats");
}
