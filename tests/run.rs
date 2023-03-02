mod common;
use common::{TestRunner, User, DB};
use db_compare::Job;

use db_compare::*;

fn default_args() -> Args {
    Args {
        db1: Some(DB::A.url()),
        db2: Some(DB::B.url()),
        limit: 1,
        no_tls: false,
        all_columns_sample_size: None,
        diff_file: None,
        tables_file: None,
        config: None,
    }
}
fn default_config(jobs: Vec<Job>) -> Config {
    Config {
        jobs,
        white_listed_tables: Some(vec!["users".to_string()]),
        ..Config::new(&default_args())
    }
}
#[test]
fn test_counters() {
    let config = default_config(vec![Job::Counters]);
    TestRunner::new(&config).run(|c| {
        let first = User::new().insert(DB::A).unwrap();
        assert_eq!(first.id, Some(1));
        assert_eq!(User::all(DB::A).len(), 1);
        assert_eq!(User::all(DB::B).len(), 0);
        db_compare::run(c).unwrap();
    });
}
#[test]
fn test_updated_ats() {
    let config = default_config(vec![Job::UpdatedAts]);
    TestRunner::new(&config).run(|c| {
        let first = User::new().insert(DB::A).unwrap();
        assert_eq!(first.id, Some(1));
        assert_eq!(User::all(DB::A).len(), 1);
        assert_eq!(User::all(DB::B).len(), 0);
        db_compare::run(c).unwrap();
    });
}
#[test]
fn test_created_ats() {
    let config = default_config(vec![Job::CreatedAts]);
    TestRunner::new(&config).run(|c| {
        let first = User::new().insert(DB::A).unwrap();
        assert_eq!(first.id, Some(1));
        assert_eq!(User::all(DB::A).len(), 1);
        assert_eq!(User::all(DB::B).len(), 0);
        db_compare::run(c).unwrap();
    });
}

#[test]
fn test_all_columns() {
    let config = default_config(vec![Job::AllColumns]);
    TestRunner::new(&config).run(|c| {
        let first = User::new().insert(DB::A).unwrap();
        assert_eq!(first.id, Some(1));
        assert_eq!(User::all(DB::A).len(), 1);
        assert_eq!(User::all(DB::B).len(), 0);
        db_compare::run(c).unwrap();
    });
}
#[test]
fn test_sequences() {
    let config = default_config(vec![Job::Sequences]);
    TestRunner::new(&config).run(|c| {
        let first = User::new().insert(DB::A).unwrap();
        assert_eq!(first.id, Some(1));
        assert_eq!(User::all(DB::A).len(), 1);
        assert_eq!(User::all(DB::B).len(), 0);
        db_compare::run(c).unwrap();
    });
}
