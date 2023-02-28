mod common;
use common::{TestRunner, User, DB};

use db_compare::*;

fn default_args() -> Args {
    Args {
        db1: Some(DB::A.url().to_string()),
        db2: Some(DB::B.url().to_string()),
        limit: 1,
        no_tls: false,
        all_columns_sample_size: None,
        diff_file: None,
        tables_file: None,
        config: None,
    }
}
fn default_config(jobs: Option<Vec<String>>) -> Config {
    Config {
        jobs,
        white_listed_tables: Some(vec!["users".to_string()]),
        ..Config::new(&default_args())
    }
}
#[test]
fn test_counters() {
    let config = default_config(Some(vec!["counters".to_string()]));
    TestRunner::new(&config).run(|c| {
        let first = User::new().insert(DB::A).unwrap();
        assert_eq!(first.id, Some(1));
        assert_eq!(User::all(DB::A).len(), 1);
        assert_eq!(User::all(DB::B).len(), 0);
        db_compare::run(&c).unwrap();
    });
}
#[test]
fn test_updated_ats() {
    let config = default_config(Some(vec!["last_updated_ats".to_string()]));
    TestRunner::new(&config).run(|c| {
        let first = User::new().insert(DB::A).unwrap();
        assert_eq!(first.id, Some(1));
        assert_eq!(User::all(DB::A).len(), 1);
        assert_eq!(User::all(DB::B).len(), 0);
        db_compare::run(&c).unwrap();
    });
}
#[test]
fn test_created_ats() {
    let config = default_config(Some(vec!["last_created_ats".to_string()]));
    TestRunner::new(&config).run(|c| {
        let first = User::new().insert(DB::A).unwrap();
        assert_eq!(first.id, Some(1));
        assert_eq!(User::all(DB::A).len(), 1);
        assert_eq!(User::all(DB::B).len(), 0);
        db_compare::run(&c).unwrap();
    });
}

#[test]
fn test_all_columns() {
    let config = default_config(Some(vec!["all_columns".to_string()]));
    TestRunner::new(&config).run(|c| {
        let first = User::new().insert(DB::A).unwrap();
        assert_eq!(first.id, Some(1));
        assert_eq!(User::all(DB::A).len(), 1);
        assert_eq!(User::all(DB::B).len(), 0);
        db_compare::run(&c).unwrap();
    });
}
#[test]
fn test_sequences() {
    let config = default_config(Some(vec!["sequences".to_string()]));
    TestRunner::new(&config).run(|c| {
        let first = User::new().insert(DB::A).unwrap();
        assert_eq!(first.id, Some(1));
        assert_eq!(User::all(DB::A).len(), 1);
        assert_eq!(User::all(DB::B).len(), 0);
        db_compare::run(&c).unwrap();
    });
}
