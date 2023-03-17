mod common;
use common::{Msg, TestRunner, User, DB};
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
    TestRunner::new(&config).run("db1 has one record more than db2", |c| {
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
    TestRunner::new(&config).run("db1 has one record more than db2", |c| {
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
    TestRunner::new(&config).run("db1 has one record more than db2", |c| {
        User::new().insert(DB::A).unwrap();
        let first = Msg::new().insert(DB::A).unwrap();
        assert_eq!(first.id, Some(1));
        assert_eq!(Msg::all(DB::A).len(), 1);
        assert_eq!(Msg::all(DB::B).len(), 0);
        assert_eq!(User::all(DB::A).len(), 1);
        assert_eq!(User::all(DB::B).len(), 0);
        db_compare::run(c).unwrap();
    });
}

#[test]
fn test_all_columns() {
    let config = default_config(vec![Job::AllColumns]);
    TestRunner::new(&config).run("db1 has one record more than db2", |c| {
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
    TestRunner::new(&config).run("db1 has one record more than db2", |c| {
        let first = User::new().insert(DB::A).unwrap();
        assert_eq!(first.id, Some(1));
        assert_eq!(User::all(DB::A).len(), 1);
        assert_eq!(User::all(DB::B).len(), 0);
        db_compare::run(c).unwrap();
    });
}
#[test]
fn test_updated_ats_until() {
    let mut config = default_config(vec![Job::UpdatedAtsUntil]);

    let (updated_at, _) = (1..=10).fold((config.rows_until, User::new()), |(_tm, u), _i| {
        let u2 = u.next();
        (u2.updated_at, u2)
    });

    config.rows_until = updated_at;
    config.limit = 2;

    TestRunner::new(&config).run("db1 has more records than db2", |c| {
        (1..=10).fold(User::new(), |prev, _i| {
            let u = prev.next();
            u.insert(DB::A).unwrap();
            u
        });
        assert_eq!(User::all(DB::A).len(), 10);
        assert_eq!(User::all(DB::B).len(), 0);
        db_compare::run(c).unwrap();
    });

    config.limit = 1;

    TestRunner::new(&config).run("db1 has more records than db2 limit 1", |c| {
        (1..=10).fold(User::new(), |prev, _i| {
            let u = prev.next();
            u.insert(DB::A).unwrap();
            u
        });
        assert_eq!(User::all(DB::A).len(), 10);
        assert_eq!(User::all(DB::B).len(), 0);
        db_compare::run(c).unwrap();
    });
}