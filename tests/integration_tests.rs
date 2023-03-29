mod common;
use std::ops::Add;

use chrono::{Days, NaiveDateTime};
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
    let (users, _) = generate_users(20);
    let (msgs, _) = generate_msgs(20);

    TestRunner::new(&config).run("db1 has one record more than db2", |c| {
        seed_test_data(Some(&users), Some(&msgs));

        db_compare::run(c).unwrap();
    });
}
#[test]
fn test_updated_ats() {
    let config = default_config(vec![Job::UpdatedAts]);
    let (users, _) = generate_users(20);
    let (msgs, _) = generate_msgs(20);

    TestRunner::new(&config).run("db1 has one record more than db2", |c| {
        seed_test_data(Some(&users), Some(&msgs));
        db_compare::run(c).unwrap();
    });
}
#[test]
fn test_created_ats() {
    let config = default_config(vec![Job::CreatedAts]);
    let (users, _) = generate_users(20);
    let (msgs, _) = generate_msgs(20);

    TestRunner::new(&config).run("db1 has one record more than db2", |c| {
        seed_test_data(Some(&users), Some(&msgs));
        db_compare::run(c).unwrap();
    });
}

#[test]
fn test_all_columns() {
    let config = default_config(vec![Job::AllColumns]);
    let (users, _) = generate_users(20);
    let (msgs, _) = generate_msgs(20);

    TestRunner::new(&config).run("db1 has one record more than db2", move |c| {
        seed_test_data(Some(&users), Some(&msgs));
        db_compare::run(c).unwrap();
    });
}
#[test]
fn test_sequences() {
    let config = default_config(vec![Job::Sequences]);
    let (users, _) = generate_users(20);
    let (msgs, _) = generate_msgs(20);
    TestRunner::new(&config).run("db1 has one record more than db2", move |c| {
        seed_test_data(Some(&users), Some(&msgs));
        db_compare::run(c).unwrap();
    });
}
#[test]
fn test_updated_ats_until() {
    let mut config = default_config(vec![Job::UpdatedAtsUntil]);

    let (users, updated_at) = generate_users(20);
    let (msgs, _) = generate_msgs(20);

    config.rows_until = updated_at.add(Days::new(10));
    config.limit = 2;

    TestRunner::new(&config).run("db1 has more records than db2", move |c| {
        seed_test_data(Some(&users), Some(&msgs));
        db_compare::run(c).unwrap();
    });
}

#[test]
fn test_updated_ats_until_limit_1() {
    let mut config = default_config(vec![Job::UpdatedAtsUntil]);

    let (users, updated_at) = generate_users(20);
    let (msgs, _) = generate_msgs(20);

    config.rows_until = updated_at.add(Days::new(10));
    config.limit = 1;
    TestRunner::new(&config).run("db1 has more records than db2 limit 1", move |c| {
        seed_test_data(Some(&users), Some(&msgs));
        db_compare::run(c).unwrap();
    });
}
#[test]
fn test_updated_ats_until_limit_2() {
    let mut config = default_config(vec![Job::UpdatedAtsUntil]);
    let (users, updated_at) = generate_users(20);
    let (msgs, _) = generate_msgs(20);

    config.rows_until = updated_at.add(Days::new(10));
    config.limit = 2;

    TestRunner::new(&config).run("db1 has more records than db2 limit 2", |c| {
        seed_test_data(Some(&users), Some(&msgs));
        db_compare::run(c).unwrap();
    });
}
#[test]
fn test_updated_ats_until_limit_5() {
    let mut config = default_config(vec![Job::UpdatedAtsUntil]);
    let (users, updated_at) = generate_users(20);
    let (msgs, _) = generate_msgs(20);

    config.rows_until = updated_at.add(Days::new(10));
    config.limit = 5;

    TestRunner::new(&config).run("db1 has more records than db2 limit 5", |c| {
        seed_test_data(Some(&users), Some(&msgs));
        db_compare::run(c).unwrap();
    });
}

fn generate_users(amount: u32) -> (Vec<User>, NaiveDateTime) {
    let first = User::new();
    let (_u, t, acc) = (1..=amount).fold(
        (first.clone(), first.updated_at, vec![first]),
        |(u, _t, mut acc), _i| {
            let u = u.next();
            let t = u.updated_at;
            acc.push(u.clone());
            (u, t, acc)
        },
    );

    (acc, t)
}
fn generate_msgs(amount: u32) -> (Vec<Msg>, NaiveDateTime) {
    let first = Msg::new();
    let (_u, t, acc) = (1..=amount).fold(
        (first.clone(), first.created_at, vec![first]),
        |(u, _t, mut acc), _i| {
            let u = u.next();
            let t = u.created_at;
            acc.push(u.clone());
            (u, t, acc)
        },
    );

    (acc, t)
}
fn seed_test_data(users: Option<&Vec<User>>, msgs: Option<&Vec<Msg>>) {
    if let Some(users) = users {
        for (i, u) in users.iter().enumerate() {
            let u = u.insert(DB::A).unwrap();
            if i % 2 == 0 {
                u.insert(DB::B).unwrap();
            }
            if i % 3 == 0 {
                User {
                    name: format!("{} changed", u.name.clone()),
                    ..u.clone()
                }
                .insert(DB::B)
                .unwrap();
            }
        }
        users.last().unwrap().next().insert(DB::B).unwrap();
    }
    if let Some(msgs) = msgs {
        for (i, msg) in msgs.iter().enumerate() {
            let msg = msg.insert(DB::A).unwrap();
            if i % 2 == 0 {
                msg.insert(DB::B).unwrap();
            }
            if i % 3 == 0 {
                Msg {
                    txt: format!("{} changed", msg.txt.clone()),
                    ..msg.clone()
                }
                .insert(DB::B)
                .unwrap();
            }
        }
        msgs.last().unwrap().next().insert(DB::B).unwrap();
    }
}
