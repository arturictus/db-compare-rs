mod common;
use common::{around, User, DB};
use db_compare::IOType;
use db_compare::*;
use std::cell::RefCell;

const REGENERATE_EXAMPLES: bool = false;

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
fn integration_test() {
    around(|| {
        let first = User::new().insert(DB::A).unwrap();
        println!("DB1: {:?}", User::all(DB::A));
        println!("DB2: {:?}", User::all(DB::B));
        assert_eq!(first.id, Some(1));
        let config = default_config(Some(vec!["counters".to_string()]));
        println!("{:?}", config);
        test_runner(&config);
        Ok(())
    });
}

use uuid::Uuid;
fn test_runner(config: &Config) {
    let tmp_file = format!("tmp/{}.diff", Uuid::new_v4());
    let fixture_file = format!(
        "tests/fixtures/examples/{}_{}_example.diff",
        config.white_listed_tables.clone().unwrap().join("_"),
        config.jobs.clone().unwrap().join("_")
    );
    let config = Config {
        diff_io: RefCell::new(IOType::new_from_path(tmp_file.clone())),
        db1: config.db1.clone(),
        db2: config.db2.clone(),
        limit: config.limit,
        tls: false,
        white_listed_tables: config.white_listed_tables.clone(),
        jobs: config.jobs.clone(),
        all_columns_sample_size: config.all_columns_sample_size,
    };
    db_compare::run(&config).unwrap();
    if REGENERATE_EXAMPLES {
        std::fs::copy(&tmp_file, &fixture_file).unwrap();
    }
    let tmp = std::fs::read_to_string(&tmp_file).unwrap();
    let fixture = std::fs::read_to_string(&fixture_file).unwrap();
    std::fs::remove_file(&tmp_file).unwrap();
    assert_eq!(fixture, tmp)
}
