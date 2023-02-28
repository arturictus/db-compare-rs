mod common;
use std::cell::RefCell;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::path::Path;

use common::{around, User, DB};
use db_compare::IOType;
use db_compare::*;

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
        assert_eq!(first.id, Some(1));
        let config = default_config(Some(vec!["counters".to_string()]));

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
        db2: config.db1.clone(),
        limit: config.limit,
        tls: false,
        white_listed_tables: config.white_listed_tables.clone(),
        jobs: config.jobs.clone(),
        all_columns_sample_size: config.all_columns_sample_size,
    };
    db_compare::run(&config).unwrap();
    if REGENERATE_EXAMPLES {
        // if Path::new(&fixture_file).exists() {
        //     fs::remove_file(&fixture_file).unwrap();
        // }
        std::fs::copy(&tmp_file, &fixture_file).unwrap();
    }
    let tmp = std::fs::read_to_string(&tmp_file).unwrap();
    let fixture = std::fs::read_to_string(&fixture_file).unwrap();
    assert_eq!(fixture, tmp);
}

fn echo(s: &str, path: &Path) -> io::Result<()> {
    let mut f = File::create(path)?;

    f.write_all(s.as_bytes())
}

fn touch(path: &Path) -> io::Result<()> {
    let prefix = path.parent().unwrap();
    std::fs::create_dir_all(prefix).unwrap();
    match OpenOptions::new().create(true).write(true).open(path) {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
