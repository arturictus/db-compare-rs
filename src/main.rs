mod counter;
mod database;
// mod last_created_records;
// mod last_updated_records;
// mod all_columns;
use clap::Parser;
use database::DBSelector::{MasterDB, ReplicaDB};
use db_compare::diff::IO;
use db_compare::{Args, Config};
use std::{cell::RefCell, fs, time::Instant};

fn main() -> Result<(), postgres::Error> {
    let start = Instant::now();
    println!("#===Start====");
    let args = Args::parse();

    let (config, diff_io) = Config::new(&args);
    database::ping_db(&config, MasterDB)?;
    database::ping_db(&config, ReplicaDB)?;

    if config.should_run_counters() {
        counter::run(&config, &diff_io);
    }
    // if config.should_run_updated_ats() {
    //     last_updated_records::tables(&config)?;
    //     last_updated_records::only_updated_ats(&config)?;
    //     last_updated_records::all_columns(&config)?;
    // }
    // if config.should_run_created_ats() {
    //     last_created_records::tables(&config)?;
    //     last_created_records::only_created_ats(&config)?;
    //     last_created_records::all_columns(&config)?;
    // }
    // if config.should_run_all_columns() {
    //     all_columns::run(&config)?;
    // }
    diff_io.borrow_mut().close();
    println!("#===== END === took: {:?}", start.elapsed());
    Ok(())
}

// #[cfg(test)]
// mod test {
//     use super::*;

//     fn default_args() -> Args {
//         Args {
//             db1: Some("postgresql://postgres:postgres@127.0.0.1/db1".to_string()),
//             db2: Some("postgresql://postgres:postgres@127.0.0.1/db2".to_string()),
//             limit: 1,
//             no_tls: false,
//             all_columns_sample_size: None,
//             diff_file: None,
//             tables_file: None,
//             config: None,
//         }
//     }

//     #[test]
//     fn test_config_new() {
//         let args_with_listed_file = Args {
//             tables_file: Some("./tests/fixtures/whitelisted_table_example.json".to_string()),
//             ..default_args()
//         };
//         let config = Config::new(&args_with_listed_file);
//         assert_eq!(
//             config.white_listed_tables,
//             Some(vec!["table_from_tables_file".to_string()])
//         );
//         assert_eq!(config.should_run_counters(), false);
//         assert_eq!(config.should_run_updated_ats(), false);
//         assert_eq!(config.should_run_created_ats(), false);
//         assert_eq!(config.should_run_all_columns(), true);
//     }
//     #[test]
//     fn test_config_from_config_file() {
//         let args = Args {
//             limit: DEFAULT_LIMIT,
//             config: Some("./tests/fixtures/testing_config.yml".to_string()),
//             ..default_args()
//         };
//         let config = Config::new(&args);
//         assert_eq!(
//             config.white_listed_tables,
//             Some(vec!["testing_tables".to_string()])
//         );
//         assert_eq!(config.limit, 999);
//         assert_eq!(config.diff_io.borrow().is_stdout(), false);
//         assert_eq!(
//             config.jobs,
//             Some(vec![
//                 "counters".to_string(),
//                 "last_updated_ats".to_string(),
//                 "last_created_ats".to_string(),
//                 "all_columns".to_string()
//             ])
//         );
//         assert_eq!(config.should_run_counters(), true);
//         assert_eq!(config.should_run_updated_ats(), true);
//         assert_eq!(config.should_run_created_ats(), true);
//         assert_eq!(config.should_run_all_columns(), true);
//     }
//     #[test]
//     fn test_config_from_config_file_with_args() {
//         let args = Args {
//             limit: 22,
//             tables_file: Some("./tests/fixtures/whitelisted_table_example.json".to_string()),
//             config: Some("./tests/fixtures/testing_config.yml".to_string()),
//             ..default_args()
//         };
//         let config = Config::new(&args);
//         assert_eq!(
//             config.white_listed_tables,
//             Some(vec!["table_from_tables_file".to_string()])
//         );
//         assert_eq!(config.limit, 22);
//         // assert_eq!(config.diff_io.borrow().is_stdout(), false);
//         assert_eq!(
//             config.jobs,
//             Some(vec![
//                 "counters".to_string(),
//                 "last_updated_ats".to_string(),
//                 "last_created_ats".to_string(),
//                 "all_columns".to_string()
//             ])
//         );
//         assert_eq!(config.should_run_counters(), true);
//         assert_eq!(config.should_run_updated_ats(), true);
//         assert_eq!(config.should_run_created_ats(), true);
//         assert_eq!(config.should_run_all_columns(), true);
//     }
// }
