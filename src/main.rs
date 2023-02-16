mod counter;
mod database;
mod diff;
use diff::IO;
mod last_created_records;
mod last_updated_records;
use std::{cell::RefCell, fs};
mod all_columns;
use clap::Parser;
use database::DBSelector::{MasterDB, ReplicaDB};
extern crate yaml_rust;
use yaml_rust::YamlLoader;

type DBsResults = (String, Vec<String>, Vec<String>);
const DEFAULT_LIMIT: u32 = 100;

#[derive(Parser, Debug, PartialEq)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long)]
    db1: Option<String>,
    #[arg(long)]
    db2: Option<String>,
    #[arg(long, default_value_t = DEFAULT_LIMIT)]
    limit: u32,
    #[arg(long = "all-columns-sample-size")]
    all_columns_sample_size: Option<u32>,
    #[arg(long = "no-tls")]
    no_tls: bool,
    #[arg(long = "diff-file")]
    diff_file: Option<String>,
    #[arg(long = "tables-file")]
    tables_file: Option<String>,
    #[arg(long, short)]
    config: Option<String>,
}
#[derive(Debug)]
pub struct Config {
    db1: String,
    db2: String,
    tls: bool,
    limit: u32,
    diff_io: RefCell<diff::IOType>,
    white_listed_tables: Option<Vec<String>>,
    jobs: Option<Vec<String>>,
    all_columns_sample_size: Option<u32>,
}

fn main() -> Result<(), postgres::Error> {
    let args = Args::parse();
    let config = Config::new(&args);
    println!("{:?}", config);
    // panic!();
    database::ping_db(&config, MasterDB)?;
    database::ping_db(&config, ReplicaDB)?;

    if config.should_run_counters() {
        counter::run(&config)?;
    }
    if config.should_run_updated_ats() {
        last_updated_records::tables(&config)?;
        last_updated_records::only_updated_ats(&config)?;
        last_updated_records::all_columns(&config)?;
    }
    if config.should_run_created_ats() {
        last_created_records::tables(&config)?;
        last_created_records::only_created_ats(&config)?;
        last_created_records::all_columns(&config)?;
    }
    if config.should_run_all_columns() {
        all_columns::run(&config)?;
    }
    config.diff_io.borrow_mut().close();
    Ok(())
}

impl Config {
    pub fn new(args: &Args) -> Config {
        let config_file = ConfigFile::build(args);

        let db1 = if let Some(db_url) = args.db1.clone() {
            db_url
        } else {
            config_file
                .db1
                .unwrap_or_else(|| panic!("Missing `db1` argument or attribute in config file"))
        };
        let db2 = if let Some(db_url) = args.db2.clone() {
            db_url
        } else {
            config_file
                .db2
                .unwrap_or_else(|| panic!("Missing `db2` argument or attribute in config file"))
        };

        let white_listed_tables = if let Some(file_path) = &args.tables_file {
            let value = {
                let text = std::fs::read_to_string(file_path)
                    .unwrap_or_else(|_| panic!("unable to read file at: {file_path}"));

                serde_json::from_str::<Vec<String>>(&text).unwrap_or_else(|_| {
                    panic!("malformed json file at: {file_path}, expected list with strings ex: [\"users\"]")
                })
            };
            Some(value)
        } else {
            config_file.white_listed_tables
        };

        let diff_io = if args.diff_file.is_some() {
            let diff_io: diff::IOType = diff::IO::new(args);
            RefCell::new(diff_io)
        } else {
            match config_file.diff_file {
                Some(file_path) => {
                    let path = diff::IO::new_from_path(file_path);
                    RefCell::new(path)
                }
                _ => RefCell::new(diff::IOType::Stdout),
            }
        };
        let limit = if args.limit != DEFAULT_LIMIT {
            args.limit
        } else {
            match config_file.limit {
                Some(limit) => limit,
                _ => DEFAULT_LIMIT,
            }
        };

        let all_columns_sample_size = if args.all_columns_sample_size.is_some() {
            args.all_columns_sample_size
        } else {
            config_file.all_columns_sample_size
        };

        Self {
            db1,
            db2,
            diff_io,
            white_listed_tables,
            limit,
            jobs: config_file.jobs,
            all_columns_sample_size,
            tls: !args.no_tls,
        }
    }

    pub fn should_run_counters(&self) -> bool {
        if let Some(list) = &self.jobs {
            return list.contains(&"counters".to_string());
        }
        false
    }
    pub fn should_run_updated_ats(&self) -> bool {
        if let Some(list) = &self.jobs {
            return list.contains(&"last_updated_ats".to_string());
        }
        false
    }
    pub fn should_run_created_ats(&self) -> bool {
        if let Some(list) = &self.jobs {
            return list.contains(&"last_created_ats".to_string());
        }
        false
    }
    pub fn should_run_all_columns(&self) -> bool {
        if let Some(list) = &self.jobs {
            return list.contains(&"all_columns".to_string());
        }
        true
    }
}

#[derive(Debug, Clone)]
struct ConfigFile {
    db1: Option<String>,
    db2: Option<String>,
    limit: Option<u32>,
    diff_file: Option<String>,
    white_listed_tables: Option<Vec<String>>,
    jobs: Option<Vec<String>>,
    all_columns_sample_size: Option<u32>,
}

impl ConfigFile {
    fn build(args: &Args) -> Self {
        if let Some(config_arg) = args.config.as_ref() {
            let file_path = config_arg;
            let data = fs::read_to_string(file_path)
                .unwrap_or_else(|_| panic!("file not found for config argument: {file_path}"));
            let yaml = YamlLoader::load_from_str(&data)
                .unwrap_or_else(|_| panic!("Unable to parse yaml config file at: {file_path}"));
            let white_listed_tables: Option<Vec<String>> = match &yaml[0]["tables"] {
                yaml_rust::Yaml::BadValue => None,
                data => Some(
                    data.as_vec()
                        .unwrap()
                        .iter()
                        .map(|e| e.clone().into_string().unwrap())
                        .collect(),
                ),
            };
            let limit: Option<u32> = match &yaml[0]["limit"] {
                yaml_rust::Yaml::BadValue => None,
                data => Some(data.as_i64().unwrap().try_into().unwrap()),
            };
            let diff_file: Option<String> = match &yaml[0]["diff-file"] {
                yaml_rust::Yaml::BadValue => None,
                data => data.clone().into_string(),
            };
            let jobs = match &yaml[0]["jobs"] {
                yaml_rust::Yaml::BadValue => None,
                data => Some(
                    data.as_vec()
                        .unwrap()
                        .iter()
                        .map(|e| e.clone().into_string().unwrap())
                        .collect(),
                ),
            };
            let all_columns_sample_size: Option<u32> = match &yaml[0]["all-columns-sample-size"] {
                yaml_rust::Yaml::BadValue => None,
                data => Some(data.as_i64().unwrap().try_into().unwrap()),
            };
            let db1: Option<String> = match &yaml[0]["db1"] {
                yaml_rust::Yaml::BadValue => None,
                data => Some(data.clone().into_string().unwrap()),
            };
            let db2: Option<String> = match &yaml[0]["db2"] {
                yaml_rust::Yaml::BadValue => None,
                data => Some(data.clone().into_string().unwrap()),
            };

            Self {
                db1,
                db2,
                limit,
                diff_file,
                white_listed_tables,
                jobs,
                all_columns_sample_size,
            }
        } else {
            Self {
                db1: None,
                db2: None,
                limit: None,
                diff_file: None,
                white_listed_tables: None,
                jobs: None,
                all_columns_sample_size: None,
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn default_args() -> Args {
        Args {
            db1: Some("postgresql://postgres:postgres@127.0.0.1/db1".to_string()),
            db2: Some("postgresql://postgres:postgres@127.0.0.1/db2".to_string()),
            limit: 1,
            no_tls: false,
            all_columns_sample_size: None,
            diff_file: None,
            tables_file: None,
            config: None,
        }
    }

    #[test]
    fn test_config_new() {
        let args_with_listed_file = Args {
            tables_file: Some("./tests/fixtures/whitelisted_table_example.json".to_string()),
            ..default_args()
        };
        let config = Config::new(&args_with_listed_file);
        assert_eq!(
            config.white_listed_tables,
            Some(vec!["table_from_tables_file".to_string()])
        );
        assert_eq!(config.should_run_counters(), false);
        assert_eq!(config.should_run_updated_ats(), false);
        assert_eq!(config.should_run_created_ats(), false);
        assert_eq!(config.should_run_all_columns(), true);
    }
    #[test]
    fn test_config_from_config_file() {
        let args = Args {
            limit: DEFAULT_LIMIT,
            config: Some("./tests/fixtures/testing_config.yml".to_string()),
            ..default_args()
        };
        let config = Config::new(&args);
        assert_eq!(
            config.white_listed_tables,
            Some(vec!["testing_tables".to_string()])
        );
        assert_eq!(config.limit, 999);
        assert_eq!(config.diff_io.borrow().is_stdout(), false);
        assert_eq!(
            config.jobs,
            Some(vec![
                "counters".to_string(),
                "last_updated_ats".to_string(),
                "last_created_ats".to_string(),
                "all_columns".to_string()
            ])
        );
        assert_eq!(config.should_run_counters(), true);
        assert_eq!(config.should_run_updated_ats(), true);
        assert_eq!(config.should_run_created_ats(), true);
        assert_eq!(config.should_run_all_columns(), true);
    }
    #[test]
    fn test_config_from_config_file_with_args() {
        let args = Args {
            limit: 22,
            tables_file: Some("./tests/fixtures/whitelisted_table_example.json".to_string()),
            config: Some("./tests/fixtures/testing_config.yml".to_string()),
            ..default_args()
        };
        let config = Config::new(&args);
        assert_eq!(
            config.white_listed_tables,
            Some(vec!["table_from_tables_file".to_string()])
        );
        assert_eq!(config.limit, 22);
        assert_eq!(config.diff_io.borrow().is_stdout(), false);
        assert_eq!(
            config.jobs,
            Some(vec![
                "counters".to_string(),
                "last_updated_ats".to_string(),
                "last_created_ats".to_string(),
                "all_columns".to_string()
            ])
        );
        assert_eq!(config.should_run_counters(), true);
        assert_eq!(config.should_run_updated_ats(), true);
        assert_eq!(config.should_run_created_ats(), true);
        assert_eq!(config.should_run_all_columns(), true);
    }
}
