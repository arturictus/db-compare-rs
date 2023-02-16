mod counter;
mod database;
mod diff;
use diff::IO;
mod last_created_records;
mod last_updated_records;
use std::{cell::RefCell, fs};
mod all_rows;
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
    db1: String,
    #[arg(long)]
    db2: String,
    #[arg(long, default_value_t = DEFAULT_LIMIT)]
    limit: u32,
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
pub struct Config<'a> {
    args: &'a Args,
    limit: u32,
    diff_io: RefCell<diff::IOType>,
    white_listed_tables: Option<Vec<String>>,
    jobs: Option<Vec<String>>,
}

fn main() -> Result<(), postgres::Error> {
    let args = Args::parse();
    let config = Config::new(&args);
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
    if config.should_run_all_rows() {
        all_rows::run(&config)?;
    }
    config.diff_io.borrow_mut().close();
    Ok(())
}

impl<'main> Config<'main> {
    pub fn new(args: &'main Args) -> Config<'main> {
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
            None
        };
        let from_file = Self::build_from_config_file(args);

        let from_args = Self {
            args,
            diff_io: if args.diff_file.is_some() {
                let diff_io: diff::IOType = diff::IO::new(args);
                RefCell::new(diff_io)
            } else {
                RefCell::new(diff::IOType::Stdout)
            },
            white_listed_tables,
            limit: args.limit,
            jobs: None,
        };

        if let Some(file_config) = from_file {
            Self::merge(file_config, from_args)
        } else {
            from_args
        }
    }

    fn build_from_config_file(args: &'main Args) -> Option<Self> {
        let config_arg = args.config.as_ref()?;
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
        let limit: u32 = match &yaml[0]["limit"] {
            yaml_rust::Yaml::BadValue => DEFAULT_LIMIT,
            data => data.as_i64().unwrap().try_into().unwrap(),
        };
        let diff_io: RefCell<diff::IOType> = if args.diff_file.is_none() {
            match &yaml[0]["diff-file"] {
                yaml_rust::Yaml::BadValue => RefCell::new(diff::IOType::Stdout),
                data => {
                    let path = diff::IO::new_from_path(data.clone().into_string());
                    RefCell::new(path)
                }
            }
        } else {
            RefCell::new(diff::IOType::Stdout)
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

        Some(Self {
            args,
            limit,
            diff_io,
            white_listed_tables,
            jobs,
        })
    }

    fn merge(old: Self, new: Self) -> Self {
        Self {
            args: new.args,
            limit: if new.limit != DEFAULT_LIMIT {
                new.limit
            } else {
                old.limit
            },
            diff_io: if new.diff_io.borrow().is_stdout() {
                old.diff_io
            } else {
                new.diff_io
            },
            white_listed_tables: if new.white_listed_tables.is_some() {
                new.white_listed_tables
            } else {
                old.white_listed_tables
            },
            jobs: if new.jobs.is_some() {
                new.jobs
            } else {
                old.jobs
            },
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
    pub fn should_run_all_rows(&self) -> bool {
        if let Some(list) = &self.jobs {
            return list.contains(&"all_rows".to_string());
        }
        true
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn default_args() -> Args {
        Args {
            db1: "postgresql://postgres:postgres@127.0.0.1/warren_development".to_string(),
            db2: "postgresql://postgres:postgres@127.0.0.1/warren_test".to_string(),
            limit: 1,
            no_tls: false,
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
        assert_eq!(config.should_run_all_rows(), true);
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
                "all_rows".to_string()
            ])
        );
        assert_eq!(config.should_run_counters(), true);
        assert_eq!(config.should_run_updated_ats(), true);
        assert_eq!(config.should_run_created_ats(), true);
        assert_eq!(config.should_run_all_rows(), true);
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
                "all_rows".to_string()
            ])
        );
        assert_eq!(config.should_run_counters(), true);
        assert_eq!(config.should_run_updated_ats(), true);
        assert_eq!(config.should_run_created_ats(), true);
        assert_eq!(config.should_run_all_rows(), true);
    }
}
