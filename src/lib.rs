mod database;
mod diff;
use clap::Parser;
use database::RequestBuilder;
pub use diff::{IOType, IO};
use std::{cell::RefCell, error, fs, str::FromStr};
extern crate yaml_rust;
use yaml_rust::YamlLoader;
mod jobs;
use itertools::Itertools;
pub use jobs::Job;

type DBsResults = (String, Vec<String>, Vec<String>);
const DEFAULT_LIMIT: u32 = 100;

#[derive(Parser, Debug, PartialEq)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long)]
    pub db1: Option<String>,
    #[arg(long)]
    pub db2: Option<String>,
    #[arg(long, default_value_t = DEFAULT_LIMIT)]
    pub limit: u32,
    #[arg(long = "all-columns-sample-size")]
    pub all_columns_sample_size: Option<u32>,
    #[arg(long = "no-tls")]
    pub no_tls: bool,
    #[arg(long = "diff-file")]
    pub diff_file: Option<String>,
    #[arg(long = "tables-file")]
    pub tables_file: Option<String>,
    #[arg(long, short, help = "Yaml config file")]
    pub config: Option<String>,
}
#[derive(Debug)]
pub struct Config {
    pub db1: String,
    pub db2: String,
    pub tls: bool,
    pub limit: u32,
    pub diff_io: RefCell<diff::IOType>,
    pub white_listed_tables: Option<Vec<String>>,
    pub jobs: Vec<Job>,
    pub all_columns_sample_size: Option<u32>,
}

pub fn run(config: &Config) -> Result<(), Box<dyn error::Error>> {
    database::ping_db(RequestBuilder::new(config).build_master())?;
    database::ping_db(RequestBuilder::new(config).build_replica())?;
    jobs::run(config)?;
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
            jobs: if let Some(jobs) = config_file.jobs {
                jobs
            } else {
                Job::all()
            },
            all_columns_sample_size,
            tls: !args.no_tls,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ConfigFile {
    db1: Option<String>,
    db2: Option<String>,
    limit: Option<u32>,
    diff_file: Option<String>,
    white_listed_tables: Option<Vec<String>>,
    jobs: Option<Vec<Job>>,
    all_columns_sample_size: Option<u32>,
}

impl ConfigFile {
    fn build(args: &Args) -> Self {
        if args.config.is_none() {
            return Self::default();
        }

        let config_arg = args.config.as_ref().unwrap();
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
                    .unique()
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
        let jobs: Option<Vec<Job>> = match &yaml[0]["jobs"] {
            yaml_rust::Yaml::BadValue => None,
            data => Some(
                data.as_vec()
                    .unwrap()
                    .iter()
                    .map(|e| {
                        let s = e.clone().into_string().unwrap();
                        Job::from_str(&s).unwrap()
                    })
                    .collect(), // .collect::<Job>(),
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
        assert!(!config.diff_io.borrow().is_stdout());
        assert_eq!(config.jobs, Job::all());
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
        assert!(!config.diff_io.borrow().is_stdout());
        assert_eq!(config.jobs, Job::all());
    }
}
