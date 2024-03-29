mod cli;
mod database;
mod diff;
use chrono::NaiveDateTime;
pub use cli::{Cli, Commands};
use database::RequestBuilder;
pub use diff::{IOType, Summary, IO};

use std::{cell::RefCell, error, fs, str::FromStr};
extern crate yaml_rust;
use yaml_rust::YamlLoader;
mod jobs;

use itertools::Itertools;
pub use jobs::Job;

const DEFAULT_LIMIT: u32 = 100;

#[derive(Debug, Default)]
pub struct Config {
    pub db1: String,
    pub db2: String,
    pub tls: bool,
    pub limit: u32,
    pub diff_io: RefCell<diff::IOType>,
    pub white_listed_tables: Option<Vec<String>>,
    pub jobs: Vec<Job>,
    pub by_id_sample_size: Option<u32>,
    pub tm_cutoff: NaiveDateTime,
    pub output_folder: String,
    pub test_env: bool,
}

pub fn run_summary(file: &str) -> Result<(), Box<dyn error::Error>> {
    for sum in Summary::from_file(file) {
        sum.print();
    }
    Ok(())
}
pub fn run(config: &Config) -> Result<(), Box<dyn error::Error>> {
    database::ping_db(RequestBuilder::new(config).build_master())?;
    database::ping_db(RequestBuilder::new(config).build_replica())?;
    jobs::run(config)?;
    config.diff_io.borrow_mut().close();
    Ok(())
}

impl Config {
    pub fn new(args: &cli::Commands) -> Config {
        match args {
            cli::Commands::Run {
                db1: args_db1,
                db2: args_db2,
                limit: args_limit,
                by_id_sample_size: args_by_id_sample_size,
                no_tls: args_no_tls,
                output_folder: args_output_folder,
                tables: args_tables,
                jobs: args_jobs,
                tm_cutoff: args_tm_cutoff,
                ..
            } => {
                let config_file = ConfigFile::build(args);

                let db1 = if let Some(db_url) = args_db1.clone() {
                    db_url
                } else {
                    config_file.db1.unwrap_or_else(|| {
                        panic!("Missing `db1` argument or attribute in config file")
                    })
                };
                let db2 = if let Some(db_url) = args_db2.clone() {
                    db_url
                } else {
                    config_file.db2.unwrap_or_else(|| {
                        panic!("Missing `db2` argument or attribute in config file")
                    })
                };

                let white_listed_tables = if let Some(tables) = &args_tables {
                    let value = {
                        let mut v = vec![];
                        for table in tables.split(',') {
                            v.push(table.trim().to_string());
                        }
                        v
                    };
                    Some(value)
                } else {
                    config_file.white_listed_tables
                };

                let output_folder = if args_output_folder.is_some() {
                    args_output_folder.clone().unwrap()
                } else {
                    match config_file.output_folder {
                        Some(folder) => folder,
                        _ => "./diffs".to_string(),
                    }
                };

                let limit = if *args_limit != DEFAULT_LIMIT {
                    *args_limit
                } else {
                    match config_file.limit {
                        Some(limit) => limit,
                        _ => DEFAULT_LIMIT,
                    }
                };

                let by_id_sample_size = if args_by_id_sample_size.is_some() {
                    *args_by_id_sample_size
                } else {
                    config_file.by_id_sample_size
                };

                let tm_cutoff = if let Some(tm) = *args_tm_cutoff {
                    NaiveDateTime::from_timestamp_opt(tm, 0).unwrap()
                } else {
                    NaiveDateTime::from_timestamp_opt(chrono::offset::Utc::now().timestamp(), 0)
                        .unwrap()
                };
                let jobs = if let Some(jobs) = &args_jobs {
                    let mut value = vec![];
                    for job in jobs.split(',') {
                        value.push(Job::from_str(job.trim()).unwrap());
                    }
                    value
                } else if let Some(jobs) = config_file.jobs {
                    jobs
                } else {
                    Job::default_list()
                };

                Self {
                    db1,
                    db2,
                    output_folder,
                    white_listed_tables,
                    limit,
                    jobs,
                    by_id_sample_size,
                    tm_cutoff,
                    tls: !args_no_tls,
                    test_env: false,
                    diff_io: RefCell::new(diff::IOType::default()),
                }
            }
            cli::Commands::Summarize { .. } => {
                panic!("summarize command does not require config")
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ConfigFile {
    db1: Option<String>,
    db2: Option<String>,
    limit: Option<u32>,
    white_listed_tables: Option<Vec<String>>,
    jobs: Option<Vec<Job>>,
    by_id_sample_size: Option<u32>,
    output_folder: Option<String>,
}

impl ConfigFile {
    fn build(args: &cli::Commands) -> Self {
        match args {
            cli::Commands::Run {
                config: args_config,
                ..
            } => {
                if args_config.is_none() {
                    return Self::default();
                }

                let config_arg = args_config.as_ref().unwrap();
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
                let output_folder: Option<String> = match &yaml[0]["output-folder"] {
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
                let by_id_sample_size: Option<u32> = match &yaml[0]["by-id-sample-size"] {
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
                    output_folder,
                    white_listed_tables,
                    jobs,
                    by_id_sample_size,
                }
            }
            cli::Commands::Summarize { .. } => {
                panic!("summarize command does not require config file")
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_config_new() {
        let args_with_listed_file = cli::Commands::Run {
            tables: Some("users, collections".to_string()),
            db1: Some("postgresql://postgres:postgres@127.0.0.1/db1".to_string()),
            db2: Some("postgresql://postgres:postgres@127.0.0.1/db2".to_string()),
            limit: 1,
            no_tls: false,
            by_id_sample_size: None,
            jobs: None,
            config: None,
            tm_cutoff: None,
            output_folder: None,
        };
        let config = Config::new(&args_with_listed_file);
        assert_eq!(
            config.white_listed_tables,
            Some(vec!["users".to_string(), "collections".to_string()])
        );
    }
    #[test]
    fn test_config_from_config_file() {
        let args = cli::Commands::Run {
            limit: DEFAULT_LIMIT,
            config: Some("./tests/fixtures/testing_config.yml".to_string()),
            db1: Some("postgresql://postgres:postgres@127.0.0.1/db1".to_string()),
            db2: Some("postgresql://postgres:postgres@127.0.0.1/db2".to_string()),
            no_tls: false,
            by_id_sample_size: None,
            jobs: None,
            tables: None,
            tm_cutoff: None,
            output_folder: None,
        };
        let config = Config::new(&args);
        assert_eq!(
            config.white_listed_tables,
            Some(vec!["testing_tables".to_string()])
        );
        assert_eq!(config.limit, 999);
        assert_eq!(config.jobs, Job::all());
    }
    #[test]
    fn test_config_from_config_file_with_args() {
        let args = cli::Commands::Run {
            limit: 22,
            tables: Some("table_from_args".to_string()),
            config: Some("./tests/fixtures/testing_config.yml".to_string()),
            db1: Some("postgresql://postgres:postgres@127.0.0.1/db1".to_string()),
            db2: Some("postgresql://postgres:postgres@127.0.0.1/db2".to_string()),
            no_tls: false,
            by_id_sample_size: None,
            jobs: None,
            tm_cutoff: None,
            output_folder: None,
        };
        let config = Config::new(&args);
        assert_eq!(
            config.white_listed_tables,
            Some(vec!["table_from_args".to_string()])
        );
        assert_eq!(config.limit, 22);
        assert_eq!(config.jobs, Job::all());
    }
}
