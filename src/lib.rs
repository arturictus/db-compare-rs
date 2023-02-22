use clap::Parser;
use std::{cell::RefCell, fs};
extern crate yaml_rust;
use yaml_rust::YamlLoader;
pub mod counter;
pub mod database;
pub mod diff;

// pub use diff;
// mod last_created_records;
// mod last_updated_records;
// mod all_columns;

type DBsResults = (String, Vec<String>, Vec<String>);
const DEFAULT_LIMIT: u32 = 100;

#[derive(Parser, Debug, PartialEq, Default)]
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
#[derive(Debug, Clone)]
pub struct Config {
    pub db1: String,
    pub db2: String,
    pub tls: bool,
    pub limit: u32,
    // diff_io: RefCell<diff::IOType>,
    pub white_listed_tables: Option<Vec<String>>,
    pub jobs: Option<Vec<String>>,
    pub all_columns_sample_size: Option<u32>,
}

impl Config {
    pub fn new(args: &Args) -> (Config, RefCell<diff::IOType>) {
        let config_file = ConfigFile::build(&args);
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

        // let diff_io = if args.diff_file.is_some() {
        //     let diff_io: diff::IOType = diff::IO::new(args);
        //     RefCell::new(diff_io)
        // } else {
        //     match config_file.diff_file {
        //         Some(file_path) => {
        //             let path = diff::IO::new_from_path(file_path);
        //             RefCell::new(path)
        //         }
        //         _ => RefCell::new(diff::IOType::Stdout),
        //     }
        // };
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
        let diff_io = if args.diff_file.is_some() {
            let diff_io: diff::IOType = diff::IO::new(&args);
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

        (
            Self {
                db1,
                db2,
                white_listed_tables,
                limit,
                jobs: config_file.jobs,
                all_columns_sample_size,
                tls: !args.no_tls,
            },
            diff_io,
        )
    }

    pub fn should_run_counters(&self) -> bool {
        if let Some(list) = &self.jobs {
            return list.contains(&"counters".to_string());
        }
        // FIX:
        true
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
