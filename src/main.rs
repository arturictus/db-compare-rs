mod counter;
mod database;
mod diff;
use diff::IO;
mod last_created_records;
mod last_updated_records;
use std::cell::RefCell;
use tokio;

use clap::Parser;

type DBsResults = (String, Vec<String>, Vec<String>);
enum DBSelector {
    Master,
    Replica,
}

#[derive(Parser, Debug, PartialEq)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long)]
    db1: String,
    #[arg(long)]
    db2: String,
    #[arg(long, default_value_t = 100)]
    limit: u32,
    #[arg(long = "no-tls")]
    no_tls: bool,
    #[arg(long = "diff-file")]
    diff_file: Option<String>,
    #[arg(long = "tables-file")]
    tables_file: Option<String>,
}
#[derive(Debug)]
pub struct Config<'a> {
    args: &'a Args,
    diff_io: RefCell<diff::IOType>,
    white_listed_tables: Option<Vec<String>>,
    db1_conn: tokio_postgres::Client,
    db2_conn: tokio_postgres::Client,
}

#[tokio::main]
async fn main() -> Result<(), postgres::Error> {
    let args = Args::parse();
    let config = Config::new(&args).await;
    database::ping_db(&config, &args.db1)?;
    database::ping_db(&config, &args.db2)?;
    counter::run(&config)?;
    last_updated_records::tables(&config)?;
    last_updated_records::only_updated_ats(&config)?;
    last_updated_records::all_columns(&config)?;
    last_created_records::tables(&config)?;
    last_created_records::only_created_ats(&config)?;
    last_created_records::all_columns(&config)?;
    config.diff_io.borrow_mut().close();
    Ok(())
}

impl<'a> Config<'a> {
    pub async fn new(args: &'a Args) -> Config<'a> {
        let diff_io: diff::IOType = diff::IO::new(args);
        let db1_conn: tokio_postgres::Client = database::connect(args, &args.db1).await.unwrap();
        let db2_conn: tokio_postgres::Client = database::connect(args, &args.db1).await.unwrap();
        if let Some(file_path) = &args.tables_file {
            let value = {
                let text = std::fs::read_to_string(file_path)
                    .unwrap_or_else(|_| panic!("unable to read file at: {file_path}"));

                serde_json::from_str::<Vec<String>>(&text).unwrap_or_else(|_| {
                    panic!("malformed json file at: {file_path}, expected list with strings ex: [\"users\"]")
                })
            };
            Self {
                args,
                diff_io: RefCell::new(diff_io),
                white_listed_tables: Some(value),
                db1_conn,
                db2_conn,
            }
        } else {
            Self {
                args,
                diff_io: RefCell::new(diff_io),
                white_listed_tables: None,
                db1_conn,
                db2_conn,
            }
        }
    }
    pub fn db_url_shortener(&self, db_url: &str) -> String {
        if db_url == self.args.db1 {
            "DB1".to_string()
        } else {
            "DB2".to_string()
        }
    }
}

impl DBSelector {
    fn conn(&self, config: &Config) -> tokio_postgres::Client {
        match self {
            Self::Master => config.db1_conn,
            _ => config.db2_conn,
        }
    }

    fn name(&self) -> String {
        match self {
            Self::Master => "DB1".to_string(),
            _ => "DB2".to_string(),
        }
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
        }
    }

    #[test]
    fn test_config_new() {
        let args_with_listed_file = Args {
            tables_file: Some("./tests/fixtures/whitelisted_table_example.json".to_string()),
            ..default_args()
        };

        assert_eq!(
            Config::new(&args_with_listed_file).white_listed_tables,
            Some(vec!["users".to_string()])
        );
    }
}
