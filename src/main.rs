mod counter;
mod database;
mod diff;
use diff::IO;
mod last_created_records;
mod last_updated_records;

use clap::Parser;

type DBsResults = (String, Vec<String>, Vec<String>);

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
#[derive(PartialEq, Debug)]
pub struct Config<'a> {
    args: &'a Args,
    white_listed_tables: Option<Vec<String>>,
}

fn main() -> Result<(), postgres::Error> {
    let args = Args::parse();
    println!("{:?}", args);
    let mut out: diff::IOType = diff::IO::new(&args);
    let config = Config::new(&args);
    database::ping_db(&config, &args.db1)?;
    database::ping_db(&config, &args.db2)?;
    counter::run(&config, &mut out)?;
    last_updated_records::tables(&config, &mut out)?;
    last_updated_records::only_updated_ats(&config, &mut out)?;
    last_updated_records::all_columns(&config, &mut out)?;
    last_created_records::tables(&config, &mut out)?;
    last_created_records::only_created_ats(&config, &mut out)?;
    last_created_records::all_columns(&config, &mut out)?;
    out.close();
    Ok(())
}

impl<'a> Config<'a> {
    pub fn new(args: &'a Args) -> Config<'a> {
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
                white_listed_tables: Some(value),
            }
        } else {
            Self {
                args,
                white_listed_tables: None,
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

#[cfg(test)]
mod test {
    use super::*;

    fn default_args() -> Args {
        Args {
            db1: "postgresql://postgres:postgres@127.0.0.1/db1".to_string(),
            db2: "postgresql://postgres:postgres@127.0.0.1/db2".to_string(),
            limit: 1,
            tls: false,
            diff_file: None,
            tables_file: None,
        }
    }

    #[test]
    fn test_config_new() {
        let args = default_args();
        assert_eq!(
            Config::new(&args),
            Config {
                args: &args,
                white_listed_tables: None
            }
        );
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
