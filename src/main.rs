mod counter;
mod database;
mod diff;
use diff::IO;
mod last_created_records;
mod last_updated_records;

use clap::Parser;

type DBsResult = (String, Vec<String>, Vec<String>);
type Diff = (String, String);

#[derive(Parser, Debug, PartialEq)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long)]
    db1: String,
    #[arg(long)]
    db2: String,
    #[arg(long, default_value_t = 100)]
    limit: u32,
    #[arg(long = "tls", default_value_t = true)]
    tls: bool,
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
    let mut out: diff::IOType = diff::IO::new(&args);
    let internal_config = Config::new(&args);
    database::ping_db(&internal_config, &args.db1)?;
    database::ping_db(&internal_config, &args.db2)?;
    counter::run(&internal_config, &mut out)?;
    last_updated_records::tables(&internal_config, &mut out)?;
    last_updated_records::only_updated_ats(&internal_config, &mut out)?;
    last_updated_records::all_columns(&internal_config, &mut out)?;
    last_created_records::tables(&internal_config, &mut out)?;
    last_created_records::only_created_ats(&internal_config, &mut out)?;
    last_created_records::all_columns(&internal_config, &mut out)?;
    out.close();
    Ok(())
}

impl<'a> Config<'a> {
    pub fn new(args: &'a Args) -> Config<'a> {
        if let Some(file_path) = &args.tables_file {
            let value = {
                // Load the first file into a string.
                let text = std::fs::read_to_string(file_path)
                    .unwrap_or_else(|_| panic!("unable to read file at: {file_path}"));

                // Parse the string
                serde_json::from_str::<Vec<String>>(&text).unwrap_or_else(|_| {
                    panic!("malformed json file at: {file_path}, expected list with strings")
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
}

fn db_url_shortener(config: &Config, db_url: &str) -> String {
    if db_url == config.args.db1 {
        "DB1".to_string()
    } else {
        "DB2".to_string()
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
