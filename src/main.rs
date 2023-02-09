mod counter;
mod database;
mod diff_formatter;
mod last_created_records;
mod last_updated_records;
mod presenter;
use presenter::{Presenter, PresenterAbstract};

use clap::Parser;

type DBsResult = (String, Vec<String>, Vec<String>);
type Diff = (String, String);

#[derive(Parser, Debug)]
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

pub struct Config<'a> {
    args: &'a Args,
    white_listed_tables: Option<Vec<String>>,
}

fn main() -> Result<(), postgres::Error> {
    let args = Args::parse();
    let mut out = Presenter::new(&args);
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
        if let Some(_file_path) = &args.tables_file {
            Self {
                args,
                white_listed_tables: None,
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

    #[test]
    fn test_generate_config() {}
}
