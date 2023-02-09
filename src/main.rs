mod counter;
mod database;
mod diff_formatter;
mod last_created_records;
mod last_updated_records;
mod presenter;
use presenter::Presenter;

use clap::Parser;

type DBsResult = (String, Vec<String>, Vec<String>);
type Diff = (String, String);

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct CliArgs {
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

pub struct InternalArgs<'a> {
    cli_args: &'a CliArgs,
    white_listed_tables: Option<Vec<String>>,
}

fn main() -> Result<(), postgres::Error> {
    let args = CliArgs::parse();
    let mut out = Presenter::new(&args);
    let internal_args = InternalArgs::new(&args);
    database::ping_db(&internal_args, &args.db1)?;
    database::ping_db(&internal_args, &args.db2)?;
    counter::run(&internal_args, &mut out)?;
    last_updated_records::tables(&internal_args, &mut out)?;
    last_updated_records::only_updated_ats(&internal_args, &mut out)?;
    last_updated_records::all_columns(&internal_args, &mut out)?;
    last_created_records::tables(&internal_args, &mut out)?;
    last_created_records::only_created_ats(&internal_args, &mut out)?;
    last_created_records::all_columns(&internal_args, &mut out)?;
    out.end();
    Ok(())
}

impl<'a> InternalArgs<'a> {
    pub fn new(cli_args: &'a CliArgs) -> InternalArgs<'a> {
        if let Some(_file_path) = &cli_args.tables_file {
            Self {
                cli_args,
                white_listed_tables: None,
            }
        } else {
            Self {
                cli_args,
                white_listed_tables: None,
            }
        }
    }
}

fn db_url_shortener(args: &InternalArgs, db_url: &str) -> String {
    if db_url == args.cli_args.db1 {
        "DB1".to_string()
    } else {
        "DB2".to_string()
    }
}
