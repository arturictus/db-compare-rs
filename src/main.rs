mod counter;
mod database;
mod diff_formatter;
mod last_created_records;
mod last_updated_records;
mod presenter;
use presenter::Presenter;
use std::fs::File;

use clap::Parser;
use std::io::LineWriter;
mod file_presenter;

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
    #[arg(long = "tls")]
    tls: Option<bool>,
    #[arg(long)]
    file: Option<String>,
}
fn main() -> Result<(), postgres::Error> {
    let args = Args::parse();
    let mut out = Presenter::new(&args);
    database::ping_db(&args, &args.db1)?;
    database::ping_db(&args, &args.db2)?;
    counter::run(&args, &mut out)?;
    last_updated_records::tables(&args, &mut out)?;
    last_updated_records::only_updated_ats(&args, &mut out)?;
    last_updated_records::all_columns(&args, &mut out)?;
    last_created_records::tables(&args, &mut out)?;
    last_created_records::only_created_ats(&args, &mut out)?;
    last_created_records::all_columns(&args, &mut out)?;
    out.end();
    Ok(())
}

fn db_url_shortener(args: &Args, db_url: &str) -> String {
    if db_url == args.db1 {
        "DB1".to_string()
    } else {
        "DB2".to_string()
    }
}

fn do_presenter(result: DBsResult) {
    let diff = diff_formatter::call(result);
    console_output(diff);
}
fn console_output(diff: Diff) {
    let (header, result) = diff;
    println!("{}", header);
    format!("{}", result);
}
