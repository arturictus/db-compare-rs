mod counter;
mod database;
mod last_created_records;
mod last_updated_records;
mod presenter;
use clap::Parser;

type DBsResult = (String, Vec<String>, Vec<String>);

#[derive(Parser, Debug, Default)]
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
}
fn main() -> Result<(), postgres::Error> {
    let args = Args::parse();
    database::ping_db(&args, &args.db1)?;
    database::ping_db(&args, &args.db2)?;
    counter::run(&args, presenter::call)?;
    last_updated_records::tables(&args, presenter::call)?;
    last_updated_records::only_updated_ats(&args, presenter::call)?;
    last_updated_records::all_columns(&args, presenter::call)?;
    last_created_records::tables(&args, presenter::call)?;
    last_created_records::only_created_ats(&args, presenter::call)?;
    last_created_records::all_columns(&args, presenter::call)?;
    Ok(())
}

fn db_url_shortener(args: &Args, db_url: &str) -> String {
    if db_url == args.db1 {
        "DB1".to_string()
    } else {
        "DB2".to_string()
    }
}
