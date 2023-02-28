use clap::Parser;
use db_compare::{Args, Config};

fn main() -> Result<(), postgres::Error> {
    let args = Args::parse();
    let config = Config::new(&args);
    db_compare::run(&config)?;
    Ok(())
}
