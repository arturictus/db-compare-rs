use clap::Parser;
use db_compare::{Args, Config};
use std::error;

fn main() -> Result<(), Box<dyn error::Error>> {
    let args = Args::parse();
    let config = Config::new(&args);
    db_compare::run(&config)?;
    Ok(())
}
