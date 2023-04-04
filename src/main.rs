use clap::Parser;
use db_compare::{Cli, Config};
use std::error;

fn main() -> Result<(), Box<dyn error::Error>> {
    let args = Cli::parse();
    let config = Config::new(&args.command);

    db_compare::run(&config)?;
    Ok(())
}
