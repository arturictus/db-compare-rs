use clap::Parser;
use db_compare::{Args, Config, IO};
use std::error;

fn main() -> Result<(), Box<dyn error::Error>> {
    let args = Args::parse();
    let config = Config::new(&args);
    {
        let mut f = config.diff_io.borrow_mut();
        f.echo(&format!("--- {} ---", config.db1));
        f.echo(&format!("+++ {} +++", config.db2));
    }

    db_compare::run(&config)?;
    Ok(())
}
