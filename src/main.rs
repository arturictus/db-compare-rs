use clap::Parser;
use db_compare::{Cli, Commands, Config};
use std::error;

fn main() -> Result<(), Box<dyn error::Error>> {
    let args = Cli::parse();
    match &args.command {
        Commands::Compare { .. } => {
            let config = Config::new(&args.command);
            db_compare::run(&config)?;
        }
        Commands::Summarize { file, .. } => {
            let config = Config::new(&args.command);
            db_compare::run_summary(&config, file)?;
        }
    }

    Ok(())
}
