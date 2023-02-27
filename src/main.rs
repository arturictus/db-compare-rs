use clap::Parser;
use db_compare::Args;

fn main() -> Result<(), postgres::Error> {
    let args = Args::parse();
    db_compare::run(args)?;
    Ok(())
}
