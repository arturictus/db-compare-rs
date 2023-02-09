use crate::database;
use crate::diff::DiffIO;
use crate::Config;
use postgres::Error;

pub fn run<T: DiffIO>(config: &Config, presenter: &mut T) -> Result<(), postgres::Error> {
    let count1 = count(config, &config.args.db1).unwrap();
    let count2 = count(config, &config.args.db2).unwrap();

    presenter.write(("======== Counts for all tables".to_string(), count1, count2));
    Ok(())
}

fn count(config: &Config, db_url: &str) -> Result<Vec<String>, Error> {
    let tables = database::all_tables(config, db_url)?;
    let mut counts = Vec::new();
    for table_name in tables {
        let result = database::count_for(config, db_url, &table_name).unwrap();
        counts.push(format!("{table_name} : {result}"));
    }
    counts.sort();
    Ok(counts)
}
