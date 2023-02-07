use crate::database;
use crate::{Args, DBsResult};
use postgres::{Error, SimpleQueryMessage};

pub fn run(args: &Args, presenter: fn(DBsResult)) -> Result<(), postgres::Error> {
    let count1 = count(args, &args.db1).unwrap();
    let count2 = count(args, &args.db2).unwrap();

    presenter(("======== Counts for all tables".to_string(), count1, count2));
    Ok(())
}

fn count(args: &Args, db_url: &str) -> Result<Vec<String>, Error> {
    let tables = database::all_tables(args, db_url)?;
    println!("== QUERY: Getting counts from db");
    let mut counts = Vec::new();
    for table_name in tables {
        let result = database::count_for(args, db_url, &table_name).unwrap();
        counts.push(format!("{} : {}", table_name, result));
    }
    counts.sort();
    Ok(counts)
}
