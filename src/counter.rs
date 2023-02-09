use crate::database;
use crate::InternalArgs;
use crate::Presenter;
use postgres::Error;

pub fn run(args: &InternalArgs, presenter: &mut Presenter) -> Result<(), postgres::Error> {
    let count1 = count(args, &args.cli_args.db1).unwrap();
    let count2 = count(args, &args.cli_args.db2).unwrap();

    presenter.call(("======== Counts for all tables".to_string(), count1, count2));
    Ok(())
}

fn count(args: &InternalArgs, db_url: &str) -> Result<Vec<String>, Error> {
    let tables = database::all_tables(args, db_url)?;
    let mut counts = Vec::new();
    for table_name in tables {
        let result = database::count_for(args, db_url, &table_name).unwrap();
        counts.push(format!("{table_name} : {result}"));
    }
    counts.sort();
    Ok(counts)
}
