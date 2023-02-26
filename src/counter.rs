use crate::database;
use crate::database::DBSelector::MasterDB;
use crate::database::QueryBuilder;
use crate::diff::IO;
use crate::Config;

pub fn run(config: &Config) -> Result<(), postgres::Error> {
    let tables = database::all_tables(config, MasterDB)?;
    for table in tables {
        let (master_q, replica_q) = QueryBuilder::new(config).table(&table).build().unwrap();
        let (result1, result2) = rayon::join(
            || database::count_for(master_q).unwrap(),
            || database::count_for(replica_q).unwrap(),
        );
        let mut diff_io = config.diff_io.borrow_mut();
        diff_io.write((
            format!("== `{table}` count"),
            vec![format!("{result1}")],
            vec![format!("{result2}")],
        ));
    }
    Ok(())
}
