use crate::database;
use crate::database::DBSelector::{MasterDB, ReplicaDB};
use crate::diff::IO;
use crate::Config;

pub fn run(config: &Config) -> Result<(), postgres::Error> {
    let tables = database::all_tables(config, MasterDB)?;
    for table in tables {
        let result1 = database::count_for(config, MasterDB, &table).unwrap();
        let result2 = database::count_for(config, ReplicaDB, &table).unwrap();
        let mut diff_io = config.diff_io.borrow_mut();
        diff_io.write((
            format!("== `{table}` count"),
            vec![format!("{result1}")],
            vec![format!("{result2}")],
        ));
    }
    Ok(())
}
