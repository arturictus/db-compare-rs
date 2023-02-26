use openssl::conf;

use crate::database;
use crate::database::DBSelector::MasterDB;
use crate::database::QueryBuilder;
use crate::diff::IO;
use crate::Config;

pub fn run(config: &Config) -> Result<(), postgres::Error> {
    let tables = database::all_tables(QueryBuilder::new(config).build_master())?;
    for table in tables {
        let builder = QueryBuilder::new(config).table(&table);
        let (result1, result2) = rayon::join(
            || database::count_for(builder.build_master()).unwrap(),
            || database::count_for(builder.build_replica()).unwrap(),
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
