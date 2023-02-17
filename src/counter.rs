use crate::database;
use crate::database::DBSelector::{MasterDB, ReplicaDB};
use crate::diff::IO;
use crate::Config;
use tokio;

pub async fn run(config: &Config) -> Result<(), postgres::Error> {
    let tables = database::all_tables(config, MasterDB)?;
    for table in tables {
        let (aresult1, aresult2) = tokio::join!(
            database::count_for(config, MasterDB, &table),
            database::count_for(config, ReplicaDB, &table)
        );
        let result1 = aresult1.unwrap();
        let result2 = aresult2.unwrap();
        let mut diff_io = config.diff_io.borrow_mut();
        diff_io.write((
            format!("== `{table}` count"),
            vec![format!("{result1}")],
            vec![format!("{result2}")],
        ));
    }
    Ok(())
}
