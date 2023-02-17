use crate::database;
use crate::database::DBSelector::{MasterDB, ReplicaDB};
use crate::diff::{IOType, IO};
use crate::Config;
use std::{cell::RefCell, fs};
use tokio;

pub async fn run(config: &Config, diff_io: &RefCell<IOType>) -> Result<(), postgres::Error> {
    let tables = database::all_tables(config, MasterDB)?;
    for table in tables {
        // let config_a = config.clone();
        // let config_b = config.clone();
        let table_a = table.clone();
        let table_b = table.clone();
        let call1 = database::count_for(config.clone(), MasterDB, table_a);
        let call2 = database::count_for(config.clone(), ReplicaDB, table_b);
        let (aresult1, aresult2) = tokio::join!(call1, call2);

        let result1 = aresult1.unwrap();
        let result2 = aresult2.unwrap();
        let mut io = diff_io.borrow_mut();
        io.write((
            format!("== `{table}` count"),
            vec![format!("{result1}")],
            vec![format!("{result2}")],
        ));
    }
    Ok(())
}
