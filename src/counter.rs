use crate::database;
use crate::database::DBSelector::{MasterDB, ReplicaDB};
use crate::diff::{IOType, IO};
use crate::Config;

use rayon::prelude::*;
use std::cell::RefCell;

pub fn rayon_run(config: &Config, diff_io: &RefCell<IOType>) -> Result<(), postgres::Error> {
    let tables = database::all_tables(config, MasterDB)?;
    for table in tables {
        // let config_a = config.clone();
        // let config_b = config.clone();
        let table_a = table.clone();
        let table_b = table.clone();
        let (a, b) = rayon::join(
            || database::count_for(config.clone(), MasterDB.url(config).to_string(), table_a),
            || database::count_for(config.clone(), ReplicaDB.url(config).to_string(), table_b),
        );

        let result1 = a.unwrap();
        let result2 = b.unwrap();
        let mut io = diff_io.borrow_mut();
        io.write((
            format!("== `{table}` count"),
            vec![format!("{result1}")],
            vec![format!("{result2}")],
        ));
    }
    Ok(())
}
pub fn run(config: &Config, diff_io: &RefCell<IOType>) -> Result<(), postgres::Error> {
    let tables = database::all_tables(config, MasterDB)?;
    for table in tables {
        // let config_a = config.clone();
        // let config_b = config.clone();
        let table_a = table.clone();
        let table_b = table.clone();

        let result1 =
            database::count_for(config.clone(), MasterDB.url(config).to_string(), table_a).unwrap();
        let result2 =
            database::count_for(config.clone(), ReplicaDB.url(config).to_string(), table_b)
                .unwrap();
        let mut io = diff_io.borrow_mut();
        io.write((
            format!("== `{table}` count"),
            vec![format!("{result1}")],
            vec![format!("{result2}")],
        ));
    }
    Ok(())
}
