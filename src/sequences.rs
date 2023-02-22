use crate::database;
use crate::database::DBSelector::{MasterDB, ReplicaDB};
use crate::diff::IO;
use crate::Config;

pub fn run(config: &Config) -> Result<(), postgres::Error> {
    let mut data1 = database::get_sequences(config, MasterDB).unwrap();
    data1.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    let data2 = database::get_sequences(config, ReplicaDB).unwrap();

    let mut diff_io = config.diff_io.borrow_mut();
    for (table, num) in data1 {
        let found = data2
            .iter()
            .find(|(t, _)| t == &table)
            .map(|data| data.1.to_string());
        diff_io.write((
            format!("== `{table}` sequence:"),
            vec![num.to_string()],
            vec![found.unwrap_or_else(|| "Not set".to_string())],
        ));
    }

    Ok(())
}
