use crate::diff::IO;
use crate::{counter, database};
use crate::{Config, DBSelector};

pub fn run(config: &Config) -> Result<(), postgres::Error> {
    let tables = database::tables_with_column(config, &config.args.db1, "id".to_string()).unwrap();
    for table in tables {
        compare_table(config, &table).unwrap();
    }
    Ok(())
}

fn compare_table(config: &Config, table: &str) -> Result<(), postgres::Error> {
    let mut counter = database::count_for(&config, &config.args.db1, table).unwrap();

    while counter > 0 {
        counter = counter - config.args.limit;
        let ids =
            database::get_rows_ids_from_offset(config, &config.args.db1, table, counter).unwrap();
        for id in ids {
            let record1 = database::find(&config, &config.args.db1, &table, id).unwrap();
            let record2 = database::find(&config, &config.args.db2, &table, id).unwrap();
            let mut diff_io = config.diff_io.borrow_mut();
            diff_io.write((
                format!("====== `{table}` compare id {id}"),
                record1,
                record2,
            ));
        }
    }
    Ok(())
}
