use crate::database;
use crate::diff::IO;
use crate::Config;

pub fn run(config: &Config) -> Result<(), postgres::Error> {
    let tables = database::tables_with_column(config, &config.args.db1, "id".to_string()).unwrap();
    for table in tables {
        compare_table(config, &table).unwrap();
    }
    Ok(())
}

fn compare_table(config: &Config, table: &str) -> Result<(), postgres::Error> {
    let mut upper_bound = database::get_greatest_id_from(&config, &config.args.db1, table).unwrap();
    while upper_bound != 0 {
        let lower_bound = if upper_bound > config.args.limit {
            upper_bound - config.args.limit
        } else {
            0
        };
        let records1 = database::get_row_by_id_range(
            config,
            &config.args.db1,
            table,
            lower_bound,
            upper_bound,
        )
        .unwrap();
        let records2 = database::get_row_by_id_range(
            config,
            &config.args.db2,
            table,
            lower_bound,
            upper_bound,
        )
        .unwrap();

        let mut diff_io = config.diff_io.borrow_mut();
        diff_io.write((
            format!("====== `{table}` compare rows with ids from {lower_bound} to {upper_bound}"),
            records1,
            records2,
        ));
        upper_bound = lower_bound;
    }
    Ok(())
}
