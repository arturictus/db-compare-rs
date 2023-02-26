use clap::builder;

use crate::database;
use crate::database::DBSelector::MasterDB;
use crate::database::QueryBuilder;
use crate::diff::IO;
use crate::Config;

pub fn run(config: &Config) -> Result<(), postgres::Error> {
    let tables = database::tables_with_column(config, MasterDB, "id".to_string()).unwrap();
    for table in tables {
        compare_table(config, &table).unwrap();
    }
    Ok(())
}

fn compare_table(config: &Config, table: &str) -> Result<(), postgres::Error> {
    let mut upper_bound = database::get_greatest_id_from(config, MasterDB, table).unwrap();
    let mut counter = 0u32;
    while upper_bound != 0 {
        if config.all_columns_sample_size.is_some()
            && counter >= config.all_columns_sample_size.unwrap()
        {
            break;
        }
        let lower_bound = if upper_bound > config.limit {
            upper_bound - config.limit
        } else {
            0
        };

        let builder = QueryBuilder::new(config)
            .table(table)
            .bounds((lower_bound, upper_bound));

        let (records1, records2) = rayon::join(
            || database::get_row_by_id_range(builder.build_master()).unwrap(),
            || database::get_row_by_id_range(builder.build_replica()).unwrap(),
        );

        let mut diff_io = config.diff_io.borrow_mut();
        diff_io.write((
            format!("====== `{table}` compare rows with ids from {lower_bound} to {upper_bound}"),
            records1,
            records2,
        ));
        upper_bound = lower_bound;
        counter += config.limit;
    }
    Ok(())
}
