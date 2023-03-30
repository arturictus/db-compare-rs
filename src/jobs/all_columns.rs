use crate::database;
use crate::database::RequestBuilder;
use crate::diff::IO;
use crate::Config;

use super::par_run;

pub fn run(config: &Config) -> Result<(), postgres::Error> {
    let q = RequestBuilder::new(config).column("id");
    let tables = database::tables_with_column(q.build_master())?.to_s();
    for table in tables {
        compare_table(config, &table)?;
    }
    Ok(())
}

fn compare_table(config: &Config, table: &str) -> Result<(), postgres::Error> {
    let q = RequestBuilder::new(config).table(table);
    let mut upper_bound = database::get_greatest_id_from(q.build_master())?;
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

        let builder = RequestBuilder::new(config)
            .table(table)
            .bounds((lower_bound, upper_bound));

        let (records1, records2) = par_run(builder, database::get_row_by_id_range)?;

        let mut diff_io = config.diff_io.borrow_mut();
        diff_io.write(
            config,
            (
                format!("`{table}` compare rows with ids from {lower_bound} to {upper_bound}"),
                records1,
                records2,
            ),
        );
        upper_bound = lower_bound;
        counter += config.limit;
    }
    Ok(())
}
