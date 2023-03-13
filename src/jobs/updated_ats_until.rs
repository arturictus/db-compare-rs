use crate::database::{self, RequestBuilder};
use crate::diff::IO;
use crate::Config;

use super::par_run;

pub fn run(config: &Config) -> Result<(), postgres::Error> {
    let query = RequestBuilder::new(config).column(column());
    let db1_tables = database::tables_with_column(query.build_master())?;
    for table in db1_tables {
        compare_rows(config, &table)?;
    }
    Ok(())
}

fn column() -> String {
    "updated_at".to_string()
}

fn compare_rows(config: &Config, table: &str) -> Result<(), postgres::Error> {
    let builder = RequestBuilder::new(config)
        .table(table)
        .column(column())
        // date +%s
        .until(1678715737);
    let (records1, records2) = par_run(builder, database::full_row_ordered_by_until)?;
    let mut diff_io = config.diff_io.borrow_mut();
    diff_io.write((format!("====== `{table}` all columns"), records1, records2));
    Ok(())
}
