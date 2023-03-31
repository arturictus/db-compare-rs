use super::utils::compare_table_for_all_columns;
use crate::database;
use crate::database::RequestBuilder;
use crate::Config;

pub fn run(config: &Config) -> Result<(), postgres::Error> {
    let q = RequestBuilder::new(config).column("id");
    let tables = database::tables_with_column(q.build_master())?.to_s();
    for table in tables {
        compare_table_for_all_columns(config, &table, None)?;
    }
    Ok(())
}
