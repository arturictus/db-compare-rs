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

fn column() -> String {
    "updated_at".to_string()
}

fn updated_ids_after_cutoff(
    config: &Config,
    table: &str,
    cutoff: &str,
) -> Result<Vec<String>, postgres::Error> {
    let q = RequestBuilder::new(config).table(table).column(column());
    // .where_clause(format!("updated_at > '{}'", cutoff));
    let ids = database::updated_ids_after_cutoff(q.build_replica())?.to_s();
    Ok(ids)
}
