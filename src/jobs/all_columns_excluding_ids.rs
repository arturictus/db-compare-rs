use super::utils::compare_table_for_all_columns;
use crate::database::RequestBuilder;
use crate::Config;
use crate::{database, DBResultTypes};

pub fn run(config: &Config) -> Result<(), postgres::Error> {
    let id_tables =
        database::tables_with_column(RequestBuilder::new(config).column("id").build_master())?
            .to_s();
    let updated_at_tables =
        database::tables_with_column(RequestBuilder::new(config).column(column()).build_master())?
            .to_s();
    let tables = updated_at_tables
        .into_iter()
        .filter(|t| id_tables.contains(t))
        .collect::<Vec<String>>();
    for table in tables {
        let ids = updated_ids_after_cutoff(config, &table)?;
        compare_table_for_all_columns(config, &table, Some(ids))?;
    }
    Ok(())
}

fn column() -> String {
    "updated_at".to_string()
}

fn updated_ids_after_cutoff(config: &Config, table: &str) -> Result<Vec<u32>, postgres::Error> {
    let q = RequestBuilder::new(config)
        .table(table)
        .tm_cutoff(config.tm_cutoff);
    if let DBResultTypes::Ids(ids) = database::updated_ids_after_cutoff(q.build_replica())? {
        Ok(ids)
    } else {
        panic!("Expected DBResultTypes::Ids");
    }
}
