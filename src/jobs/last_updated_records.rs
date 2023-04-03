use super::{Job, Output};
use crate::database::{self, RequestBuilder};
use crate::Config;

use super::par_run;

pub fn tables(config: &Config) -> Result<(), postgres::Error> {
    let builder = RequestBuilder::new(config).column(column());
    let (db1_tables, db2_tables) = par_run(builder, database::tables_with_column)?;
    let mut output = Output::new(
        config,
        Job::UpdatedAts,
        Some("tables_with_updated_at".to_string()),
    );

    let result = (
        "Tables with `updated_at` column".to_string(),
        db1_tables,
        db2_tables,
    );
    output.write(result.clone());
    output.end();
    Ok(())
}

pub fn all_columns(config: &Config) -> Result<(), postgres::Error> {
    let query = RequestBuilder::new(config).column(column());
    let db1_tables = database::tables_with_column(query.build_master())?.to_s();
    for table in db1_tables {
        let mut output = Output::new(config, Job::UpdatedAts, Some(table.clone()));
        compare_rows(&mut output, &table)?;
        output.end();
    }
    Ok(())
}

fn column() -> String {
    "updated_at".to_string()
}

fn compare_rows(output: &mut Output, table: &str) -> Result<(), postgres::Error> {
    let config = output.config;
    let builder = RequestBuilder::new(config).table(table).column(column());
    let (records1, records2) = par_run(builder, database::full_row_ordered_by)?;
    let result = (format!("`{table}` all columns"), records1, records2);

    output.write(result.clone());

    Ok(())
}
