use crate::database::{self, RequestBuilder};
use crate::diff::IO;
use crate::{Config, DBResultTypes};

use super::par_run;

pub fn tables(config: &Config) -> Result<(), postgres::Error> {
    let builder = RequestBuilder::new(config).column(column());
    let (db1_tables, db2_tables) = par_run(builder, database::tables_with_column)?;
    let mut diff_io = config.diff_io.borrow_mut();
    diff_io.write((
        "========  Tables with `updated_at` column".to_string(),
        db1_tables,
        db2_tables,
    ));
    Ok(())
}

pub fn only_updated_ats(config: &Config) -> Result<(), postgres::Error> {
    let query = RequestBuilder::new(config).column(column());
    let db1_tables = database::tables_with_column(query.build_master())?.to_s();
    for table in db1_tables {
        compare_table_updated_ats(config, &table)?;
    }
    Ok(())
}

pub fn all_columns(config: &Config) -> Result<(), postgres::Error> {
    let query = RequestBuilder::new(config).column(column());
    let db1_tables = database::tables_with_column(query.build_master())?.to_s();
    for table in db1_tables {
        compare_rows(config, &table)?;
    }
    Ok(())
}

fn column() -> String {
    "updated_at".to_string()
}

fn compare_table_updated_ats(config: &Config, table: &str) -> Result<(), postgres::Error> {
    let builder = RequestBuilder::new(config).table(table).column(column());
    let (records1, records2) = par_run(builder, database::id_and_column_value)?;

    let mut diff_io = config.diff_io.borrow_mut();
    diff_io.write((
        format!("====== `{table}` updated_at values"),
        records1,
        records2,
    ));
    Ok(())
}

fn compare_rows(config: &Config, table: &str) -> Result<(), postgres::Error> {
    let builder = RequestBuilder::new(config).table(table).column(column());
    let (records1, records2) = par_run(builder, database::full_row_ordered_by)?;
    let mut diff_io = config.diff_io.borrow_mut();
    diff_io.write((format!("====== `{table}` all columns"), records1, records2));
    Ok(())
}
