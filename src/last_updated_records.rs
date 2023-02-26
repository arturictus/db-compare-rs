use crate::database::{self, QueryBuilder};
use crate::diff::IO;
use crate::Config;

pub fn tables(config: &Config) -> Result<(), postgres::Error> {
    let query = QueryBuilder::new(config).column(column());
    let (db1_tables, db2_tables) = rayon::join(
        || database::tables_with_column(query.build_master()).unwrap(),
        || database::tables_with_column(query.build_replica()).unwrap(),
    );
    let mut diff_io = config.diff_io.borrow_mut();
    diff_io.write((
        "========  Tables with `updated_at` column".to_string(),
        db1_tables,
        db2_tables,
    ));
    Ok(())
}

pub fn only_updated_ats(config: &Config) -> Result<(), postgres::Error> {
    let query = QueryBuilder::new(config).column(column());
    let db1_tables = database::tables_with_column(query.build_master()).unwrap();
    for table in db1_tables {
        compare_table_updated_ats(config, &table)?;
    }
    Ok(())
}

pub fn all_columns(config: &Config) -> Result<(), postgres::Error> {
    let query = QueryBuilder::new(config).column(column());
    let db1_tables = database::tables_with_column(query.build_master()).unwrap();
    for table in db1_tables {
        compare_rows(config, &table)?;
    }
    Ok(())
}

fn column() -> String {
    "updated_at".to_string()
}

fn compare_table_updated_ats(config: &Config, table: &str) -> Result<(), postgres::Error> {
    let query = QueryBuilder::new(config).table(table).column(column());
    let (records1, records2) = rayon::join(
        || database::id_and_column_value(query.build_master()).unwrap(),
        || database::id_and_column_value(query.build_replica()).unwrap(),
    );
    let mut diff_io = config.diff_io.borrow_mut();
    diff_io.write((
        format!("====== `{table}` updated_at values"),
        records1,
        records2,
    ));
    Ok(())
}

fn compare_rows(config: &Config, table: &str) -> Result<(), postgres::Error> {
    let query = QueryBuilder::new(config).table(table).column(column());
    let (records1, records2) = rayon::join(
        || database::full_row_ordered_by(query.build_master()).unwrap(),
        || database::full_row_ordered_by(query.build_replica()).unwrap(),
    );
    let mut diff_io = config.diff_io.borrow_mut();
    diff_io.write((format!("====== `{table}` all columns"), records1, records2));
    Ok(())
}
