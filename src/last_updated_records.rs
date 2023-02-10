use crate::database;
use crate::diff::IO;
use crate::Config;

pub fn tables(config: &Config) -> Result<(), postgres::Error> {
    let db1_tables = database::tables_with_column(config, &config.args.db1, column()).unwrap();
    let db2_tables = database::tables_with_column(config, &config.args.db2, column()).unwrap();
    let mut diff_io = config.diff_io.borrow_mut();
    diff_io.write((
        "========  Tables with `updated_at` column".to_string(),
        db1_tables,
        db2_tables,
    ));
    Ok(())
}

pub fn only_updated_ats(config: &Config) -> Result<(), postgres::Error> {
    let db1_tables = database::tables_with_column(config, &config.args.db1, column()).unwrap();
    for table in db1_tables {
        compare_table_updated_ats(config, &table)?;
    }
    Ok(())
}

pub fn all_columns(config: &Config) -> Result<(), postgres::Error> {
    let db1_tables = database::tables_with_column(config, &config.args.db1, column()).unwrap();
    for table in db1_tables {
        compare_rows(config, &table)?;
    }
    Ok(())
}

fn column() -> String {
    "updated_at".to_string()
}

fn compare_table_updated_ats(config: &Config, table: &str) -> Result<(), postgres::Error> {
    let records1 =
        database::id_and_column_value(config, &config.args.db1, table, column()).unwrap();
    let records2 =
        database::id_and_column_value(config, &config.args.db2, table, column()).unwrap();

    let mut diff_io = config.diff_io.borrow_mut();
    diff_io.write((
        format!("====== `{table}` updated_at values"),
        records1,
        records2,
    ));
    Ok(())
}

fn compare_rows(config: &Config, table: &str) -> Result<(), postgres::Error> {
    let records1 =
        database::full_row_ordered_by(config, &config.args.db1, table, column()).unwrap();
    let records2 =
        database::full_row_ordered_by(config, &config.args.db2, table, column()).unwrap();
    let mut diff_io = config.diff_io.borrow_mut();
    diff_io.write((format!("====== `{table}` all columns"), records1, records2));
    Ok(())
}
