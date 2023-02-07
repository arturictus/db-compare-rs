use crate::database;
use crate::{Args, DBsResult};

pub fn tables(args: &Args, presenter: fn(DBsResult)) -> Result<(), postgres::Error> {
    let db1_tables = database::tables_with_column(args, &args.db1, column()).unwrap();
    let db2_tables = database::tables_with_column(args, &args.db2, column()).unwrap();
    presenter((
        "========  Tables with `updated_at` column".to_string(),
        db1_tables,
        db2_tables,
    ));
    Ok(())
}

pub fn only_updated_ats(args: &Args, presenter: fn(DBsResult)) -> Result<(), postgres::Error> {
    let db1_tables = database::tables_with_column(args, &args.db1, column()).unwrap();
    for table in db1_tables {
        compare_table_updated_ats(args, &table, presenter)?;
    }
    Ok(())
}

pub fn all_columns(args: &Args, presenter: fn(DBsResult)) -> Result<(), postgres::Error> {
    let db1_tables = database::tables_with_column(args, &args.db1, column()).unwrap();
    for table in db1_tables {
        compare_rows(args, &table, presenter)?;
    }
    Ok(())
}

fn column() -> String {
    "updated_at".to_string()
}

fn compare_table_updated_ats(
    args: &Args,
    table: &str,
    presenter: fn(DBsResult),
) -> Result<(), postgres::Error> {
    let records1 = database::id_and_column_value(args, &args.db1, table, column()).unwrap();
    let records2 = database::id_and_column_value(args, &args.db2, table, column()).unwrap();

    presenter((
        format!("====== `{}` updated_at values", table),
        records1,
        records2,
    ));
    Ok(())
}

fn compare_rows(args: &Args, table: &str, presenter: fn(DBsResult)) -> Result<(), postgres::Error> {
    let records1 = database::full_row_ordered_by(&args, &args.db1, table, column()).unwrap();
    let records2 = database::full_row_ordered_by(&args, &args.db2, table, column()).unwrap();
    presenter((
        format!("====== `{}` all columns", table),
        records1,
        records2,
    ));
    Ok(())
}
