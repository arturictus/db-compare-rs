use crate::database;
use crate::Args;
use crate::Presenter;

pub fn tables(args: &Args, presenter: &mut Presenter) -> Result<(), postgres::Error> {
    let db1_tables = database::tables_with_column(args, &args.db1, column()).unwrap();
    let db2_tables = database::tables_with_column(args, &args.db2, column()).unwrap();
    presenter.call((
        "========  Tables with `updated_at` column".to_string(),
        db1_tables,
        db2_tables,
    ));
    Ok(())
}

pub fn only_updated_ats(args: &Args, presenter: &mut Presenter) -> Result<(), postgres::Error> {
    let db1_tables = database::tables_with_column(args, &args.db1, column()).unwrap();
    for table in db1_tables {
        compare_table_updated_ats(args, &table, presenter)?;
    }
    Ok(())
}

pub fn all_columns(args: &Args, presenter: &mut Presenter) -> Result<(), postgres::Error> {
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
    presenter: &mut Presenter,
) -> Result<(), postgres::Error> {
    let records1 = database::id_and_column_value(args, &args.db1, table, column()).unwrap();
    let records2 = database::id_and_column_value(args, &args.db2, table, column()).unwrap();

    presenter.call((
        format!("====== `{table}` updated_at values"),
        records1,
        records2,
    ));
    Ok(())
}

fn compare_rows(
    args: &Args,
    table: &str,
    presenter: &mut Presenter,
) -> Result<(), postgres::Error> {
    let records1 = database::full_row_ordered_by(args, &args.db1, table, column()).unwrap();
    let records2 = database::full_row_ordered_by(args, &args.db2, table, column()).unwrap();
    presenter.call((format!("====== `{table}` all columns"), records1, records2));
    Ok(())
}
