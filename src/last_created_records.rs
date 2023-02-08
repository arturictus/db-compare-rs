use crate::database;
use crate::Args;
use crate::Presenter;

pub fn tables(args: &Args, presenter: &mut Presenter) -> Result<(), postgres::Error> {
    let db1_tables = non_updated_at_tables(args, &args.db1).unwrap();
    let db2_tables = non_updated_at_tables(args, &args.db2).unwrap();
    println!("# -----  List of tables without `updated_at`");
    println!("{db1_tables:?}");
    println!("# ---------------");
    presenter.call((
        "========  Tables with `created_at` column but not `updated_at` difference between DBs"
            .to_string(),
        db1_tables,
        db2_tables,
    ));
    Ok(())
}

pub fn only_created_ats(args: &Args, presenter: &mut Presenter) -> Result<(), postgres::Error> {
    let db1_tables = non_updated_at_tables(args, &args.db1).unwrap();
    for table in db1_tables {
        compare_table_created_ats(args, &table, presenter)?;
    }
    Ok(())
}

pub fn all_columns(args: &Args, presenter: &mut Presenter) -> Result<(), postgres::Error> {
    let db1_tables = non_updated_at_tables(args, &args.db1).unwrap();
    for table in db1_tables {
        compare_rows(args, &table, presenter)?;
    }
    Ok(())
}

fn column() -> String {
    "created_at".to_string()
}

fn non_updated_at_tables(args: &Args, db_url: &str) -> Result<Vec<String>, postgres::Error> {
    let created_at_tables = database::tables_with_column(args, db_url, column()).unwrap();
    let updated_at_tables =
        database::tables_with_column(args, db_url, "updated_at".to_string()).unwrap();
    let difference: Vec<String> = created_at_tables
        .into_iter()
        .filter(|item| !updated_at_tables.contains(item))
        .collect();

    Ok(difference)
}

fn compare_table_created_ats(
    args: &Args,
    table: &str,
    presenter: &mut Presenter,
) -> Result<(), postgres::Error> {
    let records1 = database::id_and_column_value(args, &args.db1, table, column()).unwrap();
    let records2 = database::id_and_column_value(args, &args.db2, table, column()).unwrap();

    presenter.call((
        format!("====== `{table}` created_at values"),
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
