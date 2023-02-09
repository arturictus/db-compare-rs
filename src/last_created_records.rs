use crate::database;
use crate::Config;
use crate::PresenterAbstract;

pub fn tables<T: PresenterAbstract>(
    config: &Config,
    presenter: &mut T,
) -> Result<(), postgres::Error> {
    let db1_tables = non_updated_at_tables(config, &config.args.db1).unwrap();
    let db2_tables = non_updated_at_tables(config, &config.args.db2).unwrap();
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

pub fn only_created_ats<T: PresenterAbstract>(
    config: &Config,
    presenter: &mut T,
) -> Result<(), postgres::Error> {
    let db1_tables = non_updated_at_tables(config, &config.args.db1).unwrap();
    for table in db1_tables {
        compare_table_created_ats(config, &table, presenter)?;
    }
    Ok(())
}

pub fn all_columns<T: PresenterAbstract>(
    config: &Config,
    presenter: &mut T,
) -> Result<(), postgres::Error> {
    let db1_tables = non_updated_at_tables(config, &config.args.db1).unwrap();
    for table in db1_tables {
        compare_rows(config, &table, presenter)?;
    }
    Ok(())
}

fn column() -> String {
    "created_at".to_string()
}

fn non_updated_at_tables(config: &Config, db_url: &str) -> Result<Vec<String>, postgres::Error> {
    let created_at_tables = database::tables_with_column(config, db_url, column()).unwrap();
    let updated_at_tables =
        database::tables_with_column(config, db_url, "updated_at".to_string()).unwrap();
    let difference: Vec<String> = created_at_tables
        .into_iter()
        .filter(|item| !updated_at_tables.contains(item))
        .collect();

    Ok(difference)
}

fn compare_table_created_ats<T: PresenterAbstract>(
    config: &Config,
    table: &str,
    presenter: &mut T,
) -> Result<(), postgres::Error> {
    let records1 =
        database::id_and_column_value(config, &config.args.db1, table, column()).unwrap();
    let records2 =
        database::id_and_column_value(config, &config.args.db2, table, column()).unwrap();

    presenter.call((
        format!("====== `{table}` created_at values"),
        records1,
        records2,
    ));
    Ok(())
}

fn compare_rows<T: PresenterAbstract>(
    config: &Config,
    table: &str,
    presenter: &mut T,
) -> Result<(), postgres::Error> {
    let records1 =
        database::full_row_ordered_by(config, &config.args.db1, table, column()).unwrap();
    let records2 =
        database::full_row_ordered_by(config, &config.args.db2, table, column()).unwrap();
    presenter.call((format!("====== `{table}` all columns"), records1, records2));
    Ok(())
}
