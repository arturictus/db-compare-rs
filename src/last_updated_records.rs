use crate::database;
use crate::Config;
use crate::PresenterAbstract;

pub fn tables<T: PresenterAbstract>(
    config: &Config,
    presenter: &mut T,
) -> Result<(), postgres::Error> {
    let db1_tables = database::tables_with_column(config, &config.args.db1, column()).unwrap();
    let db2_tables = database::tables_with_column(config, &config.args.db2, column()).unwrap();
    presenter.write((
        "========  Tables with `updated_at` column".to_string(),
        db1_tables,
        db2_tables,
    ));
    Ok(())
}

pub fn only_updated_ats<T: PresenterAbstract>(
    config: &Config,
    presenter: &mut T,
) -> Result<(), postgres::Error> {
    let db1_tables = database::tables_with_column(config, &config.args.db1, column()).unwrap();
    for table in db1_tables {
        compare_table_updated_ats(config, &table, presenter)?;
    }
    Ok(())
}

pub fn all_columns<T: PresenterAbstract>(
    config: &Config,
    presenter: &mut T,
) -> Result<(), postgres::Error> {
    let db1_tables = database::tables_with_column(config, &config.args.db1, column()).unwrap();
    for table in db1_tables {
        compare_rows(config, &table, presenter)?;
    }
    Ok(())
}

fn column() -> String {
    "updated_at".to_string()
}

fn compare_table_updated_ats<T: PresenterAbstract>(
    config: &Config,
    table: &str,
    presenter: &mut T,
) -> Result<(), postgres::Error> {
    let records1 =
        database::id_and_column_value(config, &config.args.db1, table, column()).unwrap();
    let records2 =
        database::id_and_column_value(config, &config.args.db2, table, column()).unwrap();

    presenter.write((
        format!("====== `{table}` updated_at values"),
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
    presenter.write((format!("====== `{table}` all columns"), records1, records2));
    Ok(())
}
