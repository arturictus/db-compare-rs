use crate::database;
use crate::database::DBSelector;
use crate::database::DBSelector::{MasterDB, ReplicaDB};
use crate::diff::IO;
use crate::Config;

pub fn tables(config: &Config, diff_io: &RefCell<IOType>) -> Result<(), postgres::Error> {
    let (a, b) = rayon::join(
        || non_updated_at_tables(config.clone(), MasterDB, table.clone()),
        || non_updated_at_tables(config.clone(), ReplicaDB, table.clone()),
    );
    let db1_tables = a.unwrap();
    let db2_tables = b.unwrap();
    println!("# -----  List of tables without `updated_at`");
    println!("{db1_tables:?}");
    println!("# ---------------");
    let mut diff_io = diff_io.borrow_mut();
    diff_io.write((
        "========  Tables with `created_at` column but not `updated_at` difference between DBs"
            .to_string(),
        db1_tables,
        db2_tables,
    ));
    Ok(())
}

pub fn only_created_ats(config: &Config, diff_io: &RefCell<IOType>) -> Result<(), postgres::Error> {
    let db1_tables = non_updated_at_tables(config, MasterDB).unwrap();
    for table in db1_tables {
        compare_table_created_ats(config, &table, diff_io)?;
    }
    Ok(())
}

pub fn all_columns(config: &Config, diff_io: &RefCell<IOType>) -> Result<(), postgres::Error> {
    let db1_tables = non_updated_at_tables(config, MasterDB).unwrap();
    for table in db1_tables {
        compare_rows(config, &table)?;
    }
    Ok(())
}

fn column() -> String {
    "created_at".to_string()
}

fn non_updated_at_tables(config: &Config, db: DBSelector) -> Result<Vec<String>, postgres::Error> {
    let created_at_tables = database::tables_with_column(config, db, column()).unwrap();
    let updated_at_tables =
        database::tables_with_column(config, db, "updated_at".to_string()).unwrap();
    let difference: Vec<String> = created_at_tables
        .into_iter()
        .filter(|item| !updated_at_tables.contains(item))
        .collect();

    Ok(difference)
}

fn compare_table_created_ats(
    config: &Config,
    diff_io: &RefCell<IOType>,
    table: &str,
) -> Result<(), postgres::Error> {
    let (a, b) = rayon::join(
        || database::id_and_column_value(config.clone(), MasterDB, table.clone()),
        || database::id_and_column_value(config.clone(), ReplicaDB, table.clone()),
    );
    let records1 = a.unwrap();
    let records2 = b.unwrap();

    let mut diff_io = diff_io.borrow_mut();
    diff_io.write((
        format!("====== `{table}` created_at values"),
        records1,
        records2,
    ));
    Ok(())
}

fn compare_rows(
    config: &Config,
    diff_io: &RefCell<IOType>,
    table: &str,
) -> Result<(), postgres::Error> {
    let (a, b) = rayon::join(
        || database::full_row_ordered_by(config.clone(), MasterDB, table.clone(), column()),
        || database::full_row_ordered_by(config.clone(), ReplicaDB, table.clone(), column()),
    );
    let records1 = a.unwrap();
    let records2 = b.unwrap();
    let mut diff_io = config.diff_io.borrow_mut();
    diff_io.write((format!("====== `{table}` all columns"), records1, records2));
    Ok(())
}
