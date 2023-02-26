use crate::database::DBSelector;
use crate::database::DBSelector::{MasterDB, ReplicaDB};
use crate::database::{self, QueryBuilder};
use crate::diff::IO;
use crate::Config;

pub fn tables(config: &Config) -> Result<(), postgres::Error> {
    let db1_tables = non_updated_at_tables(config, MasterDB).unwrap();
    let db2_tables = non_updated_at_tables(config, ReplicaDB).unwrap();
    println!("# -----  List of tables without `updated_at`");
    println!("{db1_tables:?}");
    println!("# ---------------");
    let mut diff_io = config.diff_io.borrow_mut();
    diff_io.write((
        "========  Tables with `created_at` column but not `updated_at` difference between DBs"
            .to_string(),
        db1_tables,
        db2_tables,
    ));
    Ok(())
}

pub fn only_created_ats(config: &Config) -> Result<(), postgres::Error> {
    let db1_tables = non_updated_at_tables(config, MasterDB).unwrap();
    for table in db1_tables {
        compare_table_created_ats(config, &table)?;
    }
    Ok(())
}

pub fn all_columns(config: &Config) -> Result<(), postgres::Error> {
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

fn compare_table_created_ats(config: &Config, table: &str) -> Result<(), postgres::Error> {
    let builder = QueryBuilder::new(config).table(table).column(column());
    let (records1, records2) = rayon::join(
        || database::id_and_column_value(builder.build_master()).unwrap(),
        || database::id_and_column_value(builder.build_replica()).unwrap(),
    );

    let mut diff_io = config.diff_io.borrow_mut();
    diff_io.write((
        format!("====== `{table}` created_at values"),
        records1,
        records2,
    ));
    Ok(())
}

fn compare_rows(config: &Config, table: &str) -> Result<(), postgres::Error> {
    let records1 = database::full_row_ordered_by(config, MasterDB, table, column()).unwrap();
    let records2 = database::full_row_ordered_by(config, ReplicaDB, table, column()).unwrap();
    let mut diff_io = config.diff_io.borrow_mut();
    diff_io.write((format!("====== `{table}` all columns"), records1, records2));
    Ok(())
}
