use super::utils::echo;
use crate::database::{self, DBResultType, RequestBuilder};
use crate::diff::IO;
use crate::Config;

use super::par_run;

pub fn tables(config: &Config) -> Result<(), postgres::Error> {
    let db1_tables = non_updated_at_tables(config)?;
    let db2_tables = non_updated_at_tables(config)?;
    println!("## List of tables without `updated_at` ##");
    println!("{db1_tables:?}");
    println!("## ----------- ##");
    let mut diff_io = config.diff_io.borrow_mut();
    diff_io.write(
        config,
        (
            "Tables with `created_at` column but not `updated_at` difference between DBs"
                .to_string(),
            DBResultType::Strings(db1_tables),
            DBResultType::Strings(db2_tables),
        ),
    );
    Ok(())
}

#[allow(dead_code)]
pub fn only_created_ats(config: &Config) -> Result<(), postgres::Error> {
    let db1_tables = non_updated_at_tables(config)?;
    for table in db1_tables {
        compare_table_created_ats(config, &table)?;
    }
    Ok(())
}

pub fn all_columns(config: &Config) -> Result<(), postgres::Error> {
    let db1_tables = non_updated_at_tables(config)?;
    for table in db1_tables {
        echo(
            config,
            &format!("#start# Job: last_created_ats Table: `{table}`"),
        );
        compare_rows(config, &table)?;
        echo(
            config,
            &format!("Job: last_created_ats Table: `{table}` #end#"),
        );
    }
    Ok(())
}

fn column() -> String {
    "created_at".to_string()
}

fn non_updated_at_tables(config: &Config) -> Result<Vec<String>, postgres::Error> {
    let q_created_at = RequestBuilder::new(config).column(column());
    let q_updated_at = RequestBuilder::new(config).column("updated_at");

    let (created_at_tables, r_updated_at_tables) = rayon::join(
        || database::tables_with_column(q_created_at.build_master()),
        || database::tables_with_column(q_updated_at.build_master()),
    );
    let updated_at_tables = r_updated_at_tables?.to_s();

    let difference = {
        let other = created_at_tables?.to_s();
        other
            .into_iter()
            .filter(|item| !updated_at_tables.contains(item))
            .collect()
    };

    Ok(difference)
}

#[allow(dead_code)]
fn compare_table_created_ats(config: &Config, table: &str) -> Result<(), postgres::Error> {
    let builder = RequestBuilder::new(config).table(table).column(column());
    let (records1, records2) = par_run(builder, database::id_and_column_value)?;

    let mut diff_io = config.diff_io.borrow_mut();
    diff_io.write(
        config,
        (format!("`{table}` created_at values"), records1, records2),
    );
    Ok(())
}

fn compare_rows(config: &Config, table: &str) -> Result<(), postgres::Error> {
    let builder = RequestBuilder::new(config).table(table).column(column());
    let (records1, records2) = par_run(builder, database::full_row_ordered_by)?;
    let mut diff_io = config.diff_io.borrow_mut();
    diff_io.write(
        config,
        (format!("`{table}` all columns"), records1, records2),
    );
    Ok(())
}
