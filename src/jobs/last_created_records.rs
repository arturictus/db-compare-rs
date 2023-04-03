use crate::database::{self, DBResultType, RequestBuilder};
use crate::jobs::Output;
use crate::Config;

use super::par_run;

pub fn tables(config: &Config) -> Result<(), postgres::Error> {
    let db1_tables = non_updated_at_tables(config)?;
    let db2_tables = non_updated_at_tables(config)?;
    println!("## List of tables without `updated_at` ##");
    println!("{db1_tables:?}");
    println!("## ----------- ##");
    let mut output = Output::new(
        config,
        crate::Job::CreatedAts,
        Some("tables_with_created_at".to_string()),
    );
    let result = (
        "Tables with `created_at` column but not `updated_at` difference between DBs".to_string(),
        DBResultType::Strings(db1_tables),
        DBResultType::Strings(db2_tables),
    );
    output.write(result);
    output.end();
    Ok(())
}

pub fn all_columns(config: &Config) -> Result<(), postgres::Error> {
    let db1_tables = non_updated_at_tables(config)?;
    for table in db1_tables {
        let mut output = Output::new(config, crate::Job::CreatedAts, None);
        compare_rows(&mut output, &table)?;
        output.end();
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

fn compare_rows(output: &mut Output, table: &str) -> Result<(), postgres::Error> {
    let config = output.config;
    let builder = RequestBuilder::new(config).table(table).column(column());
    let (records1, records2) = par_run(builder, database::full_row_ordered_by)?;

    let result = (format!("`{table}` all columns"), records1, records2);
    output.write(result);
    Ok(())
}
