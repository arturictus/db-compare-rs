use super::{
    utils::{compare_table_for_all_columns, echo},
    Job, Output,
};
use crate::database;
use crate::database::RequestBuilder;
use crate::Config;

fn job() -> Job {
    Job::ByID
}

pub fn run(config: &Config) -> Result<(), postgres::Error> {
    let q = RequestBuilder::new(config).column("id");
    let tables = database::tables_with_column(q.build_master())?.to_s();
    for table in tables {
        let mut output = Output::new(config, job(), Some(table.clone()));
        echo(config, &format!("#start# Job: `by_id` Table: `{table}`"));

        compare_table_for_all_columns(&mut output, &table, None)?;

        echo(config, &format!("Job: `by_id` Table: `{table}` #end#"));
        output.end();
    }
    Ok(())
}
