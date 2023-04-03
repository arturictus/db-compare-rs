use super::{par_run, Job, Output};

use crate::database::{self, DBResultType, RequestBuilder};
use crate::Config;

pub fn run(config: &Config) -> Result<(), postgres::Error> {
    let tables = database::all_tables(RequestBuilder::new(config).build_master())?.to_s();
    let mut output = Output::new(config, Job::Counters, None);
    for table in tables {
        let builder = RequestBuilder::new(config).table(&table);
        let (result1, result2) = par_run::<u32>(builder, database::count_for)?;

        let diff_result = (
            format!("`{table}` count"),
            DBResultType::Strings(vec![format!("{}", result1)]),
            DBResultType::Strings(vec![format!("{}", result2)]),
        );
        output.write(diff_result);
    }
    output.end();
    Ok(())
}
