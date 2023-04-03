use super::{par_run, Job, Output};
use crate::database::{self, DBResultType, RequestBuilder};
use crate::diff::IO;
use crate::Config;

pub fn run<'a>(config: &'a Config<'a>) -> Result<(), postgres::Error> {
    let builder = RequestBuilder::new(config);
    let (mut data1, data2) = par_run(builder, database::get_sequences)?;

    data1.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

    let mut output = Output::new(config, Job::Sequences, Some("all_tables".to_string()));

    let mut diff_io = config.diff_io.borrow_mut();
    for (table, num) in data1 {
        let found = data2
            .iter()
            .find(|(t, _)| t == &table)
            .map(|data| data.1.to_string());

        let result = (
            format!("== `{table}` sequence:"),
            DBResultType::Strings(vec![num.to_string()]),
            DBResultType::Strings(vec![found.unwrap_or_else(|| "Not set".to_string())]),
        );
        output.write(result.clone());
        diff_io.write(config, result);
    }
    output.end();
    Ok(())
}
