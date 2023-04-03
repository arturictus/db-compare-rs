use super::{par_run, Job, Output};
use crate::database::{self, DBResultType, RequestBuilder};
use crate::Config;

pub fn run(config: &Config) -> Result<(), postgres::Error> {
    let builder = RequestBuilder::new(config);
    let (mut data1, data2) = par_run(builder, database::get_sequences)?;

    data1.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

    let mut output = Output::new(config, Job::Sequences, Some("all_tables".to_string()));

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
    }
    output.end();
    Ok(())
}
