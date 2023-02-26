use crate::database::{self, RequestBuilder};
use crate::diff::IO;
use crate::Config;

pub fn run(config: &Config) -> Result<(), postgres::Error> {
    let query = RequestBuilder::new(config);
    let (mut data1, data2) = rayon::join(
        || database::get_sequences(query.build_master()).unwrap(),
        || database::get_sequences(query.build_replica()).unwrap(),
    );
    data1.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

    let mut diff_io = config.diff_io.borrow_mut();
    for (table, num) in data1 {
        let found = data2
            .iter()
            .find(|(t, _)| t == &table)
            .map(|data| data.1.to_string());
        diff_io.write((
            format!("== `{table}` sequence:"),
            vec![num.to_string()],
            vec![found.unwrap_or_else(|| "Not set".to_string())],
        ));
    }

    Ok(())
}
