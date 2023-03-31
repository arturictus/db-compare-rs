use super::par_run;
use crate::database::{self, DBResultType, RequestBuilder};
use crate::diff::IO;
use crate::Config;

pub fn run(config: &Config) -> Result<(), postgres::Error> {
    let tables = database::all_tables(RequestBuilder::new(config).build_master())?.to_s();
    for table in tables {
        let builder = RequestBuilder::new(config).table(&table);
        let (result1, result2) = par_run::<u32>(builder, database::count_for)?;

        let mut diff_io = config.diff_io.borrow_mut();
        diff_io.write(
            config,
            (
                format!("== `{table}` count"),
                DBResultType::Strings(vec![format!("{}", result1)]),
                DBResultType::Strings(vec![format!("{}", result2)]),
            ),
        );
    }
    Ok(())
}
