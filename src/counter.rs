use crate::database;
use crate::diff::IO;
use crate::Config;

pub fn run(config: &Config) -> Result<(), postgres::Error> {
    let tables = database::all_tables(config, &config.args.db1)?;
    for table in tables {
        let result1 = database::count_for(config, &config.args.db1, &table).unwrap();
        let result2 = database::count_for(config, &config.args.db2, &table).unwrap();
        let mut diff_io = config.diff_io.borrow_mut();
        diff_io.write((
            format!("== `{table}`"),
            vec![format!("{result1}")],
            vec![format!("{result2}")],
        ));
    }
    Ok(())
}
