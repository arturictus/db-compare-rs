use crate::queries;
use crate::{Args, DBsResult};
use postgres::{Error, SimpleQueryMessage};

pub fn run(args: &Args, presenter: fn(DBsResult)) -> Result<(), postgres::Error> {
    let count1 = count(args, &args.db1).unwrap();
    let count2 = count(args, &args.db2).unwrap();

    presenter(("======== Counts for all tables".to_string(), count1, count2));
    Ok(())
}

fn count(args: &Args, db_url: &str) -> Result<Vec<String>, Error> {
    let mut client = queries::connect(args, db_url)?;

    let tables = queries::all_tables(args, db_url)?;
    println!("== QUERY: Getting counts from db");
    let mut counts = Vec::new();
    for table_name in tables {
        for row in client.simple_query(&format!("SELECT COUNT(*) FROM {};", table_name)) {
            for data in row {
                match data {
                    SimpleQueryMessage::Row(result) => {
                        counts.push(format!(
                            "{} : {}",
                            table_name.clone(),
                            result.get(0).unwrap_or("0").parse::<u32>().unwrap()
                        ));
                        println!("== RESULT: count from  {}", table_name);
                    }

                    _ => (),
                }
            }
        }
    }
    counts.sort();
    Ok(counts)
}
