#![allow(dead_code)]
use crate::Config;
use anyhow::Result;

#[derive(Clone, Debug, PartialEq)]
enum DB {
    Master(String),
    Replica(String),
    None,
}

impl Default for DB {
    fn default() -> Self {
        DB::None
    }
}

impl DB {
    pub fn name(&self) -> String {
        match self {
            Self::Master(_) => "DB1".to_string(),
            Self::Replica(_) => "DB2".to_string(),
            _ => panic!("Unset Database"),
        }
    }

    pub fn url(&self) -> String {
        match self {
            Self::Master(url) => url.clone(),
            Self::Replica(url) => url.clone(),
            _ => panic!("Unset Database"),
        }
    }
}
#[derive(Clone, Debug)]
pub struct DBQuery {
    config: QConfig,
    db: DB,
    table: Option<String>,
    column: Option<String>,
    bounds: Option<(u32, u32)>,
}

#[derive(Default, Clone, Debug)]
pub struct QueryBuilder {
    config: Option<QConfig>,
    table: Option<String>,
    column: Option<String>,
    bounds: Option<(u32, u32)>,
}

#[derive(Clone, Debug)]
struct QConfig {
    db1: String,
    db2: String,
    tls: bool,
    limit: u32,
}
impl Default for QConfig {
    fn default() -> Self {
        Self {
            db1: "FAKE".to_string(),
            db2: "FAKE".to_string(),
            tls: true,
            limit: 100,
        }
    }
}

impl QueryBuilder {
    pub fn new(config: &Config) -> Self {
        QueryBuilder {
            config: QConfig {
                db1: config.db1.clone(),
                db2: config.db2.clone(),
                tls: config.tls,
                limit: config.limit,
            }
            .into(),
            ..QueryBuilder::default()
        }
    }
    pub fn table(&mut self, table: impl Into<String>) -> &mut Self {
        self.table = Some(table.into());
        self
    }
    pub fn column(&mut self, column: impl Into<String>) -> &mut Self {
        self.column = Some(column.into());
        self
    }
    pub fn bounds(&mut self, bounds: (u32, u32)) -> &mut Self {
        self.bounds = Some(bounds.into());
        self
    }

    pub fn build(self) -> Result<(DBQuery, DBQuery)> {
        let master = DBQuery {
            config: self.config.clone().unwrap().clone(),
            db: DB::Master(self.config.clone().unwrap().db1.clone()),
            table: self.table,
            column: self.column,
            bounds: self.bounds,
        };
        let replica = DBQuery {
            db: DB::Replica(self.config.clone().unwrap().db2.clone()),
            ..master.clone()
        };
        Ok((master, replica))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::Args;

    fn default_args() -> Args {
        Args {
            db1: Some("db1".to_string()),
            db2: Some("db2".to_string()),
            limit: 1,
            no_tls: false,
            all_columns_sample_size: None,
            diff_file: None,
            tables_file: None,
            config: None,
        }
    }
    fn config() -> Config {
        Config::new(&default_args())
    }

    #[test]
    fn test_query_builder() {
        let config = config();
        let mut builder = QueryBuilder::new(&config);
        builder.table("table").bounds((1, 2)).column("column");

        assert_eq!(builder.bounds, Some((1, 2)));
        assert_eq!(builder.column, Some("column".to_string()));
        assert_eq!(builder.table, Some("table".to_string()));
        println!("{builder:#?}");

        let (master, replica) = builder.build().unwrap();
        assert_eq!(master.db, DB::Master("db1".to_string()));
        assert_eq!(master.column, Some("column".to_string()));
        assert_eq!(master.table, Some("table".to_string()));
        assert_eq!(master.bounds, Some((1, 2)));

        assert_eq!(replica.db, DB::Replica("db2".to_string()));
        assert_eq!(replica.column, Some("column".to_string()));
        assert_eq!(replica.table, Some("table".to_string()));
        assert_eq!(replica.bounds, Some((1, 2)));
    }
}
