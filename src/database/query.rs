#![allow(dead_code)]
use crate::Config;
use anyhow::Result;

#[derive(Clone, Debug, PartialEq)]
pub enum DB {
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
    pub config: QConfig,
    pub db: DB,
    pub table: Option<String>,
    pub column: Option<String>,
    pub bounds: Option<(u32, u32)>,
}

#[derive(Default, Clone, Debug)]
pub struct QueryBuilder {
    config: QConfig,
    table: Option<String>,
    column: Option<String>,
    bounds: Option<(u32, u32)>,
}

#[derive(Clone, Debug)]
pub struct QConfig {
    pub db1: String,
    pub db2: String,
    pub tls: bool,
    pub limit: u32,
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
    pub fn table(mut self, table: impl Into<String>) -> Self {
        self.table = Some(table.into());
        self
    }
    pub fn column(mut self, column: impl Into<String>) -> Self {
        self.column = Some(column.into());
        self
    }
    pub fn bounds(mut self, bounds: (u32, u32)) -> Self {
        self.bounds = Some(bounds);
        self
    }

    pub fn build_master(self) -> Result<DBQuery> {
        Ok(DBQuery {
            config: self.config.clone(),
            db: DB::Master(self.config.db1),
            table: self.table,
            column: self.column,
            bounds: self.bounds,
        })
    }

    pub fn build(self) -> Result<(DBQuery, DBQuery)> {
        let master = DBQuery {
            config: self.config.clone(),
            db: DB::Master(self.config.db1),
            table: self.table,
            column: self.column,
            bounds: self.bounds,
        };
        let replica = DBQuery {
            db: DB::Replica(self.config.db2),
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
        let builder = QueryBuilder::new(&config)
            .table("table")
            .bounds((1, 2))
            .column("column");

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
