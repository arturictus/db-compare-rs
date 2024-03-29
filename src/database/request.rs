use crate::Config;

#[derive(Clone, Debug, PartialEq, Default)]
pub enum DB {
    Master(String),
    Replica(String),
    #[default]
    None,
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
pub struct Request {
    pub config: DBConfig,
    pub db: DB,
    pub table: Option<String>,
    pub column: Option<String>,
    pub bounds: Option<(u32, u32)>,
    pub tm_cutoff: chrono::NaiveDateTime,
}

#[derive(Default, Clone)]
pub struct RequestBuilder {
    config: DBConfig,
    table: Option<String>,
    column: Option<String>,
    bounds: Option<(u32, u32)>,
    tm_cutoff: chrono::NaiveDateTime,
}

#[derive(Clone, Debug)]
pub struct DBConfig {
    pub db1: String,
    pub db2: String,
    pub white_listed_tables: Option<Vec<String>>,
    pub tls: bool,
    pub limit: u32,
}
impl Default for DBConfig {
    fn default() -> Self {
        Self {
            db1: "FAKE".to_string(),
            db2: "FAKE".to_string(),
            white_listed_tables: None,
            tls: true,
            limit: 100,
        }
    }
}

impl RequestBuilder {
    pub fn new(config: &Config) -> Self {
        RequestBuilder {
            config: DBConfig {
                db1: config.db1.clone(),
                db2: config.db2.clone(),
                white_listed_tables: config.white_listed_tables.clone(),
                tls: config.tls,
                limit: config.limit,
            },
            ..RequestBuilder::default()
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

    pub fn tm_cutoff(mut self, cutoff: chrono::NaiveDateTime) -> Self {
        self.tm_cutoff = cutoff;
        self
    }

    pub fn build_master(&self) -> Request {
        Request {
            config: self.config.clone(),
            db: DB::Master(self.config.db1.clone()),
            table: self.table.clone(),
            column: self.column.clone(),
            bounds: self.bounds,
            tm_cutoff: self.tm_cutoff,
        }
    }
    pub fn build_replica(&self) -> Request {
        Request {
            db: DB::Replica(self.config.db2.clone()),
            ..self.build_master()
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::cli;

    fn default_args() -> cli::Commands {
        cli::Commands::Run {
            db1: Some("db1".to_string()),
            db2: Some("db2".to_string()),
            limit: 1,
            no_tls: false,
            by_id_sample_size: None,
            output_folder: None,
            tables: None,
            config: None,
            tm_cutoff: None,
            jobs: None,
        }
    }
    fn config() -> Config {
        Config::new(&default_args())
    }

    #[test]
    fn test_query_builder() {
        let config = config();
        let builder = RequestBuilder::new(&config)
            .table("table")
            .bounds((1, 2))
            .column("column")
            .tm_cutoff(config.tm_cutoff);

        assert_eq!(builder.bounds, Some((1, 2)));
        assert_eq!(builder.column, Some("column".to_string()));
        assert_eq!(builder.table, Some("table".to_string()));
        // assert_eq!(builder.until, Some(3));

        let master = builder.build_master();
        assert_eq!(master.db, DB::Master("db1".to_string()));
        assert_eq!(master.column, Some("column".to_string()));
        assert_eq!(master.table, Some("table".to_string()));
        assert_eq!(master.bounds, Some((1, 2)));
        // assert_eq!(master.until, Some(3));

        let replica = builder.build_replica();
        assert_eq!(replica.db, DB::Replica("db2".to_string()));
        assert_eq!(replica.column, Some("column".to_string()));
        assert_eq!(replica.table, Some("table".to_string()));
        assert_eq!(replica.bounds, Some((1, 2)));
        // assert_eq!(replica.until, Some(3));
    }
}
