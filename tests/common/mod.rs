use anyhow::{self, Ok};
use chrono::NaiveDateTime;
use convert_case::{Case, Casing};
use db_compare::IOType;
use db_compare::*;
use itertools::Itertools;
use postgres::{Client, Error, NoTls};
use std::cell::RefCell;
use std::fs;
use std::path::Path;
use uuid::Uuid;

pub enum DB {
    A,
    B,
}

impl DB {
    pub fn url(&self) -> String {
        format!("{}/{}", self.host(), self.name())
    }
    fn host(&self) -> &str {
        "postgresql://postgres:postgres@127.0.0.1:5432"
    }
    fn name(&self) -> &str {
        match self {
            DB::A => "db_compare_test_db1",
            DB::B => "db_compare_test_db2",
        }
    }
    fn host_connect(&self) -> Result<Client, Error> {
        Client::connect(self.host(), NoTls)
    }
    fn connect(&self) -> Result<Client, Error> {
        Client::connect(&self.url(), NoTls)
    }
    fn setup(&self) -> anyhow::Result<()> {
        let mut client = self.host_connect()?;
        let db_name = self.name();
        client
            .batch_execute(&format!("CREATE DATABASE {db_name}"))
            .unwrap_or_else(|_| {
                println!("Database already exists");
            });

        let mut client = self.connect()?;
        client.batch_execute(
            "
      CREATE TABLE IF NOT EXISTS users (
          id              SERIAL PRIMARY KEY,
          name            VARCHAR NOT NULL,
          created_at      TIMESTAMP NOT NULL,
          updated_at      TIMESTAMP NOT NULL
          )
  ",
        )?;
        client.batch_execute(
            "
      CREATE TABLE IF NOT EXISTS messages (
          id              SERIAL PRIMARY KEY,
          txt            VARCHAR NOT NULL,
          created_at      TIMESTAMP NOT NULL
          )
  ",
        )?;

        Ok(())
    }
    fn drop(&self) -> anyhow::Result<()> {
        let mut client = self.host_connect()?;
        let db_name = self.name();
        client
            .batch_execute(&format!("DROP database {db_name}"))
            .map_err(anyhow::Error::msg)
            .unwrap_or_else(|_| {
                println!("Database does not exists");
            });
        Ok(())
    }
}

pub struct TestRunner {
    config: Config,
    regenerate_fixture: bool,
    tmp_file: String,
    fixture_folder: String,
    runned: bool,
}

impl TestRunner {
    pub fn new(config: &Config) -> Self {
        fs::create_dir_all("tmp").unwrap();
        let tmp_file = format!("tmp/{}.diff", Uuid::new_v4());
        let fixture_folder = format!(
            "tests/fixtures/examples/{}_{}",
            config.white_listed_tables.clone().unwrap().join("_"),
            config
                .jobs
                .clone()
                .iter()
                .map(|j| j.to_string())
                .join("_")
                .to_case(Case::Snake)
        );

        Self {
            config: Config {
                diff_io: RefCell::new(IOType::new_from_path(tmp_file.clone())),
                db1: config.db1.clone(),
                db2: config.db2.clone(),
                limit: config.limit,
                tls: false,
                white_listed_tables: config.white_listed_tables.clone(),
                jobs: config.jobs.clone(),
                all_columns_sample_size: config.all_columns_sample_size,
                rows_until: config.rows_until,
            },
            regenerate_fixture: false,
            tmp_file,
            fixture_folder,
            runned: false,
        }
    }
    #[allow(dead_code)]
    pub fn regenerate_fixture(mut self) -> Self {
        self.regenerate_fixture = true;
        self
    }
    fn fixture_file(&self, name: &str) -> String {
        let name = name.to_case(Case::Lower).to_case(Case::Snake);
        format!("{}/{}.diff", self.fixture_folder, name)
    }
    fn fixture_not_exists(&self, name: &str) -> bool {
        !Path::new(&self.fixture_file(name)).exists()
    }

    pub fn run(mut self, name: &str, exec: fn(&Config)) -> Self {
        // setup databases
        before_each().unwrap();
        exec(&self.config);
        let fixture_file = self.fixture_file(name);
        if self.regenerate_fixture || self.fixture_not_exists(name) {
            fs::create_dir_all(&self.fixture_folder).unwrap_or_else(|_| {
                panic!("unable to create folder {}", &self.fixture_folder);
            });
            println!(
                "[TestRunner]: generating fixture: {}",
                self.fixture_file(name)
            );
            // If we are creating the fixtures we copy the result to the fixture
            std::fs::copy(&self.tmp_file, &fixture_file).unwrap();
        }
        // Copy fixture and result to memory
        let tmp = std::fs::read_to_string(&self.tmp_file).unwrap();
        let fixture = std::fs::read_to_string(&fixture_file).unwrap();
        std::fs::remove_file(&self.tmp_file).unwrap();
        // Drop databases
        after_each().unwrap();
        println!("comparing: result with {}", &fixture_file);
        // Assert the current output is the expected output
        assert_eq!(fixture, tmp);
        self.runned = true;
        self
    }
}

pub fn before_each() -> anyhow::Result<()> {
    // Ensure that the databases are clean before running the test
    DB::A.drop().unwrap();
    DB::B.drop().unwrap();
    // Setup the databases
    DB::A.setup().unwrap();
    DB::B.setup().unwrap();
    Ok(())
}
fn after_each() -> anyhow::Result<()> {
    // Clean up the databases
    DB::A.drop()?;
    DB::B.drop()?;
    Ok(())
}
#[derive(Debug, Clone, PartialEq)]
pub struct User {
    pub id: Option<i32>,
    pub name: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}
impl Default for User {
    fn default() -> Self {
        let d = chrono::NaiveDate::from_ymd_opt(2015, 6, 3).unwrap();
        let t = chrono::NaiveTime::from_hms_milli_opt(12, 34, 56, 789).unwrap();
        let dt = NaiveDateTime::new(d, t);
        User {
            id: None,
            name: "John".to_string(),
            created_at: dt,
            updated_at: dt,
        }
    }
}

impl User {
    pub fn all(db: DB) -> Vec<Self> {
        let mut client = db.connect().unwrap();
        let rows = client.query("SELECT * FROM users", &[]).unwrap();
        let mut users = Vec::new();
        for row in rows {
            users.push(User {
                id: Some(row.get::<&str, i32>("id").into()),
                name: row.get("name"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            })
        }
        users
    }
    pub fn insert(&self, db: DB) -> anyhow::Result<User> {
        let mut client = db.connect()?;
        let id = client.execute(
            "INSERT INTO users (name, created_at, updated_at) VALUES ($1, $2, $3) RETURNING id",
            &[&self.name, &self.created_at, &self.updated_at],
        )?;
        Ok(User {
            id: Some(id.try_into().unwrap()),
            ..self.clone()
        })
    }
    pub fn new() -> Self {
        Self::default()
    }
    #[allow(dead_code)]
    pub fn next(&self, name: Option<String>) -> Self {
        Self {
            id: None,
            name: name.unwrap_or_else(|| {
                format!("{}-{}", self.name.clone(), chrono::Utc::now().to_string())
            }),

            created_at: NaiveDateTime::from_timestamp_millis(1_662_921_288).unwrap(),
            updated_at: NaiveDateTime::from_timestamp_millis(1_662_921_288).unwrap(),
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Msg {
    pub id: Option<i32>,
    pub txt: String,
    pub created_at: chrono::NaiveDateTime,
}
impl Default for Msg {
    fn default() -> Self {
        let d = chrono::NaiveDate::from_ymd_opt(2015, 6, 3).unwrap();
        let t = chrono::NaiveTime::from_hms_milli_opt(12, 34, 56, 789).unwrap();
        let dt = NaiveDateTime::new(d, t);
        Self {
            id: None,
            txt: "Important".to_string(),
            created_at: dt,
        }
    }
}
impl Msg {
    pub fn all(db: DB) -> Vec<Self> {
        let mut client = db.connect().unwrap();
        let rows = client.query("SELECT * FROM messages", &[]).unwrap();
        let mut msgs = Vec::new();
        for row in rows {
            msgs.push(Self {
                id: Some(row.get::<&str, i32>("id").into()),
                txt: row.get("txt"),
                created_at: row.get("created_at"),
            })
        }
        msgs
    }
    pub fn insert(&self, db: DB) -> anyhow::Result<Self> {
        let mut client = db.connect()?;
        let id = client.execute(
            "INSERT INTO messages (txt, created_at) VALUES ($1, $2) RETURNING id",
            &[&self.txt, &self.created_at],
        )?;
        Ok(Self {
            id: Some(id.try_into().unwrap()),
            ..self.clone()
        })
    }
    pub fn new() -> Self {
        Self::default()
    }
    #[allow(dead_code)]
    pub fn next(&self, name: Option<String>) -> Self {
        Self {
            id: None,
            txt: name.unwrap_or_else(|| {
                format!("{}-{}", self.txt.clone(), chrono::Utc::now().to_string())
            }),

            created_at: NaiveDateTime::from_timestamp_millis(1_662_921_288).unwrap(),
        }
    }
}
