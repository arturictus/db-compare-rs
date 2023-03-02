use anyhow::{self, Ok};
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
        client
            .batch_execute(
                "
      CREATE TABLE IF NOT EXISTS users (
          id              SERIAL PRIMARY KEY,
          name            VARCHAR NOT NULL,
          created_at      INTEGER NOT NULL,
          updated_at      INTEGER NOT NULL
          )
  ",
            )
            .map_err(anyhow::Error::msg)?;

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
    fixture_file: String,
    runned: bool,
}

impl TestRunner {
    pub fn new(config: &Config) -> Self {
        fs::create_dir_all("tmp").unwrap();
        let tmp_file = format!("tmp/{}.diff", Uuid::new_v4());
        let fixture_file = format!(
            "tests/fixtures/examples/{}_{}_example.diff",
            config.white_listed_tables.clone().unwrap().join("_"),
            config.jobs.clone().iter().map(|j| j.to_string()).join("_")
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
            },
            regenerate_fixture: false,
            tmp_file,
            fixture_file,
            runned: false,
        }
    }
    #[allow(dead_code)]
    pub fn regenerate_fixture(mut self) -> Self {
        self.regenerate_fixture = true;
        self
    }
    fn fixture_not_exists(&self) -> bool {
        !Path::new(&self.fixture_file).exists()
    }

    pub fn run(mut self, exec: fn(&Config)) -> Self {
        // setup databases
        before_each().unwrap();
        exec(&self.config);
        if self.regenerate_fixture || self.fixture_not_exists() {
            println!("[TestRunner]: generating fixture: {}", self.fixture_file);
            // If we are creating the fixtures we copy the result to the fixture
            std::fs::copy(&self.tmp_file, &self.fixture_file).unwrap();
        }
        // Copy fixture and result to memory
        let tmp = std::fs::read_to_string(&self.tmp_file).unwrap();
        let fixture = std::fs::read_to_string(&self.fixture_file).unwrap();
        std::fs::remove_file(&self.tmp_file).unwrap();
        // Drop databases
        after_each().unwrap();
        // Assert the current output is the expected output
        assert_eq!(fixture, tmp);
        self.runned = true;
        self
    }
}

pub fn before_each() -> anyhow::Result<()> {
    // Ensure that the databases are clean before running the test
    DB::A.drop()?;
    DB::B.drop()?;
    // Setup the databases
    DB::A.setup()?;
    DB::B.setup()?;
    Ok(())
}
fn after_each() -> anyhow::Result<()> {
    // Clean up the databases
    DB::A.drop()?;
    DB::B.drop()?;
    Ok(())
}
#[derive(Debug, Clone)]
pub struct User {
    pub id: Option<u64>,
    pub name: String,
    pub created_at: i32,
    pub updated_at: i32,
}
impl Default for User {
    fn default() -> Self {
        User {
            id: None,
            name: "John".to_string(),
            created_at: 1,
            updated_at: 1,
        }
    }
}

impl User {
    pub fn all(db: DB) -> Vec<Self> {
        let mut client = db.connect().unwrap();
        let rows = client.query("SELECT * FROM users", &[]).unwrap();
        let mut users = Vec::new();
        for row in rows {
            // TODO: fix this
            // let id = row.get(0);
            users.push(User {
                id: None,
                name: row.get(1),
                created_at: row.get(2),
                updated_at: row.get(3),
            })
        }
        users
    }
    pub fn insert(&self, db: DB) -> anyhow::Result<User> {
        let mut client = db.connect()?;
        let id = client
            .execute(
                "INSERT INTO users (name, created_at, updated_at) VALUES ($1, $2, $3) RETURNING id",
                &[&self.name, &self.created_at, &self.updated_at],
            )
            .map_err(anyhow::Error::msg)
            .unwrap();
        Ok(User {
            id: Some(id),
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
            name: name.unwrap_or_else(|| format!("{}-{}", self.name.clone(), self.created_at + 1)),
            created_at: self.created_at + 1,
            updated_at: self.updated_at + 1,
        }
    }
}
