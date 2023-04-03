use anyhow::{self, Ok};
use chrono::Days;
use chrono::NaiveDateTime;
use convert_case::{Case, Casing};
use db_compare::IOType;
use db_compare::*;
use itertools::Itertools;
use postgres::{Client, Error, NoTls};
use pretty_assertions::assert_eq;
use std::cell::RefCell;
use std::fs;
use std::ops::Add;
use std::path::Path;
use std::time::Duration;
use uuid::Uuid;

pub enum DB {
    A,
    B,
    Both,
}

impl DB {
    pub fn url(&self) -> String {
        self.not_both().unwrap();
        format!("{}/{}", self.host(), self.name())
    }
    fn host(&self) -> &str {
        "postgresql://postgres:postgres@127.0.0.1:5432"
    }
    fn name(&self) -> &str {
        match self {
            DB::A => "db_compare_test_db1",
            DB::B => "db_compare_test_db2",
            DB::Both => panic!("Both is not a valid database name"),
        }
    }
    fn host_connect(&self) -> Result<Client, Error> {
        Client::connect(self.host(), NoTls)
    }
    fn connect(&self) -> Result<Client, Error> {
        self.not_both().unwrap();
        Client::connect(&self.url(), NoTls)
    }
    fn setup(&self) -> anyhow::Result<()> {
        match self {
            DB::Both => {
                Self::A.setup()?;
                Self::B.setup()?;
            }
            _ => {
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
            id              INTEGER,
            name            VARCHAR NOT NULL,
            created_at      TIMESTAMP NOT NULL,
            updated_at      TIMESTAMP NOT NULL
            )
    ",
                )?;
                client.batch_execute(
                    "
        CREATE TABLE IF NOT EXISTS messages (
            id              INTEGER,
            txt             VARCHAR NOT NULL,
            created_at      TIMESTAMP NOT NULL
            )
    ",
                )?;
            }
        };

        Ok(())
    }
    fn drop(&self) -> anyhow::Result<()> {
        match self {
            DB::Both => {
                Self::A.drop()?;
                Self::B.drop()?;
            }
            _ => {
                let mut client = self.host_connect()?;
                let db_name = self.name();
                client
                    .batch_execute(&format!("DROP database {db_name}"))
                    .map_err(anyhow::Error::msg)
                    .unwrap_or_else(|_| {
                        println!("Database does not exists");
                    });
            }
        }

        Ok(())
    }

    fn not_both(&self) -> anyhow::Result<()> {
        match self {
            DB::Both => Err(anyhow::Error::msg("Both is not allowed for this operation")),
            _ => Ok(()),
        }
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
                white_listed_tables: config.white_listed_tables.clone(),
                jobs: config.jobs.clone(),
                by_id_sample_size: config.by_id_sample_size,
                tm_cutoff: config.tm_cutoff,
                ..Config::default()
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

    pub fn run(mut self, name: &str) -> Self {
        // setup databases
        before_each().unwrap();
        let (users, updated_at) = generate_users(40);
        let (msgs, _) = generate_msgs(40);
        seed_test_data(Some(&users), Some(&msgs));

        self.config.tm_cutoff = updated_at.add(Days::new(10));

        db_compare::run(&self.config).unwrap();

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
    DB::Both.drop().unwrap();
    // Setup the databases
    DB::Both.setup().unwrap();
    Ok(())
}
fn after_each() -> anyhow::Result<()> {
    // Clean up the databases
    DB::Both.drop()?;
    Ok(())
}
#[derive(Debug, Clone, PartialEq)]
pub struct User {
    pub id: i32,
    pub name: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}
impl Default for User {
    fn default() -> Self {
        User {
            id: 1,
            name: "John".to_string(),
            created_at: initial_datetime(),
            updated_at: initial_datetime(),
        }
    }
}

pub fn initial_timestamp() -> i64 {
    1_588_603_944 //Mon May 04 2020 14:52:24 GMT+0000
}

pub fn initial_datetime() -> chrono::NaiveDateTime {
    NaiveDateTime::from_timestamp_opt(initial_timestamp(), 0).unwrap()
}

impl User {
    #[allow(dead_code)]
    pub fn all(db: DB) -> Vec<Self> {
        let mut client = db.connect().unwrap();
        let rows = client.query("SELECT * FROM users", &[]).unwrap();
        let mut users = Vec::new();
        for row in rows {
            users.push(User {
                id: row.get::<&str, i32>("id"),
                name: row.get("name"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            })
        }
        users
    }
    pub fn insert(&self, db: DB) -> anyhow::Result<User> {
        let mut client = db.connect()?;
        let _id = client.execute(
            "INSERT INTO users (id, name, created_at, updated_at) VALUES ($1, $2, $3, $4) RETURNING id",
            &[&self.id, &self.name, &self.created_at, &self.updated_at],
        )?;
        Ok(self.clone())
    }
    pub fn new() -> Self {
        Self::default()
    }

    pub fn next(&self) -> Self {
        Self {
            id: self.id + 1,
            name: format!("{}-I", self.name.clone()),
            created_at: NaiveDateTime::from_timestamp_opt(self.created_at.timestamp() + 7200, 0)
                .unwrap(),
            updated_at: NaiveDateTime::from_timestamp_opt(self.updated_at.timestamp() + 7200, 0)
                .unwrap(),
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Msg {
    pub id: i32,
    pub txt: String,
    pub created_at: chrono::NaiveDateTime,
}
impl Default for Msg {
    fn default() -> Self {
        Self {
            id: 1,
            txt: "Important".to_string(),
            created_at: initial_datetime(),
        }
    }
}
impl Msg {
    #[allow(dead_code)]
    pub fn all(db: DB) -> Vec<Self> {
        let mut client = db.connect().unwrap();
        let rows = client.query("SELECT * FROM messages", &[]).unwrap();
        let mut msgs = Vec::new();
        for row in rows {
            msgs.push(Self {
                id: row.get::<&str, i32>("id"),
                txt: row.get("txt"),
                created_at: row.get("created_at"),
            })
        }
        msgs
    }
    pub fn insert(&self, db: DB) -> anyhow::Result<Self> {
        let mut client = db.connect()?;
        client.execute(
            "INSERT INTO messages (id, txt, created_at) VALUES ($1, $2, $3) RETURNING id",
            &[&self.id, &self.txt, &self.created_at],
        )?;
        Ok(self.clone())
    }
    pub fn new() -> Self {
        Self::default()
    }
    #[allow(dead_code)]
    pub fn next(&self) -> Self {
        Self {
            id: self.id + 1,
            txt: format!("{}-I", self.txt.clone()),
            created_at: NaiveDateTime::from_timestamp_opt(self.created_at.timestamp() + 7200, 0)
                .unwrap(),
        }
    }
}

fn generate_users(amount: u32) -> (Vec<User>, NaiveDateTime) {
    let first = User::new();
    let (_u, t, acc) = (1..=amount).fold(
        (first.clone(), first.updated_at, vec![first]),
        |(u, _t, mut acc), _i| {
            let u = u.next();
            let t = u.updated_at;
            acc.push(u.clone());
            (u, t, acc)
        },
    );

    (acc, t)
}
fn generate_msgs(amount: u32) -> (Vec<Msg>, NaiveDateTime) {
    let first = Msg::new();
    let (_u, t, acc) = (1..=amount).fold(
        (first.clone(), first.created_at, vec![first]),
        |(u, _t, mut acc), _i| {
            let u = u.next();
            let t = u.created_at;
            acc.push(u.clone());
            (u, t, acc)
        },
    );

    (acc, t)
}
fn seed_test_data(users: Option<&Vec<User>>, msgs: Option<&Vec<Msg>>) {
    if let Some(users) = users {
        for (i, u) in users.iter().enumerate() {
            let u = u.insert(DB::A).unwrap();
            if i % 2 == 0 {
                u.insert(DB::B).unwrap();
            }
            if i % 3 == 0 {
                let updated_at = if i > 20 {
                    u.updated_at.add(Days::new(30))
                } else {
                    u.updated_at
                };

                User {
                    name: format!("{} changed", u.name.clone()),
                    updated_at,
                    ..u.clone()
                }
                .insert(DB::B)
                .unwrap();
            }
        }
        users.last().unwrap().next().insert(DB::B).unwrap();
    }
    if let Some(msgs) = msgs {
        for (i, msg) in msgs.iter().enumerate() {
            let msg = msg.insert(DB::A).unwrap();
            if i % 2 == 0 {
                msg.insert(DB::B).unwrap();
            }
            if i % 3 == 0 {
                Msg {
                    txt: format!("{} changed", msg.txt.clone()),
                    ..msg.clone()
                }
                .insert(DB::B)
                .unwrap();
            }
        }
        msgs.last().unwrap().next().insert(DB::B).unwrap();
    }
}
