use postgres::{Client, Error, NoTls};

pub enum DB {
    A,
    B,
}

impl DB {
    pub fn url(&self) -> String {
        format!("{}/{}", self.host(), self.name())
    }
    fn host(&self) -> &str {
        "postgresql://postgres:postgres@127.0.0.1"
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
    fn setup(&self) -> Result<(), Error> {
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
          created_at      INTEGER NOT NULL,
          updated_at      INTEGER NOT NULL
          )
  ",
        )?;

        Ok(())
    }
    fn drop(&self) -> Result<(), Error> {
        let mut client = self.host_connect()?;
        let db_name = self.name();
        client
            .batch_execute(&format!("DROP database {db_name}"))
            .unwrap_or_else(|_| {
                println!("Database does not exists");
            });
        Ok(())
    }
}

pub fn around(fun: fn() -> Result<(), postgres::Error>) {
    DB::A.setup().unwrap();
    DB::B.setup().unwrap();
    let r = fun();
    DB::A.drop().unwrap();
    DB::B.drop().unwrap();
    r.unwrap();
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
    pub fn new() -> Self {
        Self::default()
    }
    pub fn next(&self, name: Option<String>) -> Self {
        Self {
            id: None,
            name: name.unwrap_or_else(|| format!("{}-{}", self.name.clone(), self.created_at + 1)),
            created_at: self.created_at + 1,
            updated_at: self.updated_at + 1,
        }
    }
    pub fn insert(&self, db: DB) -> Result<User, Error> {
        let mut client = db.connect()?;
        let id = client.execute(
            "INSERT INTO users (name, created_at, updated_at) VALUES ($1, $2, $3) RETURNING id",
            &[&self.name, &self.created_at, &self.updated_at],
        )?;
        Ok(User {
            id: Some(id),
            ..self.clone()
        })
    }
}
