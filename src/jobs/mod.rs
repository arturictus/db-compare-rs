mod by_id;
mod by_id_excluding_replica_updated_ats;
mod counter;
mod last_created_records;
mod last_updated_records;
mod sequences;
mod updated_ats_until;
mod utils;
use crate::{database::DBsResults, diff, IO};
use std::{error, fmt, str::FromStr};
pub(crate) use utils::par_run;

use crate::Config;
use anyhow::Result;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Job {
    Counters,
    UpdatedAts,
    CreatedAts,
    ByID,
    Sequences,
    UpdatedAtsUntil,
    ByIDExcludingReplicaUpdatedAts,
}

impl fmt::Display for Job {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let name = match self {
            Self::Counters => "counters".to_string(),
            Self::UpdatedAts => "updated_ats".to_string(),
            Self::CreatedAts => "created_ats".to_string(),
            Self::ByID => "by_id".to_string(),
            Self::Sequences => "sequences".to_string(),
            Self::UpdatedAtsUntil => "updated_ats_until".to_string(),
            Self::ByIDExcludingReplicaUpdatedAts => {
                "by_id_excluding_replica_updated_ats".to_string()
            }
        };
        write!(f, "{name}")
    }
}

impl FromStr for Job {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "counters" => Ok(Job::Counters),
            "updated_ats" => Ok(Job::UpdatedAts),
            "last_updated_ats" => Ok(Job::UpdatedAts),
            "created_ats" => Ok(Job::CreatedAts),
            "last_created_ats" => Ok(Job::CreatedAts),
            "by_id" => Ok(Job::ByID),
            "sequences" => Ok(Job::Sequences),
            "updated_ats_until" => Ok(Job::UpdatedAtsUntil),
            "by_id_excluding_replica_updated_ats" => Ok(Job::ByIDExcludingReplicaUpdatedAts),
            _ => Err(anyhow::anyhow!("Unknown job: {}", s)),
        }
    }
}

impl Job {
    fn run(&self, config: &Config) -> Result<(), Box<dyn error::Error>> {
        match self {
            Self::Counters => {
                counter::run(config)?;
                Ok(())
            }
            Self::UpdatedAts => {
                last_updated_records::tables(config)?;
                last_updated_records::all_columns(config)?;
                Ok(())
            }
            Self::CreatedAts => {
                last_created_records::tables(config)?;
                last_created_records::all_columns(config)?;
                Ok(())
            }
            Self::ByID => {
                by_id::run(config)?;
                Ok(())
            }
            Self::Sequences => {
                sequences::run(config)?;
                Ok(())
            }
            Self::UpdatedAtsUntil => {
                updated_ats_until::run(config)?;
                Ok(())
            }
            Self::ByIDExcludingReplicaUpdatedAts => {
                by_id_excluding_replica_updated_ats::run(config)?;
                Ok(())
            }
        }
    }

    pub fn default_list() -> Vec<Self> {
        vec![Self::ByIDExcludingReplicaUpdatedAts]
    }

    pub fn all() -> Vec<Self> {
        vec![
            Self::Counters,
            Self::UpdatedAts,
            Self::CreatedAts,
            Self::ByID,
            Self::Sequences,
        ]
    }
    pub fn diff_folder(&self, config: &Config) -> String {
        format!("{}/diffs/{self}", config.output_folder)
    }

    pub fn diff_file(&self, config: &Config, table: Option<&String>) -> diff::IOType {
        let path = format!(
            "{}/{}.diff",
            self.diff_folder(config),
            table.unwrap_or(&"all".to_string())
        );

        diff::IO::new_from_path(path)
    }
}

pub struct Output<'a> {
    config: &'a Config,
    job: Job,
    table: Option<String>,
    io: diff::IOType,
}

impl<'a> Output<'a> {
    pub fn new(config: &'a Config, job: Job, table: Option<String>) -> Self {
        let mut me = if config.test_env {
            return Self {
                config,
                job,
                table: table.clone(),
                io: diff::IOType::Phantom,
            };
        } else {
            Self {
                config,
                job,
                table: table.clone(),
                io: Self::diff_file(config, job, table.clone()),
            }
        };
        me.start();
        me
    }

    pub fn write(&mut self, results: DBsResults) {
        self.io.write(self.config, results);
    }

    fn start(&mut self) {
        self.io.echo(&format!("--- {} ---", self.config.db1));
        self.io.echo(&format!("+++ {} +++", self.config.db2));
        let table = format!(
            "Table: `{}`",
            self.table.as_ref().unwrap_or(&"all".to_string())
        );
        let msg = &format!("Job: `{}` {table}", self.job);
        self.io.start_block(msg);
    }

    pub fn end(&mut self) {
        let table = format!(
            "Table: `{}`",
            self.table.as_ref().unwrap_or(&"all".to_string())
        );
        let msg = &format!("Job: `{}` {table}", self.job);
        self.io.end_block(msg);
        self.io.close();
    }

    fn diff_file(config: &Config, job: Job, table: Option<String>) -> diff::IOType {
        job.diff_file(config, Some(&table.unwrap_or("all".to_string())))
    }
}

pub fn run(config: &Config) -> Result<(), Box<dyn error::Error>> {
    for job in &config.jobs {
        job.run(config)?;
    }
    Ok(())
}
