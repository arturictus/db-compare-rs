mod all_columns;
mod all_columns_excluding_ids;
mod counter;
mod last_created_records;
mod last_updated_records;
mod sequences;
mod updated_ats_until;
mod utils;
use std::{error, fmt, str::FromStr};
pub(crate) use utils::par_run;

use crate::Config;
use anyhow::Result;

#[derive(Debug, PartialEq, Clone)]
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
        write!(f, "{self:?}")
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
            "all_columns_excluding_replica_updated_ats" => Ok(Job::ByIDExcludingReplicaUpdatedAts),
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
                all_columns::run(config)?;
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
                all_columns_excluding_ids::run(config)?;
                Ok(())
            }
        }
    }

    pub fn all() -> Vec<Job> {
        vec![
            Job::Counters,
            Job::UpdatedAts,
            Job::CreatedAts,
            Job::ByID,
            Job::Sequences,
        ]
    }
}

pub fn run(config: &Config) -> Result<(), Box<dyn error::Error>> {
    for job in &config.jobs {
        job.run(config)?;
    }
    Ok(())
}
