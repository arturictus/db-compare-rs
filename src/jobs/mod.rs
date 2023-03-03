mod all_columns;
mod counter;
mod last_created_records;
mod last_updated_records;
mod sequences;
mod utils;
use std::{fmt, str::FromStr};
pub(crate) use utils::par_run;

use crate::Config;
use anyhow::Result;

#[derive(Debug, PartialEq, Clone)]
pub enum Job {
    Counters,
    UpdatedAts,
    CreatedAts,
    AllColumns,
    Sequences,
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
            "all_columns" => Ok(Job::AllColumns),
            "sequences" => Ok(Job::Sequences),
            _ => Err(anyhow::anyhow!("Unknown job: {}", s)),
        }
    }
}

impl Job {
    fn run(&self, config: &Config) -> Result<(), postgres::Error> {
        match self {
            Job::Counters => counter::run(config),
            Job::UpdatedAts => {
                last_updated_records::tables(config)?;
                last_updated_records::only_updated_ats(config)?;
                last_updated_records::all_columns(config)?;
                Ok(())
            }
            Job::CreatedAts => {
                last_created_records::tables(config)?;
                last_created_records::only_created_ats(config)?;
                last_created_records::all_columns(config)?;
                Ok(())
            }
            Job::AllColumns => all_columns::run(config),
            Job::Sequences => sequences::run(config),
        }
    }

    pub fn all() -> Vec<Job> {
        vec![
            Job::Counters,
            Job::UpdatedAts,
            Job::CreatedAts,
            Job::AllColumns,
            Job::Sequences,
        ]
    }
}

pub fn run(config: &Config) -> Result<(), postgres::Error> {
    for job in &config.jobs {
        job.run(config)?;
    }
    Ok(())
}

// use crate::database::{Request, RequestBuilder};

// pub type DBResults<T: Send> = (T, T);

// pub fn par_run<T: Send>(
//     r: RequestBuilder,
//     f: fn(Request) -> Result<T, postgres::Error>,
// ) -> Result<DBResults<T>, postgres::Error> {
//     let (result1, result2) = rayon::join(|| f(r.build_master()), || f(r.build_replica()));

//     Ok((result1?, result2?))
// }
