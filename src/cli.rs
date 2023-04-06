use clap::{Parser, Subcommand};
const DEFAULT_LIMIT: u32 = 100;
#[derive(Debug, Parser)]
#[command(name = "db-compare")]
#[command(about = "Tools for comparing two Databases", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand, Clone)]
pub enum Commands {
    #[command(about = "Run two databases")]
    Run {
        #[arg(long)]
        db1: Option<String>,
        #[arg(long)]
        db2: Option<String>,
        #[arg(long, default_value_t = DEFAULT_LIMIT, help = "Queries limit, default: 100")]
        limit: u32,
        #[arg(
            long = "by-id-sample-size",
            help = "Max rows to compare for `by_id` and `by_id_excluding_replica_updated_ats` job"
        )]
        by_id_sample_size: Option<u32>,
        #[arg(long = "no-tls")]
        no_tls: bool,
        #[arg(
            long = "output-folder",
            help = "Destination folder for diff files, default: `./diffs`"
        )]
        output_folder: Option<String>,
        #[arg(long = "tables", help = "Comma separated list of tables to check")]
        tables: Option<String>,
        #[arg(
            long = "jobs",
            help = "Comma separated job list to run, default: `by_id_excluding_replica_updated_ats`, options: `counters, updated_ats, created_ats, by_id, by_id_excluding_replica_updated_ats`"
        )]
        jobs: Option<String>,
        #[arg(long, short, help = "Yaml config file")]
        config: Option<String>,
        #[arg(
            long,
            help = "Check rows until this timestamp: example: `--tm_cutoff $(date +%s)`, defaults to now. Affects jobs: `updated_ats`, 'created_ats' and `by_id_excluding_replica_updated_ats`"
        )]
        tm_cutoff: Option<i64>,
    },
    #[command(about = "Summarizes run result file")]
    Summarize {
        #[arg(long, short)]
        file: String,
    },
}
