use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
pub struct Args {
    /// Database for cache
    #[clap(long, short)]
    pub database: PathBuf,
}
