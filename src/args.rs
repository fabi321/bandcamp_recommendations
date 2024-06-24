use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Parser)]
pub struct Args {
    /// Database for cache
    #[clap(long, short)]
    pub database: PathBuf,

    /// Listen address
    #[clap(long, short)]
    pub address: SocketAddr,

    /// Crawl all of bandcamp
    #[clap(long, short)]
    pub crawl: bool,
}
