use clap::Parser;
use log::{info, error};
use std::path::PathBuf;
use std::process::exit;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the local git repository
    #[arg(short, long)]
    repo_path: PathBuf,

    /// Output path for the history data
    #[arg(short, long)]
    output: PathBuf,

    /// Only process specific YYYY-MM period
    #[arg(long)]
    reprocess: Option<String>,
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    
    let args = Args::parse();

    if !args.repo_path.exists() {
        error!("Repository path does not exist: {:?}", args.repo_path);
        exit(1);
    }

    info!("Starting Theseus Engine for {:?}", args.repo_path);

    // TODO: 1. Initialize git2::Repository
    // TODO: 2. Implement `get_snapshot_periods`
    // TODO: 3. Implement full & incremental blame logic with rayon
    // TODO: 4. Output to jsonl format matching Python expectations
    
    info!("Engine initialized successfully. Pipeline integration pending.");
}
