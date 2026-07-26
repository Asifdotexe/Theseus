use clap::Parser;
use log::{error, info, warn};
use rayon::prelude::*;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::time::Instant;

use chrono::{TimeZone, Utc};
use git2::{BlameOptions, ObjectType, Oid, Repository, Tree};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    repo_path: PathBuf,

    #[arg(short, long)]
    output: PathBuf,

    #[arg(short, long)]
    state: PathBuf,

    #[arg(long)]
    reprocess: Option<String>,
}

#[derive(Serialize, serde::Deserialize)]
struct SnapshotData {
    snapshot_date: String,
    commit_hash: String,
    composition: HashMap<String, u32>,
}

type FileCompositions = HashMap<String, HashMap<String, u32>>;

fn get_snapshot_periods(repo_path: &Path) -> Result<Vec<(String, Oid)>, git2::Error> {
    let repo = Repository::open(repo_path)?;
    let mut revwalk = repo.revwalk()?;
    revwalk.push_head()?;
    revwalk.set_sorting(git2::Sort::TIME | git2::Sort::REVERSE)?;

    let mut periods: HashMap<String, Oid> = HashMap::new();

    for oid_res in revwalk {
        let oid = oid_res?;
        let commit = repo.find_commit(oid)?;
        let time = commit.committer().when();
        let datetime = Utc.timestamp_opt(time.seconds(), 0).unwrap();
        let period = datetime.format("%Y-%m").to_string();

        // Overwrites so the LAST commit in chronological order for that month is kept
        periods.insert(period, oid);
    }

    let mut filtered = Vec::new();
    let quarterly_months = ["03", "06", "09", "12"];

    for (period, oid) in periods {
        let year: i32 = period[..4].parse().unwrap_or(0);
        let month = &period[5..7];

        if year >= 2025 {
            filtered.push((period, oid));
        } else if quarterly_months.contains(&month) {
            filtered.push((period, oid));
        }
    }

    filtered.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(filtered)
}

fn get_tracked_files(repo: &Repository, tree: &Tree) -> Result<HashSet<String>, git2::Error> {
    let mut files = HashSet::new();
    tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
        if entry.kind() == Some(ObjectType::Blob) {
            let path = format!("{}{}", root, entry.name().unwrap_or(""));

            // Check if binary or empty
            if let Ok(blob) = repo.find_blob(entry.id()) {
                if !blob.is_binary() && blob.size() > 0 {
                    files.insert(path);
                }
            }
        }
        git2::TreeWalkResult::Ok
    })?;
    Ok(files)
}

fn get_changed_files(
    repo: &Repository,
    old_tree: &Tree,
    new_tree: &Tree,
) -> Result<HashSet<String>, git2::Error> {
    let mut diffopts = git2::DiffOptions::new();
    let diff = repo.diff_tree_to_tree(Some(old_tree), Some(new_tree), Some(&mut diffopts))?;

    let mut changed_files = HashSet::new();
    diff.print(git2::DiffFormat::NameOnly, |delta, _, _| {
        if let Some(path) = delta.new_file().path() {
            if let Some(path_str) = path.to_str() {
                changed_files.insert(path_str.to_string());
            }
        }
        true
    })?;
    Ok(changed_files)
}

fn process_blame(
    repo_path: &Path,
    commit_oid: Oid,
    files_to_blame: Vec<String>,
) -> FileCompositions {
    let results: Vec<(String, HashMap<String, u32>)> = files_to_blame
        .into_par_iter()
        .map_init(
            || Repository::open(repo_path).ok(),
            |repo_opt, path| {
                let repo = repo_opt.as_ref()?;
                let mut opts = BlameOptions::new();
                opts.newest_commit(commit_oid);

                let blame = repo.blame_file(Path::new(&path), Some(&mut opts)).ok()?;

                let mut year_counts = HashMap::new();
                for hunk in blame.iter() {
                    let time = hunk.final_signature().when();
                    let datetime = Utc.timestamp_opt(time.seconds(), 0).unwrap();
                    let year = datetime.format("%Y").to_string();
                    let lines = hunk.lines_in_hunk() as u32;

                    *year_counts.entry(year).or_insert(0) += lines;
                }

                Some((path, year_counts))
            },
        )
        .filter_map(|x| x)
        .collect();

    results.into_iter().collect()
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();

    if !args.repo_path.exists() {
        error!("Repository path does not exist: {:?}", args.repo_path);
        exit(1);
    }

    info!("Starting Theseus Engine for {:?}", args.repo_path);

    let repo_path = args.repo_path.clone();
    let output_path = args.output.clone();

    let periods = get_snapshot_periods(&repo_path).unwrap_or_else(|e| {
        error!("Failed to get snapshot periods: {}", e);
        exit(1);
    });

    info!("Found {} snapshot periods to process.", periods.len());

    let mut prev_tree: Option<Tree> = None;
    let mut file_compositions: FileCompositions = HashMap::new();

    // Make sure output directory exists
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent).unwrap();
    }

    let reprocess_val = args.reprocess.as_deref().unwrap_or("");
    let mut is_append = false;
    let mut processed_periods = HashSet::new();

    if reprocess_val != "all" && output_path.exists() {
        is_append = true;
        use std::io::{BufRead, BufReader};
        if let Ok(file) = std::fs::File::open(&output_path) {
            let reader = BufReader::new(file);
            for line in reader.lines().flatten() {
                if let Ok(snap) = serde_json::from_str::<SnapshotData>(&line) {
                    processed_periods.insert(snap.snapshot_date);
                }
            }
        }
    }

    if reprocess_val == "last" && !processed_periods.is_empty() {
        if let Some(max_period) = processed_periods.iter().max().cloned() {
            processed_periods.remove(&max_period);
        }
    } else if reprocess_val != "all" && !reprocess_val.is_empty() {
        processed_periods.remove(reprocess_val);
    }

    if is_append && args.state.exists() {
        if let Ok(file) = std::fs::File::open(&args.state) {
            if let Ok(state) = serde_json::from_reader(file) {
                file_compositions = state;
            }
        }
    }

    // Truncate if we are starting fresh (or reprocess=all)
    if !is_append && output_path.exists() {
        let _ = std::fs::remove_file(&output_path);
    } else if is_append && output_path.exists() {
        // Rewrite output file to remove lines that we are reprocessing
        use std::io::{BufRead, BufReader};
        let mut valid_lines = Vec::new();
        if let Ok(file) = std::fs::File::open(&output_path) {
            let reader = BufReader::new(file);
            for line in reader.lines().flatten() {
                if let Ok(snap) = serde_json::from_str::<SnapshotData>(&line) {
                    if processed_periods.contains(&snap.snapshot_date) {
                        valid_lines.push(line);
                    }
                }
            }
        }
        if let Ok(mut temp_out) = std::fs::File::create(&output_path) {
            for line in valid_lines {
                let _ = writeln!(temp_out, "{}", line);
            }
        }
    }

    let mut out_file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(&output_path)
        .unwrap();

    let start_overall = Instant::now();

    for (idx, (period, commit_oid)) in periods.iter().enumerate() {
        let repo = Repository::open(&repo_path).unwrap();
        let curr_commit = repo.find_commit(*commit_oid).unwrap();
        let curr_tree = curr_commit.tree().unwrap();

        if processed_periods.contains(period) {
            info!(
                "[{}/{}] Period {} already processed. Skipping.",
                idx + 1,
                periods.len(),
                period
            );
            prev_tree = Some(curr_tree);
            continue;
        }

        let tracked_files = get_tracked_files(&repo, &curr_tree).unwrap();

        let files_to_blame = if let Some(old_tree) = &prev_tree {
            let changed = get_changed_files(&repo, old_tree, &curr_tree).unwrap();
            // Retain compositions only for files still tracked and not changed
            file_compositions.retain(|f, _| tracked_files.contains(f) && !changed.contains(f));

            // Files to blame are changed files that are still tracked
            changed
                .into_iter()
                .filter(|f| tracked_files.contains(f))
                .collect::<Vec<_>>()
        } else {
            // No previous tree, blame all tracked files
            tracked_files.into_iter().collect::<Vec<_>>()
        };

        info!(
            "[{}/{}] Period {}: blaming {} files...",
            idx + 1,
            periods.len(),
            period,
            files_to_blame.len()
        );

        let start_blame = Instant::now();
        let new_compositions = process_blame(&repo_path, *commit_oid, files_to_blame);
        file_compositions.extend(new_compositions);

        // Aggregate
        let mut age_distribution: HashMap<String, u32> = HashMap::new();
        for f_comp in file_compositions.values() {
            for (year, count) in f_comp {
                *age_distribution.entry(year.clone()).or_insert(0) += count;
            }
        }

        let total_lines: u32 = age_distribution.values().sum();
        info!(
            "Period {} done in {:.2?}. Total lines: {}",
            period,
            start_blame.elapsed(),
            total_lines
        );

        let snapshot = SnapshotData {
            snapshot_date: period.clone(),
            commit_hash: commit_oid.to_string(),
            composition: age_distribution,
        };

        let json = serde_json::to_string(&snapshot).unwrap();
        writeln!(out_file, "{}", json).unwrap();

        if let Some(parent) = args.state.parent() {
            let _ = fs::create_dir_all(parent);
        }
        if let Ok(state_file) = std::fs::File::create(&args.state) {
            let _ = serde_json::to_writer(state_file, &file_compositions);
        }

        prev_tree = Some(curr_tree);
    }

    info!("Engine finished in {:.2?}", start_overall.elapsed());
}
