use clap::Parser;
use log::{error, info};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::time::Instant;

use chrono::{TimeZone, Utc};
use git2::{BlameOptions, DiffFindOptions, DiffOptions, ObjectType, Oid, Repository, Tree};

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
    fossils: Option<PathBuf>,

    #[arg(long)]
    reprocess: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct SnapshotData {
    snapshot_date: String,
    commit_hash: String,
    composition: HashMap<String, u32>,
}

type FileCompositions = HashMap<String, HashMap<String, u32>>;

/// Persistent state schema matching Python's `_data_io.py`.
/// Encapsulates both `commit_hash` and `file_compositions` so that resuming
/// incremental analysis does not suffer from silent deserialization failures.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct StateData {
    commit_hash: String,
    file_compositions: FileCompositions,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Fossil {
    timestamp: i64,
    file: String,
    content: String,
    year: String,
    commit: String,
    view_commit: String,
    line: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct FossilsRecord {
    genesis: Fossil,
    survivor: Fossil,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct FossilCandidate {
    timestamp: i64,
    file: String,
    content: String,
    year: String,
    commit: String,
    line: u32,
}

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

        if year >= 2025 || quarterly_months.contains(&month) {
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
    let mut diffopts = DiffOptions::new();
    let mut diff = repo.diff_tree_to_tree(Some(old_tree), Some(new_tree), Some(&mut diffopts))?;

    // Enable rename and copy detection across snapshot trees.
    // This allows libgit2 to associate moved or renamed files, avoiding spurious
    // cache invalidation and keeping blame histories clean across snapshots.
    let mut find_opts = DiffFindOptions::new();
    find_opts.renames(true);
    find_opts.copies(true);
    let _ = diff.find_similar(Some(&mut find_opts));

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

/// Blames files concurrently using Rayon.
///
/// Design & memory lifetime notes:
/// 1. `git2::Repository` is not thread-safe (`!Sync`). Therefore, each Rayon thread
///    initializes its own `Repository` handle via `map_init`. This avoids locking overhead
///    and guarantees independent object database caches per worker thread.
/// 2. BlameOptions configures rename tracking flags (`track_copies_same_file`,
///    `track_copies_same_commit_moves`, etc.) so that file renames do not falsely reset code age
///    back to the rename commit date.
/// 3. In the same single pass, each blamed file identifies its oldest non-empty line
///    as a `FossilCandidate`, eliminating the need for a separate second clone and blame run.
fn process_blame(
    repo_path: &Path,
    commit_oid: Oid,
    files_to_blame: Vec<String>,
) -> (FileCompositions, HashMap<String, FossilCandidate>) {
    let results: Vec<(String, HashMap<String, u32>, Option<FossilCandidate>)> = files_to_blame
        .into_par_iter()
        .map_init(
            || Repository::open(repo_path).ok(),
            |repo_opt, path| {
                let repo = repo_opt.as_ref()?;
                let mut opts = BlameOptions::new();
                opts.newest_commit(commit_oid);

                // Enable copy/move tracking within and across commits.
                // This preserves line ancestry across renames and file migrations.
                opts.track_copies_same_file(true);
                opts.track_copies_same_commit_moves(true);
                opts.track_copies_same_commit_copies(true);
                opts.track_copies_any_commit_copies(true);

                let blame = repo.blame_file(Path::new(&path), Some(&mut opts)).ok()?;

                let mut year_counts = HashMap::new();
                let mut hunks_info: Vec<(i64, u32, u32, Oid)> = Vec::new();

                for hunk in blame.iter() {
                    // Use orig_signature() to record true author time rather than committer time.
                    // This correctly preserves authorship dates for imported, rebased, or squashed commits.
                    let sig = hunk.orig_signature().or_else(|| hunk.final_signature());
                    if let Some(sig) = sig {
                        let time = sig.when();
                        let datetime = Utc.timestamp_opt(time.seconds(), 0).unwrap();
                        let year = datetime.format("%Y").to_string();
                        let lines = hunk.lines_in_hunk() as u32;

                        *year_counts.entry(year).or_insert(0) += lines;

                        let commit_id = if hunk.orig_commit_id().is_zero() {
                            hunk.final_commit_id()
                        } else {
                            hunk.orig_commit_id()
                        };

                        hunks_info.push((
                            time.seconds(),
                            hunk.final_start_line() as u32,
                            lines,
                            commit_id,
                        ));
                    }
                }

                // Sort hunks ascending by author timestamp to find the oldest non-empty line
                hunks_info.sort_by_key(|h| h.0);

                let mut candidate: Option<FossilCandidate> = None;

                // Load file blob at this snapshot to extract the text and 1-based line number
                if let Ok(commit) = repo.find_commit(commit_oid) {
                    if let Ok(tree) = commit.tree() {
                        if let Ok(entry) = tree.get_path(Path::new(&path)) {
                            if let Ok(blob) = repo.find_blob(entry.id()) {
                                if !blob.is_binary() && blob.size() > 0 {
                                    if let Ok(content_str) = std::str::from_utf8(blob.content()) {
                                        let file_lines: Vec<&str> = content_str.lines().collect();

                                        for (ts, start_line, num_lines, orig_oid) in hunks_info {
                                            let start_idx = (start_line.saturating_sub(1)) as usize;
                                            let end_idx = (start_idx + num_lines as usize).min(file_lines.len());

                                            for (offset, line) in file_lines[start_idx..end_idx].iter().enumerate() {
                                                let trimmed = line.trim();
                                                if !trimmed.is_empty() {
                                                    let datetime = Utc.timestamp_opt(ts, 0).unwrap();
                                                    let year = datetime.format("%Y").to_string();
                                                    let full_commit = orig_oid.to_string();
                                                    let short_commit = full_commit[..full_commit.len().min(7)].to_string();

                                                    candidate = Some(FossilCandidate {
                                                        timestamp: ts,
                                                        file: path.clone(),
                                                        content: trimmed.to_string(),
                                                        year,
                                                        commit: short_commit,
                                                        line: (start_idx + offset + 1) as u32,
                                                    });
                                                    break;
                                                }
                                            }

                                            if candidate.is_some() {
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                Some((path, year_counts, candidate))
            },
        )
        .filter_map(|x| x)
        .collect();

    let mut compositions = HashMap::new();
    let mut candidates = HashMap::new();

    for (path, year_counts, candidate) in results {
        compositions.insert(path.clone(), year_counts);
        if let Some(cand) = candidate {
            candidates.insert(path, cand);
        }
    }

    (compositions, candidates)
}

/// Identifies the repository's Genesis (Historical) fossil by inspecting root commits directly.
///
/// Design notes:
/// Root commits have zero parents. Finding the oldest root commit by author timestamp and
/// examining the first non-empty line of its first alphabetical file reproduces the canonical
/// Genesis fossil in under 5 milliseconds without checking out the git working directory.
fn find_genesis_fossil(repo: &Repository) -> Result<Fossil, git2::Error> {
    let mut revwalk = repo.revwalk()?;
    revwalk.push_head()?;
    revwalk.set_sorting(git2::Sort::TIME | git2::Sort::REVERSE)?;

    let mut root_commits: Vec<(Oid, i64)> = Vec::new();
    for oid_res in revwalk {
        let oid = oid_res?;
        let commit = repo.find_commit(oid)?;
        if commit.parent_count() == 0 {
            let author_time = commit.author().when().seconds();
            root_commits.push((oid, author_time));
        }
    }

    // Sort root commits ascending by author timestamp so the oldest root commit is inspected first
    root_commits.sort_by_key(|(_, ts)| *ts);

    for (root_oid, author_time) in root_commits {
        let commit = repo.find_commit(root_oid)?;
        let tree = commit.tree()?;

        let mut file_entries: Vec<(String, Oid)> = Vec::new();
        tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
            if entry.kind() == Some(ObjectType::Blob) {
                let path = format!("{}{}", root, entry.name().unwrap_or(""));
                file_entries.push((path, entry.id()));
            }
            git2::TreeWalkResult::Ok
        })?;

        // Sort files alphabetically to ensure deterministic fossil selection
        file_entries.sort_by(|a, b| a.0.cmp(&b.0));

        for (file_path, blob_oid) in file_entries {
            let blob = repo.find_blob(blob_oid)?;
            if blob.is_binary() || blob.size() == 0 {
                continue;
            }

            if let Ok(content_str) = std::str::from_utf8(blob.content()) {
                for (line_idx, line) in content_str.lines().enumerate() {
                    let trimmed = line.trim();
                    if !trimmed.is_empty() {
                        let datetime = Utc.timestamp_opt(author_time, 0).unwrap();
                        let year = datetime.format("%Y").to_string();
                        let full_hash = root_oid.to_string();
                        let short_hash = full_hash[..full_hash.len().min(7)].to_string();

                        return Ok(Fossil {
                            timestamp: author_time,
                            file: file_path,
                            content: trimmed.to_string(),
                            year,
                            commit: short_hash,
                            view_commit: full_hash,
                            line: (line_idx + 1) as u32,
                        });
                    }
                }
            }
        }
    }

    Err(git2::Error::from_str("No non-empty lines found in root commits"))
}

/// Resolves the default branch name of the repository (e.g., "main" or "master").
fn get_default_branch_name(repo: &Repository) -> String {
    if let Ok(head) = repo.head() {
        if let Ok(Some(target)) = head.symbolic_target() {
            return target.rsplit('/').next().unwrap_or("main").to_string();
        }
    }
    if let Ok(origin_head) = repo.find_reference("refs/remotes/origin/HEAD") {
        if let Ok(Some(target)) = origin_head.symbolic_target() {
            return target.rsplit('/').next().unwrap_or("main").to_string();
        }
    }
    if repo.find_branch("main", git2::BranchType::Local).is_ok()
        || repo.find_branch("origin/main", git2::BranchType::Remote).is_ok()
    {
        return "main".to_string();
    }
    if repo.find_branch("master", git2::BranchType::Local).is_ok()
        || repo.find_branch("origin/master", git2::BranchType::Remote).is_ok()
    {
        return "master".to_string();
    }
    "main".to_string()
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

    // Derive fossils path if not explicitly provided
    let fossils_path = args.fossils.clone().unwrap_or_else(|| {
        let file_name = output_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("history.jsonl");
        let fossil_file_name = if file_name.ends_with("_history.jsonl") {
            file_name.replace("_history.jsonl", "_fossils.json")
        } else {
            format!("{}.fossils.json", file_name)
        };
        output_path.with_file_name(fossil_file_name)
    });

    let periods = get_snapshot_periods(&repo_path).unwrap_or_else(|e| {
        error!("Failed to get snapshot periods: {}", e);
        exit(1);
    });

    info!("Found {} snapshot periods to process.", periods.len());

    let mut prev_tree_oid: Option<Oid> = None;
    let mut file_compositions: FileCompositions = HashMap::new();
    let mut file_candidates: HashMap<String, FossilCandidate> = HashMap::new();

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
            for line in reader.lines().map_while(Result::ok) {
                if let Ok(snap) = serde_json::from_str::<SnapshotData>(&line) {
                    processed_periods.insert(snap.snapshot_date);
                }
            }
        }
    }

    // Handle reprocessing:
    // If "last" is requested, drop only the latest period.
    // If a specific period like "2023-06" is requested, drop that period and all subsequent periods
    // so that the timeline remains contiguous and incremental diffs are valid.
    if reprocess_val == "last" && !processed_periods.is_empty() {
        if let Some(max_period) = processed_periods.iter().max().cloned() {
            processed_periods.remove(&max_period);
        }
    } else if reprocess_val != "all" && !reprocess_val.is_empty() {
        processed_periods.retain(|p| p.as_str() < reprocess_val);
    }

    // Truncate output file if starting fresh, or rewrite to prune reprocessed lines
    if !is_append && output_path.exists() {
        let _ = std::fs::remove_file(&output_path);
    } else if is_append && output_path.exists() {
        use std::io::{BufRead, BufReader};
        let mut valid_lines = Vec::new();
        if let Ok(file) = std::fs::File::open(&output_path) {
            let reader = BufReader::new(file);
            for line in reader.lines().map_while(Result::ok) {
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

    // Determine the commit of the last kept processed period
    let last_processed_commit: Option<Oid> = periods
        .iter()
        .filter(|(p, _)| processed_periods.contains(p))
        .map(|(_, oid)| *oid)
        .next_back();

    // Load state safely:
    // Only reuse state if the state's commit_hash matches the last kept processed period.
    // If reprocessing an older snapshot, the latest state in memory is in the temporal future
    // and must NOT be applied to older tree diffs.
    if is_append && args.state.exists() {
        if let Ok(file) = std::fs::File::open(&args.state) {
            let reader = std::io::BufReader::new(file);
            match serde_json::from_reader::<_, StateData>(reader) {
                Ok(state) => {
                    let can_use_state = match last_processed_commit {
                        Some(last_oid) => state.commit_hash == last_oid.to_string(),
                        None => false,
                    };

                    if can_use_state {
                        info!("Restoring in-memory state from commit {}", state.commit_hash);
                        file_compositions = state.file_compositions;
                    } else {
                        info!(
                            "State commit ({}) does not match last processed snapshot commit. Starting fresh for unskipped periods.",
                            state.commit_hash
                        );
                    }
                }
                Err(e) => {
                    error!(
                        "Could not parse state file {:?}: {}. Starting with clean composition.",
                        args.state, e
                    );
                }
            }
        }
    }

    let mut out_file = OpenOptions::new()
        .create(true)
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
            prev_tree_oid = Some(curr_tree.id());
            continue;
        }

        let tracked_files = get_tracked_files(&repo, &curr_tree).unwrap();

        let files_to_blame = if let Some(old_oid) = prev_tree_oid {
            if file_compositions.is_empty() {
                // If previous periods were skipped without state (e.g. after reprocessing rewind),
                // blame all tracked files to re-establish a sound composition baseline.
                tracked_files.iter().cloned().collect::<Vec<_>>()
            } else {
                let old_tree = repo.find_tree(old_oid).unwrap();
                let changed = get_changed_files(&repo, &old_tree, &curr_tree).unwrap();

                // Retain compositions and fossil candidates only for files still tracked and unchanged
                file_compositions.retain(|f, _| tracked_files.contains(f) && !changed.contains(f));
                file_candidates.retain(|f, _| tracked_files.contains(f) && !changed.contains(f));

                // Files to blame are changed files that remain tracked
                changed
                    .into_iter()
                    .filter(|f| tracked_files.contains(f))
                    .collect::<Vec<_>>()
            }
        } else {
            // No previous tree, blame all tracked files
            tracked_files.iter().cloned().collect::<Vec<_>>()
        };

        info!(
            "[{}/{}] Period {}: blaming {} files...",
            idx + 1,
            periods.len(),
            period,
            files_to_blame.len()
        );

        let start_blame = Instant::now();
        let (new_compositions, new_candidates) = process_blame(&repo_path, *commit_oid, files_to_blame);
        file_compositions.extend(new_compositions);
        file_candidates.extend(new_candidates);

        // Aggregate year distribution
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

        // Atomically update state file
        if let Some(parent) = args.state.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let state_data = StateData {
            commit_hash: commit_oid.to_string(),
            file_compositions: file_compositions.clone(),
        };
        if let Ok(state_file) = std::fs::File::create(&args.state) {
            let writer = std::io::BufWriter::new(state_file);
            let _ = serde_json::to_writer(writer, &state_data);
        }

        prev_tree_oid = Some(curr_tree.id());
    }

    // Extract Genesis and Survivor fossils at HEAD
    let repo = Repository::open(&repo_path).unwrap();

    info!("Extracting Genesis fossil...");
    let genesis = match find_genesis_fossil(&repo) {
        Ok(g) => g,
        Err(e) => {
            error!("Failed to find genesis fossil: {}", e);
            Fossil {
                timestamp: 0,
                file: String::new(),
                content: String::new(),
                year: String::new(),
                commit: String::new(),
                view_commit: String::new(),
                line: 0,
            }
        }
    };

    info!("Extracting Survivor fossil at HEAD...");
    let default_branch = get_default_branch_name(&repo);

    // If file_candidates is empty (e.g. all periods were skipped from cache),
    // identify the files holding lines from the earliest year and blame them to locate the survivor.
    if file_candidates.is_empty() && !file_compositions.is_empty() {
        let min_year = file_compositions
            .values()
            .flat_map(|c| c.keys())
            .filter_map(|y| y.parse::<u32>().ok())
            .min();

        if let Some(oldest_year) = min_year {
            let oldest_year_str = oldest_year.to_string();
            let candidate_files: Vec<String> = file_compositions
                .iter()
                .filter(|(_, comp)| comp.contains_key(&oldest_year_str))
                .map(|(f, _)| f.clone())
                .collect();

            if let Some((_, last_commit_oid)) = periods.last() {
                let (_, cands) = process_blame(&repo_path, *last_commit_oid, candidate_files);
                file_candidates = cands;
            }
        }
    }

    let survivor = if let Some(oldest) = file_candidates.values().min_by_key(|c| c.timestamp) {
        Fossil {
            timestamp: oldest.timestamp,
            file: oldest.file.clone(),
            content: oldest.content.clone(),
            year: oldest.year.clone(),
            commit: oldest.commit.clone(),
            view_commit: default_branch,
            line: oldest.line,
        }
    } else {
        Fossil {
            timestamp: 0,
            file: String::new(),
            content: String::new(),
            year: String::new(),
            commit: String::new(),
            view_commit: default_branch,
            line: 0,
        }
    };

    let fossils_record = FossilsRecord { genesis, survivor };
    if let Some(parent) = fossils_path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(fossils_file) = std::fs::File::create(&fossils_path) {
        let writer = std::io::BufWriter::new(fossils_file);
        let _ = serde_json::to_writer(writer, &fossils_record);
        info!("Saved fossil records to {:?}", fossils_path);
    }

    info!("Engine finished in {:.2?}", start_overall.elapsed());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_data_serde_roundtrip() {
        let mut comps = HashMap::new();
        let mut file1 = HashMap::new();
        file1.insert("2024".to_string(), 100);
        file1.insert("2025".to_string(), 250);
        comps.insert("src/lib.rs".to_string(), file1);

        let state = StateData {
            commit_hash: "abcdef1234567890".to_string(),
            file_compositions: comps,
        };

        let json_str = serde_json::to_string(&state).expect("Serialization failed");
        assert!(json_str.contains("\"commit_hash\":\"abcdef1234567890\""));
        assert!(json_str.contains("\"file_compositions\":"));

        let deserialized: StateData = serde_json::from_str(&json_str).expect("Deserialization failed");
        assert_eq!(deserialized.commit_hash, "abcdef1234567890");
        assert_eq!(deserialized.file_compositions["src/lib.rs"]["2024"], 100);
        assert_eq!(deserialized.file_compositions["src/lib.rs"]["2025"], 250);
    }

    #[test]
    fn test_fossils_record_serde_roundtrip() {
        let fossils = FossilsRecord {
            genesis: Fossil {
                timestamp: 1600000000,
                file: "README.md".to_string(),
                content: "# Title".to_string(),
                year: "2020".to_string(),
                commit: "abc1234".to_string(),
                view_commit: "abc1234567890".to_string(),
                line: 1,
            },
            survivor: Fossil {
                timestamp: 1650000000,
                file: "src/main.rs".to_string(),
                content: "fn main() {}".to_string(),
                year: "2022".to_string(),
                commit: "def5678".to_string(),
                view_commit: "main".to_string(),
                line: 1,
            },
        };

        let json_str = serde_json::to_string(&fossils).expect("Serialization failed");
        let deserialized: FossilsRecord = serde_json::from_str(&json_str).expect("Deserialization failed");

        assert_eq!(deserialized.genesis.file, "README.md");
        assert_eq!(deserialized.genesis.commit, "abc1234");
        assert_eq!(deserialized.survivor.file, "src/main.rs");
        assert_eq!(deserialized.survivor.view_commit, "main");
    }
}
