import json
import os
import subprocess
import logging
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def _run_command(cmd, cwd=None):
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            check=True,
            encoding="utf-8",
            errors="replace",
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {' '.join(cmd)} - {e.stderr}")
        raise RuntimeError(f"Command failed: {e.stderr}")


def get_snapshot_commit(repo_path, date_str):
    """Find the commit closest to the given date (YYYY-MM)."""
    search_date = f"{date_str}-31"
    try:
        commit = _run_command(
            ["git", "rev-list", "-n", "1", f"--before={search_date}", "HEAD"],
            cwd=repo_path,
        )
        if commit:
            return commit
    except Exception:
        pass
    return None


def get_fossil_metadata(repo_path, commit_hash):
    """Find the oldest line in the repository at a specific commit."""
    if not commit_hash:
        return {}

    logger.info(f"Analyzing fossils for commit {commit_hash[:7]}...")
    _run_command(["git", "checkout", "--force", commit_hash], cwd=repo_path)
    files_output = _run_command(["git", "ls-files"], cwd=repo_path)
    files = [
        f
        for f in files_output.splitlines()
        if os.path.isfile(os.path.join(repo_path, f))
    ]

    oldest_fossil = {
        "timestamp": 2147483647,
        "file": "",
        "content": "",
        "year": "",
        "commit": "",
        "line": 0,
    }

    for file in files:
        try:
            blame_output = _run_command(
                ["git", "blame", "--line-porcelain", file], cwd=repo_path
            )
            current_commit_data = {}
            line_num = 0

            for line in blame_output.splitlines():
                if line.startswith("\t"):
                    line_num += 1
                    timestamp = current_commit_data.get("author-time")
                    if timestamp and timestamp < oldest_fossil["timestamp"]:
                        oldest_fossil["timestamp"] = timestamp
                        oldest_fossil["file"] = file
                        oldest_fossil["content"] = line.lstrip("\t").strip()
                        oldest_fossil["year"] = datetime.fromtimestamp(
                            timestamp, timezone.utc
                        ).strftime("%Y")
                        oldest_fossil["commit"] = current_commit_data.get("commit", "")[
                            :7
                        ]
                        oldest_fossil["line"] = line_num
                else:
                    if line and line[0] != "\t":
                        commit_hash = line.split(" ")[0]
                        if len(commit_hash) == 40:
                            current_commit_data["commit"] = commit_hash
                        elif line.startswith("author-time "):
                            parts = line.split(" ")
                            if len(parts) >= 2:
                                current_commit_data["author-time"] = int(parts[1])
        except Exception:
            continue

    return oldest_fossil


def backfill_fossils(data_dir, repo_urls):
    """
    Iterates through data files and adds fossil metadata.
    """
    data_path = Path(data_dir)
    json_files = list(data_path.glob("*.json"))
    temp_dir = Path("./temp_fossil_repos")
    temp_dir.mkdir(exist_ok=True)

    for json_file in json_files:
        if json_file.name == "manifest.json":
            continue

        repo_name = json_file.stem.replace("_data", "")
        repo_url = repo_urls.get(repo_name)

        if not repo_url:
            logger.warning(f"No URL found for {repo_name}, skipping.")
            continue

        logger.info(f"Processing {repo_name}...")

        # 1. Load data
        with open(json_file, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
            if isinstance(raw_data, list):
                snapshots = raw_data
                fossils = {}
            else:
                snapshots = raw_data.get("snapshots", [])
                fossils = raw_data.get("fossils", {})

        if not snapshots:
            continue

        # 2. Clone repo if needed
        local_repo = temp_dir / repo_name
        if not local_repo.exists():
            logger.info(f"Cloning {repo_url}...")
            _run_command(["git", "clone", repo_url, str(local_repo)])

        # 3. Resolve and Get Fossils
        try:
            if not fossils.get("genesis"):
                first_date = snapshots[0]["snapshot_date"]
                first_commit = get_snapshot_commit(local_repo, first_date)
                fossils["genesis"] = get_fossil_metadata(local_repo, first_commit)

            if not fossils.get("survivor"):
                last_date = snapshots[-1]["snapshot_date"]
                last_commit = get_snapshot_commit(local_repo, last_date)
                fossils["survivor"] = get_fossil_metadata(local_repo, last_commit)

            # 4. Write back
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(
                    {"snapshots": snapshots, "fossils": fossils},
                    f,
                    separators=(",", ":"),
                )
            logger.info(f"Successfully backfilled fossils for {repo_name}")

        except Exception as e:
            logger.error(f"Error backfilling {repo_name}: {e}")


if __name__ == "__main__":
    # Registry of repo URLs
    REPO_URLS = {
        "zed": "https://github.com/zed-industries/zed.git",
        "langchain": "https://github.com/langchain-ai/langchain.git",
        "numpy": "https://github.com/numpy/numpy.git",
        "react": "https://github.com/facebook/react.git",
        "claude-code": "https://github.com/anthropics/claude-code.git",
    }
    backfill_fossils("./data", REPO_URLS)
