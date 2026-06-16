
"""
Fossil Update Orchestrator
==========================
Auto-detect missing fossils per repository and run the appropriate mode:

* No genesis fossil found → full backfill (genesis + survivor)
* Genesis exists → incremental survivor-only refresh

After all repos are processed, regenerates processed graph files.
"""

import logging
import sys
from pathlib import Path

from _data_io import load_snapshot_data
from _utils import load_config, remove_path
from add_fossils import backfill_fossils, update_survivor_fossils
from cleanup_data import generate_graph_data

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

TEMP_DIR = Path("./temp_fossil_repos")


def _fossil_identity(fossil: dict) -> tuple:
    return (fossil.get("file", ""), fossil.get("commit", ""))


def _check_genesis(data_dir: str, repo_name: str) -> str | None:
    raw_path = Path(data_dir) / "raw" / f"{repo_name}_data.json"
    if not raw_path.exists():
        return None
    try:
        data = load_snapshot_data(str(raw_path))
    except (FileNotFoundError, ValueError, KeyError):
        return None
    return data.get("fossils", {}).get("genesis", {}).get("file", "")


def _read_survivor_identity(data_dir: str, repo_name: str) -> tuple:
    raw_path = Path(data_dir) / "raw" / f"{repo_name}_data.json"
    try:
        data = load_snapshot_data(str(raw_path))
    except (FileNotFoundError, ValueError, KeyError):
        return ("", "")
    survivor = data.get("fossils", {}).get("survivor", {})
    return _fossil_identity(survivor)


def main() -> None:
    config = load_config()
    data_dir = config.get("dataDir", "./data")
    repo_urls = {
        repo["name"]: f"https://github.com/{repo['repo']}.git"
        for repo in config.get("repositories", [])
        if "name" in repo and "repo" in repo
    }
    if not repo_urls:
        logger.error("No valid repositories found in configuration.")
        sys.exit(1)

    results: list[dict] = []
    had_failures = False

    for repo_name, repo_url in repo_urls.items():
        raw_path = Path(data_dir) / "raw" / f"{repo_name}_data.json"
        if not raw_path.exists():
            logger.warning("  ⚠ No raw data file for %s, skipping.", repo_name)
            results.append({"repo": repo_name, "mode": "skipped", "status": "no raw data"})
            continue

        genesis_file = _check_genesis(data_dir, repo_name)

        if not genesis_file:
            logger.info("%s: missing genesis → full backfill", repo_name)
            error = backfill_fossils(data_dir, {repo_name: repo_url})
            if error:
                results.append({"repo": repo_name, "mode": "full backfill", "status": "error"})
                had_failures = True
            else:
                results.append({"repo": repo_name, "mode": "full backfill", "status": "ok"})
        else:
            before = _read_survivor_identity(data_dir, repo_name)
            logger.info("%s: genesis found → survivor incremental", repo_name)
            error = update_survivor_fossils(data_dir, {repo_name: repo_url})
            after = _read_survivor_identity(data_dir, repo_name)

            if error:
                results.append({"repo": repo_name, "mode": "survivor", "status": "error"})
                had_failures = True
            elif before != after:
                if before == ("", ""):
                    results.append({"repo": repo_name, "mode": "survivor", "status": "created new"})
                else:
                    results.append({"repo": repo_name, "mode": "survivor", "status": "updated"})
            else:
                results.append({"repo": repo_name, "mode": "survivor", "status": "no new information"})

    print()
    print("=" * 72)
    print("  Fossil Update Summary")
    print("=" * 72)
    repo_width = max(len(r["repo"]) for r in results) + 2 if results else 10
    for r in results:
        print(f"  {r['repo'].ljust(repo_width)}  {r['mode'].ljust(18)}  {r['status']}")
    print("=" * 72)
    print()

    logger.info("Regenerating processed graph data...")
    graph_error = generate_graph_data(data_dir)
    if graph_error:
        logger.error("Graph regeneration had errors.")

    if TEMP_DIR.exists():
        remove_path(str(TEMP_DIR))

    if had_failures:
        logger.error("One or more repositories failed. Exiting non-zero.")
        sys.exit(1)

    logger.info("Done.")


if __name__ == "__main__":
    main()
