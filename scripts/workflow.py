"""
CLI helpers for theseus-engine.yml workflow steps.

Usage:
    python scripts/workflow.py discover-repos
    poetry run python scripts/workflow.py build-pr-body
    poetry run python scripts/workflow.py validate-graph-files
"""

import json
import sys
from pathlib import Path





def build_pr_body(
    status_dir: str = "data/.status", out_file: str = "pr-body.md"
) -> None:
    """Read status markers and write the PR summary markdown body."""
    status_dir_path = Path(status_dir)
    if not status_dir_path.is_dir():
        return

    statuses: dict[str, str] = {}
    for f in sorted(status_dir_path.glob("*.json")):
        with open(f, encoding="utf-8") as fh:
            s = json.load(fh)
            statuses[s["repo"]] = s["status"]

    total = len(statuses)
    passed = sum(1 for v in statuses.values() if v == "success")

    rows = "\n".join(
        f"| {repo} | {'✅' if s == 'success' else '❌'} |"
        for repo, s in sorted(statuses.items())
    )

    header = "## Automated Theseus Data Engine Run\n\n"
    table = "| Repo | Status |\n|------|--------|\n"
    total_row = f"| **Total** | **{passed}/{total} completed** |\n\n"
    footer = (
        "This pull request contains the latest pre-computed "
        "persistence data for the tracked repositories.\n\n"
        "**Trigger:** Monthly Schedule / Workflow Dispatch"
    )
    body = header + table + rows + "\n" + total_row + footer
    Path(out_file).write_text(body, encoding="utf-8")


def validate_graph_files(data_dir: str = "data/processed") -> None:
    """Validate all graph JSON files. Exits non-zero on failure."""
    processed_path = Path(data_dir)
    files = sorted(processed_path.glob("*.json"))

    if not files:
        print("No processed files found to validate.")
        sys.exit(1)

    errors = 0
    for f in files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            if "snapshots" not in data:
                raise ValueError(f"Missing snapshots in {f}")
            if "fossils" not in data:
                raise ValueError(f"Missing fossils in {f}")
            for snap in data["snapshots"]:
                if "snapshot_date" not in snap:
                    raise ValueError(f"Missing snapshot_date in {f}")
                if "composition" not in snap:
                    raise ValueError(f"Missing composition in {f}")
            print(f"  {f.name}")
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            print(f"  {f.name}: {e}")
            errors += 1

    if errors:
        print(f"Validation failed: {errors} error(s)")
        sys.exit(1)
    print("All graph files validated.")


def main() -> None:
    """CLI entry point: dispatch to subcommand."""
    command = sys.argv[1] if len(sys.argv) > 1 else ""

    if command == "build-pr-body":
        build_pr_body()
    elif command == "validate-graph-files":
        validate_graph_files()
    else:
        print(f"Usage: python {sys.argv[0]} <command>", file=sys.stderr)
        print(
            "Commands: build-pr-body, validate-graph-files",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
