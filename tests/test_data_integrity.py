"""
Tests data integrity and schema validation for Theseus generated JSON files
and frontend assets.
"""

# pylint: disable=too-many-locals
import json
from pathlib import Path

import pytest


def test_data_integrity_optimized_schema():
    """
    Test that the data follows the optimized schema:
    1. No 'total_lines' field (it's redundant)
    2. No future-year keys in 'composition'
    3. Supports both list and object schemas (backwards compatibility)
    """
    root_dir = Path(__file__).resolve().parent.parent
    data_dir = root_dir / "public" / "data" / "processed"
    if not data_dir.exists() or len(list(data_dir.glob("*.json"))) == 0:
        data_dir = root_dir / "data" / "processed"

    json_files = list(data_dir.glob("*.json"))
    json_files = [f for f in json_files if f.name != "manifest.json"]

    if len(json_files) == 0:
        pytest.skip(f"No data files found in {data_dir}")

    for json_file in json_files:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Support both list schema (legacy) and object schema (new)
        if isinstance(data, dict):
            snapshots = data.get("snapshots", [])
            fossils = data.get("fossils", {})
        else:
            snapshots = data
            fossils = {}

        for snapshot in snapshots:
            # 1. total_lines MUST be absent
            assert "total_lines" not in snapshot, (
                f"Error in {json_file.name}: 'total_lines' field should be "
                f"stripped for optimization but was found in {snapshot.get('snapshot_date')}"
            )

            # 2. Composition year check
            snapshot_date = snapshot.get("snapshot_date")
            if not snapshot_date:
                continue

            snapshot_year = int(snapshot_date[:4])
            composition = snapshot.get("composition", {})

            for year_key in composition.keys():
                year = int(year_key)
                assert year <= snapshot_year, (
                    f"Error in {json_file.name}: Snapshot {snapshot_date} "
                    f"contains impossible future year {year} in composition."
                )

        # 3. Validate fossil structure if present (only if not empty)
        for fossil_type, fossil_data in fossils.items():
            if not isinstance(fossil_data, dict):
                continue
            if not fossil_data:  # Empty fossil object is OK
                continue
            assert (
                "year" in fossil_data
            ), f"Error in {json_file.name}: Fossil '{fossil_type}' missing 'year' field"
            assert (
                "content" in fossil_data
            ), f"Error in {json_file.name}: Fossil '{fossil_type}' missing 'content' field"
            # New required fields: commit, line, file
            assert (
                "file" in fossil_data
            ), f"Error in {json_file.name}: Fossil '{fossil_type}' missing 'file' field"
            assert (
                "commit" in fossil_data
            ), f"Error in {json_file.name}: Fossil '{fossil_type}' missing 'commit' field"
            assert (
                "line" in fossil_data
            ), f"Error in {json_file.name}: Fossil '{fossil_type}' missing 'line' field"


def test_frontend_assets_modular_and_vendored():
    """
    Verify frontend asset modularization, offline D3 vendoring, and
    zero-cost GitHub issue submission.
    """
    root_dir = Path(__file__).resolve().parent.parent
    public_dir = root_dir / "public"

    # 1. D3 vendored locally inside public/vendor
    vendor_d3 = public_dir / "vendor" / "d3.v7.min.js"
    assert vendor_d3.is_file(), f"Missing vendored D3 library at {vendor_d3}"
    assert vendor_d3.stat().st_size > 100_000, "Vendored D3 file appears truncated"

    # 2. index.html references local vendor D3 and ES6 module app.js
    index_html = (public_dir / "index.html").read_text(encoding="utf-8")
    assert 'src="vendor/d3.v7.min.js"' in index_html, "index.html must load vendored D3 from vendor/"
    assert "https://d3js.org/d3.v7.min.js" not in index_html, "index.html must not depend on CDN D3"
    assert '<script type="module" src="app.js"></script>' in index_html, (
        "index.html must load app.js as an ES6 module"
    )

    # 3. Modular ES6 scripts exist in public
    for mod_name in ["api.js", "chart.js", "ui.js", "app.js"]:
        mod_path = public_dir / mod_name
        assert mod_path.is_file(), f"Missing ES6 module in public: {mod_name}"
        assert mod_path.stat().st_size > 0, f"Module {mod_name} is empty"

    # 4. Zero-cost repository request path (no Web3Forms access key)
    app_js = (public_dir / "app.js").read_text(encoding="utf-8")
    ui_js = (public_dir / "ui.js").read_text(encoding="utf-8")
    assert "__WEB3FORM_ACCESS_KEY__" not in app_js, "app.js must not contain Web3Forms key placeholder"
    assert "__WEB3FORM_ACCESS_KEY__" not in ui_js, "ui.js must not contain Web3Forms key placeholder"
    assert "api.web3forms.com" not in app_js, "app.js must not reference Web3Forms"
    assert "api.web3forms.com" not in ui_js, "ui.js must not reference Web3Forms"
    assert "https://github.com/Asifdotexe/Theseus/issues/new" in ui_js, (
        "ui.js must direct repo requests to pre-filled GitHub issue template"
    )


def test_cloudflare_static_asset_isolation():
    """
    Verify that wrangler.jsonc points exclusively to 'public' and that internal
    source code, build definitions, and raw data are isolated from the web root.
    """
    root_dir = Path(__file__).resolve().parent.parent
    public_dir = root_dir / "public"
    wrangler_path = root_dir / "wrangler.jsonc"

    # 1. Verify wrangler.jsonc points exclusively to public
    assert wrangler_path.is_file(), "Missing wrangler.jsonc configuration file"
    wrangler_text = wrangler_path.read_text(encoding="utf-8")
    wrangler_clean = "\n".join(
        line for line in wrangler_text.splitlines() if not line.strip().startswith("//")
    )
    wrangler_data = json.loads(wrangler_clean)
    assert wrangler_data.get("assets", {}).get("directory") == "public", (
        "wrangler.jsonc must point assets.directory exclusively to 'public'"
    )

    # 2. Verify all client-facing files reside inside public/
    assert (public_dir / "index.html").is_file()
    assert (public_dir / "style.css").is_file()
    assert (public_dir / "manifest.json").is_file()
    assert (public_dir / "robots.txt").is_file()
    assert (public_dir / "sitemap.xml").is_file()
    assert (public_dir / "theseus.config.json").is_file()
    assert (public_dir / "vendor" / "d3.v7.min.js").is_file()

    for mod in ["api.js", "chart.js", "ui.js", "app.js"]:
        assert (public_dir / mod).is_file(), f"Missing ES6 module in public: {mod}"

    # 3. Verify public processed graph data is populated
    public_processed = public_dir / "data" / "processed"
    assert public_processed.is_dir()
    assert len(list(public_processed.glob("*.json"))) > 0

    # 4. Verify internal files CANNOT be fetched from public
    internal_paths = [
        public_dir / "Cargo.toml",
        public_dir / "Cargo.lock",
        public_dir / "poetry.lock",
        public_dir / "pyproject.toml",
        public_dir / "engine",
        public_dir / "scripts",
        public_dir / "tests",
        public_dir / "data" / "raw",
        public_dir / "data" / "state",
        public_dir / "data" / ".status",
    ]
    for path in internal_paths:
        assert not path.exists(), f"Security leak: internal path found inside public directory: {path}"


if __name__ == "__main__":
    test_data_integrity_optimized_schema()
    test_frontend_assets_modular_and_vendored()
    test_cloudflare_static_asset_isolation()
    print("All data integrity, asset, and isolation checks passed!")
