# ⚙️ Configuration Guide

The Ship of Theseus engine operates centrally off a single file: `theseus.config.json`. By modifying this file, you instruct both the Python backend and the JavaScript frontend on which repositories to scrape and display.

## Base Schema (`theseus.config.json`)

```json
{
  "dataDir": "./data",
  "repositories": [
    {
      "name": "react",
      "repo": "facebook/react",
      "description": "A JavaScript library for building user interfaces",
      "milestones": [
        { "date": "2013-05", "title": "Open Source", "description": "React is released." }
      ]
    }
  ]
}
```

### Global Settings

* `dataDir` *(string)*: The relative path to the directory where the engine saves output JSONs. Usually `"./data"`. The frontend uses this to know where to fetch data.

### Repositories Array

The `repositories` array takes objects consisting of the following key attributes:

| Key | Type | Description | Example |
| :--- | :---: | :--- | :--- |
| `name` | *String* | A safe, unique identifier. Used as the repo slug (`--repo NAME`) and as the data filenames — `data/raw/{name}_data.json` (raw with blame metadata) and `data/processed/{name}_graph.json` (graph for frontend). Must be kebab-case. | `"django"` |
| `repo` | *String* | The GitHub repository namespace. The engine resolves this to `https://github.com/owner/repo.git`. | `"django/django"` |
| `description` | *String* | A short UI subheading clarifying what the project is. | `"The web framework for perfectionists with deadlines."` |
| `milestones` | *Array* | An optional list of significant events to display on the timeline. | `[{"date": "2024-01", "title": "Launch"}]` |

---

## Milestone Structure

The `milestones` array contains objects with the following properties:

| Key | Type | Description | Example |
| :--- | :---: | :--- | :--- |
| `date` | *String* | The date of the milestone in `YYYY-MM` format. | `"2024-06"` |
| `title` | *String* | A short, catchy name for the event shown in tooltips. | `"Monorepo Migration"` |
| `description` | *String* | A concise explanation of the event. | `"Unified all integrations into a single repository."` |


---

## Adding a New Repository

Paste this template into the `repositories` array in `theseus.config.json`:

```json
    {
      "name": "REPO-NAME",
      "description": "Short description displayed on the dashboard",
      "repo": "OWNER/REPO-SLUG",
      "milestones": [
        {
          "date": "YYYY-MM",
          "title": "Brief milestone title",
          "description": "Optional longer description"
        }
      ]
    }
```

Then run the pipeline to generate the data:

```bash
python -m scripts.run_pipeline --repo REPO-NAME
```

This single command clones the repository, runs quarterly/monthly snapshot analysis, discovers both genesis and survivor fossils, and writes two files:
- `data/raw/{name}_data.json` — master data with per-file blame metadata (pipeline state)
- `data/processed/{name}_graph.json` — cleaned graph data for the frontend (only `snapshot_date` + `composition` per entry)

The frontend auto-discovers the new data from `data/processed/` — no additional changes needed.

> [!NOTE]
> Data filenames are derived from `name`: `data/raw/{name}_data.json` and `data/processed/{name}_graph.json`. There is no `file` field to maintain.

> [!CAUTION]
> Avoid modifying the output data within `data/` manually. Doing so can corrupt the incremental snapshot cache, forcing a full re-clone and re-analysis.
