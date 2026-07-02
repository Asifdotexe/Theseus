# Configuration guide

The Python backend and the JavaScript frontend both read from `theseus.config.json` to determine which repositories to analyze and display.

## Base schema (`theseus.config.json`)

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

### Global settings

* `dataDir` *(string)*: The relative path where the Python script saves JSON files. The frontend uses this to fetch data. Default is `"./data"`.

### Repositories array

The `repositories` array takes objects with the following keys:

| Key | Type | Description | Example |
| :--- | :---: | :--- | :--- |
| `name` | *String* | A unique identifier used for the repo slug (`--repo NAME`) and filenames. Must be kebab-case. | `"django"` |
| `repo` | *String* | The GitHub repository namespace (resolves to `https://github.com/owner/repo.git`). | `"django/django"` |
| `description` | *String* | A short subheading clarifying the project's purpose. | `"The web framework for perfectionists with deadlines."` |
| `milestones` | *Array* | An optional list of events to display on the timeline. | `[{"date": "2024-01", "title": "Launch"}]` |

## Milestone structure

Objects in the `milestones` array use these properties:

| Key | Type | Description | Example |
| :--- | :---: | :--- | :--- |
| `date` | *String* | The date in `YYYY-MM` format. | `"2024-06"` |
| `title` | *String* | The event name shown in tooltips. | `"Monorepo Migration"` |
| `description` | *String* | A short explanation of the event. | `"Unified all integrations into a single repository."` |

## Adding a new repository

Add this block to the `repositories` array in `theseus.config.json`:

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

Run the pipeline to generate the data:

```bash
python -m scripts.run_pipeline --repo REPO-NAME
```

This command clones the repository, runs the snapshot analysis, extracts the fossils, and writes two files:
- `data/raw/{name}_data.json` — raw data with per-file blame metadata.
- `data/processed/{name}_graph.json` — graph data formatted for the frontend.

The frontend automatically detects the new data in `data/processed/`.

> [!NOTE]
> Data filenames are derived directly from the `name` field. You do not need to specify file paths manually in the configuration.

> [!CAUTION]
> Do not modify the output data in `data/` manually. Doing so corrupts the incremental snapshot cache, forcing a full re-analysis.
