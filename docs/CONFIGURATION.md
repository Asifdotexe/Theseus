# ⚙️ Configuration Guide

The Ship of Theseus engine operates centrally off a single file: `theseus.config.json`. By modifying this file, you instruct both the Python backend and the JavaScript frontend on which repositories to scrape and display.

## Base Schema (`theseus.config.json`)

```json
{
  "$schema": "./schema.json",
  "dataDir": "./data",
  "repositories": [
    {
      "name": "react",
      "repo": "facebook/react",
      "displayName": "React",
      "description": "A JavaScript library for building user interfaces"
    }
  ]
}
```

### Global Settings

* `dataDir` *(string)*: The relative path to the directory where the engine will save output JSONs. Usually `"./data"`. This config also controls the Javascript engine, so the frontend needs this accurate to know where to fetch data.

### Repositories Array

The `repositories` array takes objects consisting of the following key attributes:

| Key | Type | Description | Example |
| :--- | :---: | :--- | :--- |
| `name` | *String* | A safe, unique identifier. Used for the JSON filename (`{name}_data.json`). Must be snake_case or kebab-case. | `"django"` |
| `repo` | *String* | The GitHub repository namespace (the URL ending). The engine automatically strips trailing slashes and resolves this to `https://github.com/namespace/repo.git`. | `"django/django"` |
| `displayName` | *String* | The aesthetic name rendered on UI Cards. | `"Django"` |
| `description` | *String* | A short UI subheading clarifying what the project is. | `"The web framework for perfectionists with deadlines."` |

---

## Modifying Configurations

### Adding a new target
To begin visualizing a new repository, append it to the `repositories` array.

1. Add your object to `theseus.config.json`
2. Locally run `poetry run python scripts/analyse_repository.py`
3. The engine will clone the repo into `./temp_repos/` (which can be over `1GB` for massive codebases, so ensure disk space).
4. Local data processing will generate `data/{your_repo}_data.json`.
5. Run `poetry run python scripts/add_fossils.py` to fill in the Genesis/Survivor line references.
6. Check your `index.html` file to see the newly generated visual graph!

> [!CAUTION]
> Avoid modifying the output data within `data/` manually. Doing so will corrupt the incremental snapshot logic, forcing the pipeline to wipe out the cache and restart checking out massive commit trees from scratch.
