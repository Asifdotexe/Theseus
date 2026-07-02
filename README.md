<div align="center">
  <img src="assets/theseus_favicon.png" alt="Ship of Theseus Logo" width="120" />
  <h1>Ship of Theseus</h1>
  <p><i>Does a codebase remain the same if every line is replaced? A monthly pulse on software entropy.</i></p>
  <p>
    <img src="https://img.shields.io/badge/Python-3.12%2B-blue?style=flat&logo=python&logoColor=white" alt="Python 3.12+" />
    <img src="https://img.shields.io/badge/Vanilla_JS-ES6-F7DF1E?style=flat&logo=javascript&logoColor=black" alt="Vanilla JS" />
    <img src="https://img.shields.io/badge/Deployed-GitHub_Pages-2EA44F?style=flat&logo=github" alt="Deployed on GitHub Pages" />
    <img src="https://img.shields.io/badge/GitHub_Actions-Automated-2088FF?style=flat&logo=github-actions&logoColor=white" alt="GitHub Actions" />
    <img src="https://img.shields.io/badge/Code_Style-Black-000000?style=flat&logo=python&logoColor=white" alt="Black Style" />
  </p>
</div>

---

## The philosophy

The Ship of Theseus is a thought experiment: if you replace every wooden plank on a ship, is it still the same ship? 

Software projects do this constantly. A repository can live for decades. The original developers leave, architectures shift, and eventually, the last line of the original commit is deleted. But the repo keeps its name and URL.

This project visualizes that process. It measures codebase churn by tracking when lines of code were written and how long they survive. 

People use this to:
- See how quickly a codebase is turning over. A stable architecture holds onto old code, while a frantic rewrite shows a sudden drop.
- Find the "fossils" — the oldest surviving lines of code that somehow escaped refactoring.
- Look at the history of frameworks like React or Django to see exactly when major rewrites actually shipped.

## Setup

You will need `git`, `python` (3.12+), and `poetry`.

```bash
git clone https://github.com/Asifdotexe/Theseus.git
cd Theseus
poetry install
```

### Analyzing a repository

The script reads from `theseus.config.json`. To run a full analysis:

```bash
poetry run python scripts/analyse_repository.py
```

To update the pointers to the oldest surviving lines of code (the "fossils"):

```bash
poetry run python scripts/add_fossils.py --update-survivor
```

### Viewing the chart

Open `index.html` in a browser. 

```bash
# macOS
open index.html

# Windows
start index.html
```

## Documentation

- [Architecture & Data Pipeline](docs/ARCHITECTURE.md): How the script traverses git history and captures fossils.
- [Configuration Guide](docs/CONFIGURATION.md): How to analyze your own repositories.
- [DevOps & CI/CD](docs/DEVOPS.md): How the GitHub Actions pipeline runs updates automatically.

## License

This project is available under the terms defined in the `LICENSE` file.
