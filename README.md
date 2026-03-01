<h1 align="center">
  <span>𝙙𝙖𝙩𝙖 𝙡𝙤𝙖𝙙 𝙩𝙤𝙤𝙡</span>
  <br />
  <img src="./ressources/pictures/dlt_logo.png" alt="dlt logo" width="42" />
</h1>

<p align="center">
  <img src="./ressources/pictures/dlt_banner.png" alt="dlt banner" width="720" />
</p>

<p align="center">
  <em>From APIs to Warehouses: AI-Assisted Data Ingestion with dlt</em>
</p>

## About dlt

[dlt](https://dlthub.com/docs) (data load tool) is an open-source Python library for building reliable ELT pipelines. It helps you extract data from APIs or databases, normalize it into clean relational tables, and load it into destinations like DuckDB.

This repository now covers two complementary tracks:

1. `workshop`: Open Library quickstart and conceptual notebook
2. `Homework`: NYC Taxi API pipeline, data checks, and question answers

## Repository Layout

| Path | Purpose |
|------|---------|
| `workshop.md` | Full workshop instructions (Open Library) |
| `workshop/dlt_Pipeline_Overview.ipynb` | Intro notebook about dlt pipeline concepts |
| `Homework/README.md` | Homework questions and selected answers |
| `Homework/scripts/taxi_pipeline.py` | dlt pipeline for taxi API -> DuckDB |
| `Homework/scripts/explore_api.py` | Quick endpoint inspection helper |
| `Homework/notebooks/homework_notebook.ipynb` | Notebook analysis for homework |
| `Homework/notebooks/marimo_test_notebook.py` | Marimo notebook to explore the loaded taxi data |

## Prerequisites

- Python 3.13+ (project setting in `pyproject.toml`)
- `uv` (recommended) or `pip`
- Optional: an AI IDE (Cursor, Windsurf, VS Code + Copilot)

```bash
python --version
```

Install dependencies from the project root:

```bash
uv sync
```

Or install only homework dependencies:

```bash
pip install -r Homework/requirements.txt
```

## Track 1: Workshop (Open Library)

The workshop flow remains available in full detail in [workshop.md](./workshop.md).

For the conceptual walkthrough, open:
- `workshop/dlt_Pipeline_Overview.ipynb`

Core workshop commands:

```bash
pip install "dlt[workspace]"
dlt init dlthub:open_library duckdb
python open_library_pipeline.py
dlt pipeline open_library_pipeline show
```

## Track 2: Homework (Taxi Pipeline)

Run the homework pipeline from the `Homework` folder:

```bash
cd Homework
python scripts/explore_api.py
python scripts/taxi_pipeline.py
```

Inspect metadata and loaded data:

```bash
dlt pipeline taxi_pipeline show
dlt pipeline taxi_pipeline query "SELECT COUNT(*) AS nb_rows FROM taxi_dataset.taxi_trip;"
```

Then use [Homework/README.md](./Homework/README.md) and the notebook artifacts to validate answers:
- `Homework/notebooks/homework_notebook.ipynb`
- `Homework/ressources/*.png`

Open the marimo notebook for interactive SQL exploration:

```bash
cd Homework/notebooks
marimo edit marimo_test_notebook.py
```

Run it in app mode:

```bash
cd Homework/notebooks
marimo run marimo_test_notebook.py
```

## Resources

| Resource | Link |
|----------|------|
| dlt Documentation | [dlthub.com/docs](https://dlthub.com/docs) |
| Open Library Workspace Guide | [dlthub.com/workspace/source/open-library](https://dlthub.com/workspace/source/open-library) |
| dlt Dashboard Docs | [dlthub.com/docs/general-usage/dashboard](https://dlthub.com/docs/general-usage/dashboard) |
| marimo + dlt Guide | [dlthub.com/docs/general-usage/dataset-access/marimo](https://dlthub.com/docs/general-usage/dataset-access/marimo) |
| Open Library API | [openlibrary.org/developers/api](https://openlibrary.org/developers/api) |
| Homework Instructions (repo) | [Homework/README.md](./Homework/README.md) |
| Workshop Instructions (repo) | [workshop.md](./workshop.md) |

---

*Workshop and homework materials based on [dltHub](https://dlthub.com)*
