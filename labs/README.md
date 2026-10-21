# Modern Data Engineering: labs

Hands-on notebooks for the SDAIA Academy **Modern Data Engineering** bootcamp (Data Engineering track). Every lab builds a piece of one platform for **Qafila**, a fictional quick-commerce grocery company in Riyadh, Jeddah and Dammam: from raw sources to a governed, AI-ready lakehouse.

## Quick start

**Google Colab**
1. Open a notebook from the [course page](https://sdaiaacademy.github.io/data-engineering/), or directly: `https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/<notebook>.ipynb`.
2. Run the first cell. It clones this repository, installs `requirements-colab.txt` and generates the data. A CPU runtime is enough for every lab.

**Local (any laptop CPU)**
```bash
git clone https://github.com/SDAIAAcademy/data-engineering
cd data-engineering/labs
uv sync                        # creates .venv from pyproject.toml (uv.lock pins every version)
uv run python prepare_data.py  # generates data/raw (≈35 MB, a few seconds)
uv run jupyter lab
```
Run `00_setup_check.ipynb` once before Day 1: it builds the shared warehouse, downloads the Day 4 embedding model and smoke-tests DuckDB, Iceberg and dbt.

New to SQL window functions? Work through `00_sql_windows.ipynb` before Day 1 (self-paced, about 90 minutes). It builds `PARTITION BY`, ranking, `LAG`/`LEAD`, frames and `QUALIFY` up from tables small enough to check by eye, and finishes with you deriving the CDC and SCD2 queries that Lab 1.4 needs.

New to Polars? Work through `tools/polars.ipynb` before Day 1 too (self-paced, about 90 minutes). It introduces one idea at a time on Qafila's data: DataFrames and types, expressions, `select`, `filter`, `with_columns`, `group_by`, joins, `over` and lazy queries, and finishes with you writing the kind of lazy chain Lab 2.1 starts from.

New to dbt? Before Lab 2.3, work through [`tools/dbt.ipynb`](tools/dbt.ipynb) (guided, about 100 minutes). Start with an empty project folder and a few teaching rows, write the configuration and models yourself, and read the SQL dbt generates from them. Step-by-step players replay each dbt run on the project's graph and an incremental `delete+insert` row by row, then compare it with `merge`; you break an incremental model on purpose and watch a failing test skip the report. Five short exercises along the way, and a tested city sales report at the end. This is an extended walkthrough outside the regular 25-minute session lab.

Session 1.1 is taught from [`tools/duckdb.ipynb`](tools/duckdb.ipynb), with no slides: one business question asked of the app database (SQLite) and of pandas, then of DuckDB, a fraction of the time and a file several times smaller. Then the engine itself: in-process, columnar, vectorized; connections and database files; CSV sniffing, globs with `union_by_name`, nested JSON, Parquet; tables vs. views; attaching SQLite; the one-writer lock, broken on purpose; DataFrames and relations in Python; friendly SQL; reading `EXPLAIN ANALYZE`. Three short exercises. Lab 1.1 then points it at the real sources.

Session 1.3 is taught from [`tools/dlt.ipynb`](tools/dlt.ipynb), with no slides: a loader written by hand breaks in four ways (retries, refunds, a new field, the cursor), then dlt does the same job while you read the files and tables it produces. Write dispositions, an incremental cursor whose state survives deleting the pipeline folder, a late record that slips past the cursor, nested data, variant columns and schema contracts; three short exercises. Lab 1.3 then applies it to the real payments API and app database.

Session 2.4 starts from [`tools/pandera.ipynb`](tools/pandera.ipynb): a day of payments with six problems, checked first with hand-written asserts (one failure per run, nulls slip through), then with a Pandera schema: types and nulls, built-in and custom checks, every failure at once with its row, a quarantine split, and the LazyFrame trap broken on purpose. It ends by mapping each rule to its dbt test and running the schema on the real payments feed. Three short exercises; Lab 2.4 then validates the supplier price files.

Session 4.1 is taught from [`tools/dagster.ipynb`](tools/dagster.ipynb), with no slides: a nightly script breaks the usual ways (a re-run loads a day twice, a refund flows into the report, nobody knows what was built when), then the same pipeline as Dagster assets. Selecting part of the graph, the run history and stale assets, resources for dev and prod, a partitioned asset that doubles its rows on a re-run until it is made idempotent, a non-blocking and a blocking asset check, schedules, a sensor with a cursor, retries, and a small dbt project whose models and tests join the graph. A player replays each run from its event log. Three short exercises; Lab 4.1 then orchestrates the real warehouse.

**Lab 3.2 (Spark)** needs Java 17+. On Colab the lab installs it. Locally, install a JDK (for example [Temurin 21](https://adoptium.net/)), set `JAVA_HOME`, then `uv sync --extra spark`. On native Windows, Spark also needs `winutils.exe` to write files, so Windows users should run Lab 3.2 on Colab (or in WSL).

Set `DE_QUICK=1` to shrink the heavier workloads. Useful on slow laptops and for checking that notebooks run.

## Notebooks

| Day | Session | Notebook | Runs on |
|---|---|---|---|
| 0 | Setup | [`00_setup_check.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/00_setup_check.ipynb) | CPU |
| 0 | Prerequisite: SQL window functions (self-paced) | [`00_sql_windows.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/00_sql_windows.ipynb) | CPU |
| 0 | Prerequisite: Polars, one idea at a time (self-paced) | [`tools/polars.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/tools/polars.ipynb) | CPU |
| 1 | Walkthrough: DuckDB, an analytical database in a library (taught before Lab 1.1) | [`tools/duckdb.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/tools/duckdb.ipynb) | CPU |
| 1 | Platform tour | [`D1_1_platform_tour.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/D1_1_platform_tour.ipynb) | CPU |
| 1 | Files & columnar storage | [`D1_2_files_parquet.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/D1_2_files_parquet.ipynb) | CPU |
| 1 | Walkthrough: dlt, from a hand-written loader to an incremental pipeline (taught before Lab 1.3) | [`tools/dlt.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/tools/dlt.ipynb) | CPU |
| 1 | Batch ingestion (dlt) | [`D1_3_ingestion_dlt.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/D1_3_ingestion_dlt.ipynb) | CPU |
| 1 | CDC & SCD2 | [`D1_4_cdc_merge_scd.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/D1_4_cdc_merge_scd.ipynb) | CPU |
| 2 | Bronze → silver (DuckDB, Polars) | [`D2_1_bronze_to_silver.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/D2_1_bronze_to_silver.ipynb) | CPU |
| 2 | Dimensional modeling | [`D2_2_dimensional_modeling.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/D2_2_dimensional_modeling.ipynb) | CPU |
| 2 | Guided preparation: dbt from an empty folder (about 100 min) | [`tools/dbt.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/tools/dbt.ipynb) | CPU |
| 2 | dbt project | [`D2_3_dbt_project.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/D2_3_dbt_project.ipynb) | CPU |
| 2 | Walkthrough: Pandera, checks at the door (taught before Lab 2.4) | [`tools/pandera.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/tools/pandera.ipynb) | CPU |
| 2 | Testing & data quality | [`D2_4_data_quality.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/D2_4_data_quality.ipynb) | CPU |
| 3 | Iceberg lakehouse | [`D3_1_iceberg_lakehouse.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/D3_1_iceberg_lakehouse.ipynb) | CPU |
| 3 | Spark | [`D3_2_spark.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/D3_2_spark.ipynb) | **Colab** (or Java 17+) |
| 3 | Streaming fundamentals | [`D3_3_streaming_fundamentals.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/D3_3_streaming_fundamentals.ipynb) | CPU |
| 3 | Streams into the lakehouse | [`D3_4_stream_to_lakehouse.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/D3_4_stream_to_lakehouse.ipynb) | CPU |
| 4 | Walkthrough: Dagster, assets on a schedule (taught before Lab 4.1) | [`tools/dagster.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/tools/dagster.ipynb) | CPU |
| 4 | Orchestration (Dagster) | [`D4_1_orchestration_dagster.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/D4_1_orchestration_dagster.ipynb) | CPU |
| 4 | Contracts & observability | [`D4_2_contracts_observability.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/D4_2_contracts_observability.ipynb) | CPU |
| 4 | Governance & privacy | [`D4_3_governance_privacy.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/D4_3_governance_privacy.ipynb) | CPU |
| 4 | Data for AI | [`D4_4_data_for_ai.ipynb`](https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/D4_4_data_for_ai.ipynb) | CPU (optional LLM endpoint) |

Labs 0 (SQL windows), 1.2, 1.4, 2.0 (dbt), 3.1 and 3.3 end with a **🖼️ Visual summary**: figures drawn from the lab's own results (row-group statistics, SCD2 timelines, the dbt project's graph, the Iceberg metadata tree, watermarks). The drawing code is in `de_viz.py`.

Exercises are marked **✏️ Exercise** and raise `NotImplementedError` until you fill them in. Complete reference versions are in `solutions/`. Cells marked **✱ Stretch** are optional. Self-checks print ✅ or ❌.

Labs don't depend on each other: from Day 2 on, each lab starts from `U.build_warehouse()`, the end state of the Day 1 ingestion labs (`warehouse/qafila.duckdb`, schema `raw`). Each lab writes its own outputs to `artifacts/<lab>/`.

The DuckDB walkthrough is self-contained (`artifacts/tools_duckdb/`): it builds its own 1M-row app database and toy files.
The Pandera walkthrough is self-contained (`artifacts/tools_pandera/`), apart from reading the payments feed in its last section.
The Dagster walkthrough is self-contained (`artifacts/tools_dagster/`, including its run history in `dagster_home/`), apart from reading the Qafila dbt project's graph at the end.
The dlt walkthrough is self-contained too (`artifacts/tools_dlt/`), apart from one look at the local payments API.
The guided dbt walkthrough is self-contained: it creates its own tiny teaching database and project in `artifacts/tools_dbt/`, without using the shared warehouse. Rerunning its workspace setup cell resets only that walkthrough's generated files. Its student notebook has five exercises; the reference notebook in `solutions/tools/` includes their answers.

## Data

`prepare_data.py` generates everything in `data/raw/`. It is synthetic and deterministic (fixed seed): August–September 2026, no real personal data, and it works offline.

| Source | File | Rows | Deliberate problems |
|---|---|---|---|
| App database (SQLite) | `qafila_oltp.sqlite` | 7,442 customers · 12 stores · 356 products · 45,766 orders · 172,545 order items | duplicate accounts, mixed mobile formats, soft deletes, orphan orders after erasure, negative quantities |
| Payments API (mock REST service in `de_utils.PaymentsAPI`) | `api/payments.jsonl` | 41,785 | pagination, rate limits, failed attempts, a field added on 15 September |
| Customer CDC feed (Debezium-style) | `cdc/customers_changes.jsonl` + `customers_snapshot_2026-09-01.csv` | 6,346 events | duplicates, out-of-order events |
| Courier delivery events | `events/delivery_events.jsonl` | 22,518 | late and duplicate events, three timestamp formats |
| Supplier price files | `suppliers/prices_2026-W36…W39.csv` | 640 | decimal commas, Arabic-Indic digits, a renamed column, bad costs |
| Product reviews (Arabic + English) | `reviews.csv` | 3,000 | free text for Day 4 |

`DATA_ISSUES.txt` lists every planted problem. Students find most of them in Lab 1.1, so open it afterwards.

`dbt_qafila/` is the dbt project for the course (staging → intermediate → marts, a quarantine model, tests and a model contract). Labs 2.3 to 4.4 and the capstone build it.

## Using an LLM (optional)

Only the text-to-SQL part of Lab 4.4 can use an LLM, and it falls back to a rule-based planner without one. To use any OpenAI-compatible endpoint (vLLM, Ollama, or a hosted provider), copy `.env.example` to `.env` and fill it in. The lab sends only the question and the metric and dimension descriptions, never data rows. Still, check your organisation's data policy before you use an external service.

## Capstone

`capstone/` holds the starter kit: `pipeline.py` (a working Dagster code location for option 1), an ODCS contract, the requirements, the rubric and a README template. See [capstone/README.md](capstone/README.md).

```bash
uv run python capstone/pipeline.py           # run the whole platform once (≈1–2 min)
uv run dagster dev -f capstone/pipeline.py   # the Dagster UI
```

## Editing the notebooks

Notebooks are generated from `_src/*.py` (percent format). Edit the source, then run:
```bash
uv run python _tools/py2nb.py                    # rebuild all (student + solutions)
uv run python _tools/py2nb.py D3_1_iceberg_lakehouse   # rebuild one
uv run python _tools/py2nb.py dbt                # one tool walkthrough: _src/tools/dbt.py → tools/ + solutions/tools/
```
A markdown cell headed `# %% [markdown] 🎓` is an instructor note: it goes into the solution notebook only.
