# %% [markdown]
# # Lab 2.0 · dbt, from an empty folder to a tested report
#
# Day 2, before Lab 2.3 · about 100 min · CPU only. You build a small dbt project file by file and read every piece of
# SQL dbt generates from it, so nothing in Lab 2.3's big project is magic.
#
# **The question:** how much delivered product revenue came from each store city? The rows are invented and small
# enough to check by hand (SAR, without delivery fees).
#
# **Rhythm:** 🤔 predict → run → 🔍 read. The ▶ players replay what dbt just did, one step at a time.
#
# **Exercises (✏️):** uncomment the template (`Ctrl+/`), replace every `___`, delete the two `YOUR CODE HERE` lines,
# then run the cell and the check after it (✅ = right). Answers: `solutions/D2_0_dbt_from_scratch.ipynb`.
#
# **Tasks**
# - [ ] 1 · Raw data: what dbt does not load (5')
# - [ ] 2 · The problem: three SQL files, run by hand (10')
# - [ ] 3 · A project, a profile and its sources (10')
# - [ ] 4 · A `SELECT` becomes a view (10')
# - [ ] 5 · `ref()` builds a graph, and the graph sets the order (15')
# - [ ] 6 · Materializations: view, table, incremental; break one on purpose (25')
# - [ ] 7 · Macros: write an expression once (5')
# - [ ] 8 · Tests: a build that succeeds is not a correct build (15')
# - [ ] 9 · Lineage, documentation, and one more model of your own (10')

# %% [markdown]
# ## 1 · Raw data: what dbt does not load (5')
#
# Run from `labs/` (`uv run jupyter lab`) or in Colab.

# %%
import os, sys, subprocess
from pathlib import Path

IN_COLAB = "google.colab" in sys.modules
if IN_COLAB:
    REPO_URL = "https://github.com/SDAIAAcademy/data-engineering.git"
    if not Path("/content/data-engineering").exists():
        subprocess.run(["git", "clone", "-q", REPO_URL, "/content/data-engineering"], check=True)
    os.chdir("/content/data-engineering/labs")
    subprocess.run([sys.executable, "-m", "pip", "install", "-q",
                    "duckdb>=1.4", "dbt-core>=1.10,<2", "dbt-duckdb>=1.11"], check=True)
else:
    while not Path("de_utils.py").exists() and Path.cwd() != Path.cwd().parent:
        os.chdir("..")  # solutions/ notebooks run from labs/
sys.path.insert(0, os.getcwd())

import json
import textwrap
import duckdb
from IPython.display import display
import de_utils as U
import de_viz as V

# Rerunning THIS cell starts the lab again from an empty folder (it only deletes this lab's own outputs).
OUT = U.lab_dir("D2_0_dbt_from_scratch", reset=True)
PROJ = OUT / "qafila_intro"
PROJ.mkdir()
DB = OUT / "qafila_intro.duckdb"
print("Empty project folder:", PROJ, "| files:", list(PROJ.iterdir()))


def write_file(relative_path, content):
    path = PROJ / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content).strip() + "\n", encoding="utf-8")
    print("Wrote", relative_path)


def sql(query):
    # Each query opens and closes its own connection, so the database is free when dbt opens it.
    with duckdb.connect(str(DB)) as connection:
        return connection.sql(query).df()


def execute_sql(statement):
    with duckdb.connect(str(DB)) as connection:
        connection.execute(statement)

# %% [markdown]
# **Vocabulary:** database = the DuckDB file · schema = a namespace inside it (`raw`) · table = stored rows · view = a
# stored query, run on every read.
#
# This cell plays the **ingestion step** (dlt in Lab 1.3) and loads three raw tables. dbt never loads data.

# %%
RAW_SQL = """
CREATE SCHEMA raw;
CREATE TABLE raw.stores (store_id VARCHAR, city VARCHAR);
INSERT INTO raw.stores VALUES ('RUH-04', 'Riyadh'), ('JED-03', 'Jeddah');

CREATE TABLE raw.orders (order_id BIGINT, store_id VARCHAR, status VARCHAR, updated_at TIMESTAMP);
INSERT INTO raw.orders VALUES
    (101, 'RUH-04', 'delivered', '2026-09-20 09:00:00'),
    (102, 'JED-03', 'delivered', '2026-09-20 10:00:00'),
    (103, 'JED-03', 'cancelled', '2026-09-20 11:00:00');

CREATE TABLE raw.order_items (
    order_id BIGINT, line_no INTEGER, quantity INTEGER, unit_price DECIMAL(12,2), discount DECIMAL(12,2)
);
INSERT INTO raw.order_items VALUES
    (101, 1, 1, 33.46, 0.00),
    (101, 2, 2, 121.20, 0.00),
    (102, 1, 1, 33.65, 0.00),
    (103, 1, 1, 12.00, 0.00);
"""
execute_sql(RAW_SQL)
display(sql("SELECT * FROM raw.orders ORDER BY order_id"))
display(sql("SELECT * FROM raw.order_items ORDER BY order_id, line_no"))

# %% [markdown]
# 🤔 **Predict** · What should the report say for Riyadh and for Jeddah? Our revenue: delivered lines with a
# positive quantity, worth `quantity * unit_price - discount`.
#
# 🔍 Riyadh (order 101): 33.46 + 2 × 121.20 = **275.86**. Jeddah: **33.65** (order 103 is cancelled).
#
# ## 2 · The problem: three SQL files, run by hand (10')
#
# Before dbt, let's build the report the way many teams start: a folder of SQL files, each one creating a table from
# the one before.
#
# `clean_orders.sql` → `order_sales.sql` → `city_sales.sql`
#
# We run them on a scratch copy of the raw data (an in-memory database), so the real one stays untouched.

# %%
FILES = {
    "clean_orders.sql": """
        CREATE OR REPLACE TABLE orders_clean AS
        SELECT order_id, store_id, status FROM raw.orders
    """,
    "order_sales.sql": """
        CREATE OR REPLACE TABLE order_sales AS
        SELECT o.order_id, o.store_id, o.status, i.quantity * i.unit_price - i.discount AS net_amount
        FROM raw.order_items i JOIN orders_clean o ON i.order_id = o.order_id
    """,
    "city_sales.sql": """
        CREATE OR REPLACE TABLE city_sales AS
        SELECT s.city, SUM(f.net_amount) AS revenue_sar
        FROM order_sales f JOIN raw.stores s ON f.store_id = s.store_id
        WHERE f.status = 'delivered'
        GROUP BY s.city
    """,
}


def run_by_hand(con, *names):
    for name in names:
        con.execute(FILES[name])
        print("ran", name)

# %% [markdown]
# 🤔 **Predict** · A scheduler runs every `.sql` file in the folder, in alphabetical order, on a new database. What happens?

# %%
by_hand = duckdb.connect()   # in memory
by_hand.execute(RAW_SQL)
try:
    run_by_hand(by_hand, *sorted(FILES))
except duckdb.Error as e:
    print("❌", str(e).splitlines()[0])

# %% [markdown]
# 🔍 `city_sales.sql` comes first alphabetically, and `order_sales` doesn't exist yet. The files depend on each other,
# but nothing in the folder says so: the order lives in someone's head. Run them in the right order:

# %%
run_by_hand(by_hand, "clean_orders.sql", "order_sales.sql", "city_sales.sql")
by_hand.sql("FROM city_sales ORDER BY city").df()

# %% [markdown]
# Correct. Now the source changes: order 103 is **delivered** after all. The manager asks for a fresh report, so you
# rerun the report file.
#
# 🤔 **Predict** · Does Jeddah now include order 103's 12 SAR?

# %%
by_hand.execute("UPDATE raw.orders SET status = 'delivered' WHERE order_id = 103")
run_by_hand(by_hand, "city_sales.sql")
stale = by_hand.sql("SELECT revenue_sar FROM city_sales WHERE city = 'Jeddah'").fetchone()[0]
U.check(float(stale) == 33.65, "no error, and Jeddah is still 33.65: the report was rebuilt from old tables")

# %% [markdown]
# 🔍 The report reran, but it read `order_sales`, which was built from `orders_clean`, which was built **before** the
# update. Follow order 103 through the tables:

# %%
V.table_steps([
    {"title": "raw.orders (now)", "df": by_hand.sql("SELECT order_id, status FROM raw.orders ORDER BY 1").df(),
     "new": ["status"]},
    {"title": "orders_clean (built earlier)", "df": by_hand.sql("SELECT order_id, status FROM orders_clean ORDER BY 1").df(),
     "new": ["status"]},
    {"title": "order_sales (built earlier)",
     "df": by_hand.sql("SELECT order_id, status, net_amount FROM order_sales ORDER BY 1").df(), "new": ["status"]},
    {"title": "city_sales (rebuilt just now)", "df": by_hand.sql("FROM city_sales ORDER BY city").df()},
])

# %% [markdown]
# To get it right you had to know that the report depends on `order_sales`, which depends on `orders_clean`, and rerun
# all three in that order. With 3 files that's annoying; with 300 it's how wrong numbers reach a dashboard.
#
# Look also at what every file repeats: `CREATE OR REPLACE TABLE ... AS`, around the only part that matters, the
# `SELECT`. Here is the same `order_sales` as a **dbt model**:

# %%
by_hand.close()
V.sql_steps([
    ("By hand · order_sales.sql", FILES["order_sales.sql"]),
    ("With dbt · models/order_sales.sql", """
        SELECT o.order_id, o.store_id, o.status, i.quantity * i.unit_price - i.discount AS net_amount
        FROM {{ source('raw', 'order_items') }} i JOIN {{ ref('orders_clean') }} o ON i.order_id = o.order_id
    """, "no CREATE: only the SELECT. ref() says what it reads"),
])

# %% [markdown]
# Each pain above has a dbt answer, and each one is a task of this lab:
#
# | By hand | With dbt | Task |
# |---|---|---|
# | you must know the run order | `ref()` and `source()` become a graph; dbt runs it in order | 5 |
# | after a change, you must know what to rerun | `--select orders_clean+` = it and everything downstream | 5, 6 |
# | `CREATE OR REPLACE` in every file | one setting: view, table or incremental | 4, 6 |
# | the same expression copied in many files | a macro, written once | 7 |
# | nobody checks the result | tests that run with the build | 8 |
#
# dbt doesn't run the SQL itself: it generates it and sends it to a database, here DuckDB. And it doesn't load data:
# the raw tables come from ingestion.
#
# 🤔 **Predict** · In dbt, you edit a model file and save it. Has the database changed?
#
# 🔍 No. Files are instructions; only a dbt command (`dbt run`, `dbt build`) changes the database.
#
# ## 3 · A project, a profile and its sources (10')
#
# Two files make a folder a dbt project:
# - `dbt_project.yml`: what the project is, and how to build its models;
# - `profiles.yml`: which database runs the SQL. Its name must match the project's `profile:`.

# %%
write_file("dbt_project.yml", """
name: qafila_intro
version: '1.0.0'
config-version: 2
profile: qafila_intro
model-paths: [models]
test-paths: [tests]
models:
  qafila_intro:
    staging:
      +schema: staging
      +materialized: view
    marts:
      +schema: marts
      +materialized: table
""")
write_file("profiles.yml", f"""
qafila_intro:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: '{DB.as_posix()}'
      schema: analytics
      threads: 1
""")
(PROJ / "models").mkdir(exist_ok=True)
(PROJ / "tests").mkdir(exist_ok=True)

# %% [markdown]
# **`dbt_project.yml`**, key by key:
#
# | Key | Means |
# |---|---|
# | `name` | the project's name (it reappears under `models:`) |
# | `version` | your project's version: a label, nothing checks it |
# | `config-version: 2` | the format of this file; always 2 |
# | `profile` | which entry of `profiles.yml` to connect with |
# | `model-paths`, `test-paths` | the folders holding models and tests (these are the defaults, written out) |
# | `models:` → `qafila_intro:` → `staging:` | settings for every model in `models/staging/` |
# | `+materialized` | how those models are stored: `view`, `table`, ... (Task 6) |
# | `+schema` | the schema they go to, **appended** to the profile's: `analytics` + `staging` → `analytics_staging` |
#
# The `+` marks a setting, so dbt doesn't read `materialized` as a folder name. A model can override its folder's
# settings with `{{ config(...) }}` at the top of its file (Task 6).
#
# **`profiles.yml`**:
#
# | Key | Means |
# |---|---|
# | `qafila_intro` | the profile's name, matching `profile:` above |
# | `target: dev` | which output to use unless you pass `--target prod` |
# | `outputs:` → `dev:` | one block per environment (dev, prod, ...) |
# | `type: duckdb` | the adapter: which database dbt generates SQL for (package `dbt-duckdb`) |
# | `path` | the DuckDB file (a server database would have `host`, `user`, `password` instead) |
# | `schema: analytics` | where dbt writes its models (see below) |
# | `threads: 1` | how many models dbt may build at the same time |
#
# **Why `schema: analytics`?** The database will hold two kinds of tables: the ones ingestion loads (`raw`) and the
# ones dbt builds. Keeping them in different schemas gives each a single owner. Ingestion writes to `raw`, and dbt only
# reads it. dbt writes only under `analytics`, so a `dbt run` can never overwrite source data, and anything under
# `analytics` can always be rebuilt from `raw`. The name itself is our choice: `analytics`, `dbt` or `transform` are
# all common.
#
# The folder settings then split it further, so the schema tells readers what they're looking at:
#
# | Schema | Holds | Who reads it |
# |---|---|---|
# | `raw` | source tables, as loaded | ingestion writes; dbt reads |
# | `analytics_staging` | cleaned copies, one per source | other models |
# | `analytics_marts` | facts and reports | analysts, dashboards |
#
# It's also what makes environments safe. Each developer's `dev` output gets their own schema (`schema: dev_sara`
# gives `dev_sara_staging`, `dev_sara_marts`), and only the `prod` output writes to `analytics`. Everyone runs the same
# project without touching each other's tables or production.
#
# **Not only DuckDB.** `type` picks an **adapter**, a separate package that teaches dbt one database's SQL. The same
# project can run on, for example:
#
# | Database | Package | Connection keys |
# |---|---|---|
# | DuckDB, MotherDuck | `dbt-duckdb` | `path` (a file, or `md:` for the cloud) |
# | PostgreSQL | `dbt-postgres` | `host`, `port`, `user`, `password`, `dbname` |
# | Snowflake | `dbt-snowflake` | `account`, `user`, `role`, `warehouse`, `database` |
# | BigQuery | `dbt-bigquery` | `project`, `dataset`, `method` (how to log in) |
# | Databricks, Spark | `dbt-databricks`, `dbt-spark` | `host`, `http_path`, `token` |
#
# Redshift, Trino, ClickHouse, SQL Server and others have adapters too. Typically one profile has an output per
# environment, here a local DuckDB for development and a shared Postgres for production:
#
# ```yaml
# qafila:
#   target: dev                       # the default
#   outputs:
#     dev:
#       type: duckdb
#       path: qafila_dev.duckdb
#       schema: analytics
#     prod:
#       type: postgres
#       host: warehouse.qafila.internal
#       port: 5432
#       user: dbt_prod
#       password: "{{ env_var('DBT_PROD_PASSWORD') }}"   # read from the environment, never written in the file
#       dbname: analytics
#       schema: analytics
#       threads: 8                    # a server can build 8 models at once
# ```
#
# What that gives you:
# - **one project, several databases:** `dbt build` builds in dev; `dbt build --target prod` builds the same models in
#   production. dbt writes the `CREATE` statements in each database's dialect. Your `SELECT`s must still use SQL that
#   the database understands;
# - **no secrets in the repository:** `env_var()` reads passwords and tokens from environment variables;
# - **models that know where they run:** `{{ target.name }}` is `dev` or `prod`, e.g. to read only the last 7 days of
#   data while developing.
#
# `dbt("debug")` below is `dbt debug --profiles-dir .` in a terminal. It checks the connection and builds nothing.

# %%
from dbt.cli.main import dbtRunner


def dbt(*args, expect_success=True, log=True):
    flags = ["--no-use-colors", "--no-send-anonymous-usage-stats"] + ([] if log else ["--log-level", "none"])
    result = dbtRunner().invoke([*flags, *args, "--project-dir", str(PROJ), "--profiles-dir", str(PROJ)])
    command = "dbt " + " ".join(args)
    if expect_success and not result.success:
        raise RuntimeError(command + " failed: see the log above") from result.exception
    if log:
        U.check(result.success == expect_success, command + (" succeeded" if result.success else " failed, as expected"))
    return result


def generated(kind, path):
    """A SQL file dbt wrote: kind is "compiled" (the rendered SELECT) or "run" (the SQL it executed)."""
    return (PROJ / "target" / kind / "qafila_intro" / path).read_text(encoding="utf-8")


dbt("debug")

# %% [markdown]
# 🔍 `All checks passed!` If not: a missing adapter, a profile name that doesn't match, a wrong path, or the database
# open in another program.
#
# **Sources** give the raw tables a name that models use: `source('qafila_raw', 'orders')`. In `_sources.yml`:
# `version: 2` is the format of dbt's YAML files; `name: qafila_raw` is the first argument of `source()`;
# `schema: raw` is where the tables really are; each `tables: - name` is a second argument.
#
# 🤔 **Predict** · After declaring them, how many tables does the database hold?

# %%
write_file("models/staging/_sources.yml", """
version: 2
sources:
  - name: qafila_raw
    schema: raw
    tables:
      - name: orders
      - name: order_items
      - name: stores
""")
print(dbt("ls", "--resource-type", "source", "--output", "name", log=False).result)
display(sql("SELECT table_schema, table_name, table_type FROM information_schema.tables ORDER BY 1, 2"))
U.check(len(sql("FROM information_schema.tables")) == 3, "still the same 3 raw tables: a source declaration loads nothing")

# %% [markdown]
# 🔍 Still three: a source only **describes** data that something else loaded.
#
# ## 4 · A `SELECT` becomes a view (10')
#
# The file name `stg_orders.sql` becomes the view's name; its folder makes it a view. (`stg_` = staging: one model per
# source table, rename and cast, no business logic.)
#
# 🤔 **Predict** · Does `dbt compile` create the view?

# %%
write_file("models/staging/stg_orders.sql", """
SELECT order_id, store_id, status, updated_at
FROM {{ source('qafila_raw', 'orders') }}
""")
dbt("compile", "--select", "stg_orders")
V.sql_steps([
    ("We write · models/staging/stg_orders.sql", (PROJ / "models/staging/stg_orders.sql").read_text()),
    ("dbt compile · target/compiled/…/stg_orders.sql", generated("compiled", "models/staging/stg_orders.sql"),
     "Jinja replaced by the table's full name: database.schema.table"),
])

# %%
U.check(sql("FROM information_schema.tables WHERE table_name = 'stg_orders'").empty,
        "compile wrote SQL to target/compiled/, but created nothing in the database")

# %% [markdown]
# 🔍 No. `{{ ... }}` is **Jinja**: dbt fills it in before any SQL exists, and `compile` stops there. Use it to read the
# SQL dbt will send.
#
# 🤔 **Predict** · We wrote no `CREATE VIEW` and no `CREATE SCHEMA`. What will `dbt run` do?

# %%
tables_before = sql("SELECT table_schema, table_name, table_type FROM information_schema.tables")
schemas_before = set(sql("SELECT schema_name FROM information_schema.schemata").schema_name)
dbt("run", "--select", "stg_orders")
tables_after = sql("SELECT table_schema, table_name, table_type FROM information_schema.tables")
schemas_after = set(sql("SELECT schema_name FROM information_schema.schemata").schema_name)
V.database_map(tables_after, before=tables_before)

# %%
V.sql_steps([
    ("We write · stg_orders.sql", (PROJ / "models/staging/stg_orders.sql").read_text()),
    ("dbt compiles", generated("compiled", "models/staging/stg_orders.sql")),
    ("dbt runs · target/run/…/stg_orders.sql", generated("run", "models/staging/stg_orders.sql"),
     "the same SELECT, wrapped in the SQL of a view"),
])

# %%
U.check("analytics_staging" in schemas_after - schemas_before, "dbt created the missing schema analytics_staging")
U.check(sql("SELECT table_type FROM information_schema.tables WHERE table_name = 'stg_orders'").table_type[0] == "VIEW",
        "stg_orders is a view: its folder's default")

# %% [markdown]
# 🔍 dbt picked the schema, created it, wrapped our `SELECT` in `create view`, and sent it to DuckDB. It builds under a
# temporary name (`__dbt_tmp`) and renames at the end, so nobody reads a half-built model.
#
# ## 5 · `ref()` builds a graph, and the graph sets the order (15')
#
# `stg_order_items` gives each line an id (`101-2`) and its `net_amount`: one row per line.

# %%
write_file("models/staging/stg_order_items.sql", """
SELECT
    CAST(order_id AS VARCHAR) || '-' || CAST(line_no AS VARCHAR) AS order_line_id,
    order_id, line_no, quantity, unit_price, discount,
    quantity * unit_price - discount AS net_amount
FROM {{ source('qafila_raw', 'order_items') }}
""")

# %% [markdown]
# ✏️ **Exercise 1** · Write `models/staging/stg_stores.sql`: `store_id` and `city`, read with `source()`.
# - **Check yourself:** 2 rows, and the compiled SQL contains `"raw"."stores"`.

# %%
# Template:
# write_file("models/staging/stg_stores.sql", """
# SELECT store_id, city
# FROM {{ source('___', '___') }}
# """)
# BEGIN SOLUTION
write_file("models/staging/stg_stores.sql", """
SELECT store_id, city
FROM {{ source('qafila_raw', 'stores') }}
""")
# END SOLUTION

# %%
dbt("run", "--select", "stg_stores")
U.check(len(sql("FROM analytics_staging.stg_stores")) == 2, "stg_stores has the 2 stores")
U.check('"raw"."stores"' in generated("compiled", "models/staging/stg_stores.sql"), "source() resolved to raw.stores")

# %% [markdown]
# The **fact**: one row per line, with its order's store and status. Models read other models with `ref()`. The fact
# keeps every status; the report decides what counts.

# %%
FACT_SELECT = """
SELECT
    i.order_line_id, i.order_id, i.line_no,
    o.store_id, o.status, o.updated_at,
    i.quantity, i.net_amount
FROM {{ ref('stg_order_items') }} i
JOIN {{ ref('stg_orders') }} o ON i.order_id = o.order_id
WHERE i.quantity > 0
"""
write_file("models/marts/fct_order_items.sql", FACT_SELECT)

# %% [markdown]
# ✏️ **Exercise 2** · Write `models/marts/mart_city_sales.sql`: join the fact to the stores (both with `ref()`), keep
# delivered lines, and sum `net_amount` per city as `revenue_sar`.
# - **Check yourself:** Riyadh 275.86, Jeddah 33.65.

# %%
# Template:
# write_file("models/marts/mart_city_sales.sql", """
# SELECT s.city, SUM(f.net_amount) AS revenue_sar
# FROM {{ ref('___') }} f
# JOIN {{ ref('___') }} s ON f.store_id = s.store_id
# WHERE f.status = '___'
# GROUP BY s.city
# """)
# BEGIN SOLUTION
write_file("models/marts/mart_city_sales.sql", """
SELECT s.city, SUM(f.net_amount) AS revenue_sar
FROM {{ ref('fct_order_items') }} f
JOIN {{ ref('stg_stores') }} s ON f.store_id = s.store_id
WHERE f.status = 'delivered'
GROUP BY s.city
""")
# END SOLUTION

# %% [markdown]
# 🤔 **Predict** · `+mart_city_sales` = the report **and everything it needs**. In what order will dbt build them?

# %%
dbt("run", "--select", "+mart_city_sales")
display(sql("SELECT * FROM analytics_marts.mart_city_sales ORDER BY city"))

# %%
city = lambda name: round(float(sql(f"SELECT revenue_sar FROM analytics_marts.mart_city_sales WHERE city = '{name}'").revenue_sar[0]), 2)
U.check(city("Riyadh") == 275.86, "Riyadh matches the hand calculation")
U.check(city("Jeddah") == 33.65, "cancelled order 103 contributes nothing to Jeddah")

# %% [markdown]
# ▶ **Watch it run.** `V.animate_dbt` replays the last command from `target/manifest.json` (the graph) and
# `target/run_results.json` (what ran). Press **Play**, or **Next**. (Buttons do nothing? Run the cell again.)

# %%
V.animate_dbt(PROJ)

# %% [markdown]
# 🔍 Each `ref()` is an arrow. dbt reads them all first, then runs each model after its parents. The staging views
# don't depend on each other, so any order works (with more threads, in parallel).
#
# 🧪 **Experiment** · A model that names the fact's table directly, without `ref()`. What are its parents?

# %%
write_file("models/marts/mart_hardcoded.sql", "SELECT COUNT(*) AS n_lines FROM analytics_marts.fct_order_items")
print("+mart_city_sales →", dbt("ls", "--select", "+mart_city_sales", "--output", "name", log=False).result)
print("+mart_hardcoded  →", dbt("ls", "--select", "+mart_hardcoded", "--output", "name", log=False).result)
(PROJ / "models/marts/mart_hardcoded.sql").unlink()

# %% [markdown]
# 🔍 None. dbt may build it before the fact, `fct_order_items+` won't refresh it, and the lineage loses it. Always
# `ref()` models and `source()` raw tables.
#
# | `--select` | Selects |
# |---|---|
# | `fct_order_items` | that model |
# | `+fct_order_items` | it and its parents |
# | `fct_order_items+` | it and its children |
# | `staging` | the `models/staging/` folder |
#
# The folders are **layers**: staging (clean each source) → intermediate (reshape for reuse, e.g. Lab 2.3's payments →
# one row per order) → marts (facts and reports at a declared grain).
#
# ## 6 · Materializations: view, table, incremental; break one on purpose (25')
#
# | `materialized=` | Stores | The work happens |
# |---|---|---|
# | `view` | a query | on every read |
# | `table` | rows, rebuilt each run | at `dbt run`, over all rows |
# | `incremental` | rows, updated each run | at `dbt run`, over the rows its filter selects |
# | `ephemeral` | nothing (pasted into its readers as a CTE) | inside the readers |
#
# ### 6a · A view sees a change at once; a table needs a rebuild
#
# 🤔 **Predict** · Order 103 is now delivered in the source. Which shows it first: the view or the table?

# %%
execute_sql("UPDATE raw.orders SET status = 'delivered', updated_at = '2026-09-22 09:00:00' WHERE order_id = 103")
display(sql("""
    SELECT 'stg_orders (view)' AS model, status FROM analytics_staging.stg_orders WHERE order_id = 103
    UNION ALL
    SELECT 'fct_order_items (table)', status FROM analytics_marts.fct_order_items WHERE order_id = 103
"""))

# %%
U.check(sql("SELECT status FROM analytics_staging.stg_orders WHERE order_id = 103").status[0] == "delivered",
        "the view re-reads the source every time: it already sees the update")
U.check(sql("SELECT status FROM analytics_marts.fct_order_items WHERE order_id = 103").status[0] == "cancelled",
        "the table still holds the rows of its last build")

# %% [markdown]
# 🔍 A view is never stale but pays on every read; a table is fast but frozen at its last build. `fct_order_items+`
# rebuilds the fact and its children:

# %%
dbt("run", "--select", "fct_order_items+")
U.check(city("Jeddah") == 45.65, "after the rebuild, Jeddah includes order 103's 12 SAR")
V.animate_dbt(PROJ)

# %% [markdown]
# ### 6b · An incremental model processes only the rows that changed
#
# It loads everything on the first run, then only the rows its filter selects. Three choices are ours:
# - **filter:** orders updated at most 1 day before the newest one loaded (the **overlap** catches late changes);
# - **`unique_key`:** how to recognize a row already in the table;
# - **`delete+insert`:** delete the stored rows whose key comes back, then insert the new versions.
#
# In the model below:
#
# | Part | Means |
# |---|---|
# | `{{ config(...) }}` | settings for this model only; they override the folder's `+materialized: table` |
# | `materialized='incremental'` | keep the table between runs and add to it |
# | `unique_key='order_line_id'` | the column that identifies a row |
# | `incremental_strategy='delete+insert'` | how the new rows are written into the table (6c lists the others) |
# | `{% if is_incremental() %} ... {% endif %}` | the filter, left out on the first run and with `--full-refresh` |
# | `{{ this }}` | the model's own table, so the filter can ask what's already loaded |

# %%
INCREMENTAL_SQL = """
{{ config(materialized='incremental', unique_key='order_line_id', incremental_strategy='delete+insert') }}
""" + FACT_SELECT + """
{% if is_incremental() %}
  AND o.updated_at >= (
    SELECT COALESCE(MAX(updated_at), TIMESTAMP '1970-01-01') - INTERVAL 1 DAY
    FROM {{ this }}
  )
{% endif %}
"""
write_file("models/marts/fct_order_items_incremental.sql", INCREMENTAL_SQL)
dbt("run", "--select", "+fct_order_items_incremental")
U.check(len(sql("FROM analytics_marts.fct_order_items_incremental")) == 4, "the first run loads all four lines")

# %% [markdown]
# Now order 101 is refunded.
#
# 🤔 **Predict** · The newest `updated_at` loaded is 22 Sept 09:00. Which lines will the next run reprocess?
#
# We compile first and run that `SELECT` ourselves: it returns exactly what dbt will hand to `delete+insert`.

# %%
execute_sql("UPDATE raw.orders SET status = 'refunded', updated_at = '2026-09-25 12:00:00' WHERE order_id = 101")
before = sql("SELECT * FROM analytics_marts.fct_order_items_incremental ORDER BY order_line_id")
cutoff = sql("SELECT MAX(updated_at) - INTERVAL 1 DAY AS c FROM analytics_marts.fct_order_items_incremental").c[0]
dbt("compile", "--select", "fct_order_items_incremental")
candidates = sql(generated("compiled", "models/marts/fct_order_items_incremental.sql"))
dbt("run", "--select", "fct_order_items_incremental")
after = sql("SELECT * FROM analytics_marts.fct_order_items_incremental ORDER BY order_line_id")
display(after)

# %% [markdown]
# ▶ **Watch it run.** Left: the model's `SELECT`. Right: the incremental table.

# %%
V.animate_incremental(
    all_rows=sql(generated("compiled", "models/marts/fct_order_items.sql")), candidates=candidates, before=before,
    key="order_line_id", cutoff=cutoff, show=["order_line_id", "status", "updated_at", "net_amount"])

# %%
U.check(sorted(candidates.order_line_id) == ["101-1", "101-2", "103-1"], "3 lines in the window: 101's two, and 103's")
U.check(len(after) == 4 and after.order_line_id.is_unique, "still 4 lines, one per order_line_id: no duplicates")
U.check(set(after[after.order_id == 101].status) == {"refunded"}, "both of order 101's lines are refunded")
dbt("run", "--select", "fct_order_items_incremental")
U.check(sql("SELECT * FROM analytics_marts.fct_order_items_incremental ORDER BY order_line_id").equals(after),
        "running again over the same data changes nothing: reprocessing the overlap is safe")

# %% [markdown]
# 🔍 103-1 was reprocessed unchanged: that's the overlap, and it's harmless (replaced, not duplicated). 102-1 was
# outside the window and never read. All the SQL of that run:

# %%
DELETE_INSERT_RUN = generated("run", "models/marts/fct_order_items_incremental.sql")
V.sql_steps([
    ("We write · fct_order_items_incremental.sql", INCREMENTAL_SQL),
    ("dbt compiles (is_incremental() is true)", generated("compiled", "models/marts/fct_order_items_incremental.sql"),
     "the filter is in, and {{ this }} is the model's own table"),
    ("dbt runs · target/run/…", DELETE_INSERT_RUN,
     "the SELECT went into a temporary table first; then delete + insert"),
])

# %% [markdown]
# `unique_key` matches rows; it doesn't **check** that they're unique. Only a test does (Task 8).
#
# ### 6c · Other strategies, and one side by side
#
# `incremental_strategy` decides how the batch the filter selected is written into the table:
#
# | Strategy | What one run does | Typical use |
# |---|---|---|
# | `append` | inserts the batch; no key, nothing is replaced | events that never change (clicks, logs) |
# | `delete+insert` | deletes the stored rows whose key comes back, then inserts the batch | this lab; DuckDB's default |
# | `merge` | one `MERGE INTO`: updates the rows whose key matches, inserts the new keys | default on Snowflake, BigQuery, Databricks |
# | `insert_overwrite` | replaces every partition (e.g. a day) the batch touches | partitioned tables on BigQuery, Spark |
# | `microbatch` | splits the work into time slices (e.g. one per day), each rerunnable on its own | large event tables |
#
# Which ones you can use depends on the adapter. Let's try `merge` once on the same model, then switch back.
#
# 🤔 **Predict** · Same filter, same `unique_key`, same data; only the strategy changes. Does the table change?

# %%
MERGE_SQL = INCREMENTAL_SQL.replace("incremental_strategy='delete+insert'", "incremental_strategy='merge'")
write_file("models/marts/fct_order_items_incremental.sql", MERGE_SQL)
dbt("run", "--select", "fct_order_items_incremental")
U.check(sql("SELECT * FROM analytics_marts.fct_order_items_incremental ORDER BY order_line_id").equals(after),
        "same table: the strategy changes how the rows are written, not which rows end up there")
V.sql_steps([
    ("delete+insert · target/run/…", DELETE_INSERT_RUN, "two statements: DELETE the keys that came back, then INSERT"),
    ("merge · target/run/…", generated("run", "models/marts/fct_order_items_incremental.sql"),
     "one statement: UPDATE on a match, INSERT otherwise"),
])
write_file("models/marts/fct_order_items_incremental.sql", INCREMENTAL_SQL)   # back to delete+insert for the lab

# %% [markdown]
# 🔍 Same rows, different SQL. A practical plus of `merge`: it can update only some columns of a matched row
# (`merge_update_columns=['status', 'updated_at']` in the `config()`); `delete+insert` always replaces the whole row.
#
# Every strategy writes only what the filter selected. So what happens when a row changes and the filter doesn't
# select it?
#
# ### 6d · 🧪 Break it on purpose: a change the filter can't see
#
# 🤔 **Predict** · Line 102-1 gets a 2 SAR discount. The line changes; its order's `updated_at` doesn't. After
# rebuilding both facts, which one has the discount?

# %%
execute_sql("UPDATE raw.order_items SET discount = 2.00 WHERE order_id = 102 AND line_no = 1")
dbt("run", "--select", "fct_order_items", "fct_order_items_incremental")
line_102 = sql("""
    SELECT 'fct_order_items (table)' AS model, net_amount FROM analytics_marts.fct_order_items WHERE order_line_id = '102-1'
    UNION ALL
    SELECT 'fct_order_items_incremental', net_amount FROM analytics_marts.fct_order_items_incremental WHERE order_line_id = '102-1'
""")
line_102

# %%
U.check(line_102.net_amount.tolist() == [31.65, 33.65], "the table has the discount, the incremental model missed it")

# %% [markdown]
# 🔍 No error, and the incremental model stays wrong: its filter only watches `orders.updated_at`. The filter defines
# what you can miss:
#
# | Change | Found? |
# |---|---|
# | an order updated inside the overlap | yes |
# | a line changed, its order untouched (just now) | no |
# | a late record, older than the overlap | no |
# | a deleted source row | no |
# | a fix to the model's logic | only for new rows |
#
# Fixes: watch the right timestamp, widen the overlap, or rebuild with `--full-refresh` (the `SELECT` over all rows).

# %%
dbt("run", "--select", "fct_order_items_incremental", "--full-refresh")
U.check(sql("SELECT net_amount FROM analytics_marts.fct_order_items_incremental WHERE order_line_id = '102-1'")
        .net_amount[0] == 31.65, "the full refresh picked up the discount")
dbt("run", "--select", "fct_order_items+")
U.check(city("Jeddah") == 43.65, "and the report shows it: 31.65 + 12.00 for Jeddah, nothing for refunded Riyadh")

# %% [markdown]
# ## 7 · Macros: write an expression once (5')
#
# `quantity * unit_price - discount` is our revenue definition; copies of it drift apart. A **macro** is named Jinja
# that returns SQL text. dbt pastes it in before the SQL reaches the database.

# %%
write_file("macros/net_amount.sql", """
{% macro net_amount(quantity, unit_price, discount) -%}
    ({{ quantity }} * {{ unit_price }} - {{ discount }})
{%- endmacro %}
""")

# %% [markdown]
# ✏️ **Exercise 3** · In `stg_order_items.sql`, replace the expression with a call: `{{ net_amount('quantity', ...) }}`
# (column names as strings).
# - **Check yourself:** the compiled SQL contains `(quantity * unit_price - discount)`; the report doesn't change.

# %%
# Template:
# write_file("models/staging/stg_order_items.sql", """
# SELECT
#     CAST(order_id AS VARCHAR) || '-' || CAST(line_no AS VARCHAR) AS order_line_id,
#     order_id, line_no, quantity, unit_price, discount,
#     {{ ___('quantity', 'unit_price', '___') }} AS net_amount
# FROM {{ source('qafila_raw', 'order_items') }}
# """)
# BEGIN SOLUTION
write_file("models/staging/stg_order_items.sql", """
SELECT
    CAST(order_id AS VARCHAR) || '-' || CAST(line_no AS VARCHAR) AS order_line_id,
    order_id, line_no, quantity, unit_price, discount,
    {{ net_amount('quantity', 'unit_price', 'discount') }} AS net_amount
FROM {{ source('qafila_raw', 'order_items') }}
""")
# END SOLUTION

# %%
dbt("run", "--select", "stg_order_items+")
V.sql_steps([
    ("We write · stg_order_items.sql", (PROJ / "models/staging/stg_order_items.sql").read_text()),
    ("dbt compiles", generated("compiled", "models/staging/stg_order_items.sql"), "the macro call became its SQL"),
])
U.check("(quantity * unit_price - discount)" in generated("compiled", "models/staging/stg_order_items.sql"),
        "the macro expanded into the expression")
U.check(city("Jeddah") == 43.65, "same revenue as before: the definition moved, it didn't change")

# %% [markdown]
# ## 8 · Tests: a build that succeeds is not a correct build (15')
#
# A **test** is a `SELECT` of the rows that **break** a rule: 0 rows = pass. **Generic** tests (`unique`, `not_null`,
# ...) are declared in YAML; **singular** tests are SQL files in `tests/`.
#
# In `_models.yml`, each `- name:` is a model; `description` goes into the documentation (Task 9); under `columns:`,
# `data_tests: [unique, not_null]` attaches those two tests to that column. A test that takes settings is written as a
# mapping: `accepted_values` gets the list of allowed values under `arguments:`, so a new or misspelled `status` fails.

# %%
write_file("models/marts/_models.yml", """
version: 2
models:
  - name: fct_order_items
    description: One row per order line with a positive quantity; keeps the order's status.
    columns:
      - name: order_line_id
        data_tests: [unique, not_null]
      - name: status
        data_tests:
          - accepted_values:
              arguments:
                values: [placed, delivered, cancelled,
                         refunded, failed_delivery]
  - name: fct_order_items_incremental
    columns:
      - name: order_line_id
        data_tests: [unique, not_null]
  - name: mart_city_sales
    description: Delivered product revenue in SAR by store city, excluding delivery fees.
    columns:
      - name: city
        data_tests: [unique, not_null]
""")
write_file("tests/assert_fact_stores_exist.sql", """
-- every line's store must exist in stg_stores: return the lines whose store doesn't
SELECT f.order_line_id, f.store_id
FROM {{ ref('fct_order_items') }} f
LEFT JOIN {{ ref('stg_stores') }} s ON f.store_id = s.store_id
WHERE s.store_id IS NULL
""")
dbt("build")

# %% [markdown]
# `dbt build` = run + test along the graph: a model, its tests, then its children. What `[unique]` turns into:

# %%
UNIQUE_TEST = "models/marts/_models.yml/unique_fct_order_items_order_line_id.sql"
V.sql_steps([
    ("We write · _models.yml", "columns:\n  - name: order_line_id\n    data_tests: [unique, not_null]"),
    ("dbt generates · target/compiled/…/unique_fct_order_items_order_line_id.sql", generated("compiled", UNIQUE_TEST),
     "the keys that appear more than once"),
])
display(sql(generated("compiled", UNIQUE_TEST)))

# %% [markdown]
# 🤔 **Predict** · Line 102-1 is now delivered twice. Which test fails? Is the report rebuilt? Is the duplicate
# already in the fact?

# %%
execute_sql("INSERT INTO raw.order_items SELECT * FROM raw.order_items WHERE order_id = 102 AND line_no = 1")
dbt("build", "--select", "+mart_city_sales", expect_success=False)
statuses = {r["unique_id"].split(".")[2]: r["status"]
            for r in json.loads((PROJ / "target/run_results.json").read_text(encoding="utf-8"))["results"]}
statuses

# %% [markdown]
# ▶ **Watch it run.** Follow the red.

# %%
V.animate_dbt(PROJ)

# %%
display(sql(generated("compiled", UNIQUE_TEST)))
U.check(statuses["unique_fct_order_items_order_line_id"] == "fail", "the unique test found the duplicated line")
U.check(statuses["mart_city_sales"] == "skipped", "the report was skipped: dbt doesn't build on a failed test")
U.check(len(sql("FROM analytics_marts.fct_order_items")) == 5, "but the fact already holds the 5 rows, duplicate included")

# %% [markdown]
# 🔍 The report was **skipped** and keeps its last good rows. But the fact was built **before** its test, so the
# duplicate is already in the database. A test stops what comes after; it doesn't undo what came before.
#
# Fix the source, then rebuild:

# %%
execute_sql("CREATE OR REPLACE TABLE raw.order_items AS SELECT DISTINCT * FROM raw.order_items")
dbt("build")
U.check(len(sql("FROM analytics_marts.fct_order_items")) == 4, "the repaired fact has 4 lines again")

# %% [markdown]
# Tests check rules, not numbers. Last check: recompute the report straight from the raw tables.

# %%
expected = sql("""
    SELECT s.city, SUM(i.quantity * i.unit_price - i.discount) AS revenue_sar
    FROM raw.orders o
    JOIN raw.order_items i ON o.order_id = i.order_id
    JOIN raw.stores s ON o.store_id = s.store_id
    WHERE o.status = 'delivered' AND i.quantity > 0
    GROUP BY s.city ORDER BY s.city
""")
actual = sql("SELECT * FROM analytics_marts.mart_city_sales ORDER BY city")
display(actual)
U.check(actual.equals(expected), "the report reconciles with a direct calculation from the raw tables")

# %% [markdown]
# ✏️ **Exercise 4** · Write the singular test `tests/assert_city_revenue_nonnegative.sql`: the rows of
# `mart_city_sales` whose `revenue_sar` is negative or `NULL`.
# - **Check yourself:** `dbt test` on it passes.

# %%
# Template:
# write_file("tests/assert_city_revenue_nonnegative.sql", """
# SELECT * FROM {{ ref('___') }}
# WHERE ___
# """)
# BEGIN SOLUTION
write_file("tests/assert_city_revenue_nonnegative.sql", """
SELECT * FROM {{ ref('mart_city_sales') }}
WHERE revenue_sar < 0 OR revenue_sar IS NULL
""")
# END SOLUTION

# %%
result = dbt("test", "--select", "assert_city_revenue_nonnegative")
U.check([r.status for r in result.result] == ["pass"], "dbt ran your test, and no city breaks the rule")

# %% [markdown]
# ## 9 · Lineage, documentation, and one more model of your own (10')
#
# `target/manifest.json` (the graph) and `target/run_results.json` (the last command) are what the players read.
# Orchestrators read them too (Lab 4.1).

# %%
dbt("docs", "generate")
manifest = json.loads((PROJ / "target/manifest.json").read_text(encoding="utf-8"))
for name in ["fct_order_items", "mart_city_sales"]:
    print(name, "depends on:", manifest["nodes"][f"model.qafila_intro.{name}"]["depends_on"]["nodes"])
print("Documentation files:", [f for f in ["index.html", "manifest.json", "catalog.json"] if (PROJ / "target" / f).exists()])

# %% [markdown]
# To browse the docs: `dbt docs serve --profiles-dir .` in a terminal, in the project folder (`Ctrl+C` stops it).
#
# | Command | Does |
# |---|---|
# | `dbt debug` | checks the connection |
# | `dbt compile` | renders the Jinja, builds nothing |
# | `dbt run` | builds models |
# | `dbt test` | tests what's built |
# | `dbt build` | builds and tests, along the graph |
# | `dbt ls` | lists what a selection selects |
# | `dbt docs generate` | writes the documentation |
#
# ✏️ **Exercise 5** · Write `models/marts/mart_city_order_counts.sql`: delivered **orders** per city, as
# `delivered_orders`, from `stg_orders` and `stg_stores`.
# - **Check yourself:** one row: Jeddah, 2 (order 101 is refunded).

# %%
# Template:
# write_file("models/marts/mart_city_order_counts.sql", """
# SELECT s.city, ___ AS delivered_orders
# FROM {{ ref('___') }} o
# JOIN {{ ref('___') }} s ON o.store_id = s.store_id
# WHERE o.status = '___'
# GROUP BY s.city
# """)
# BEGIN SOLUTION
write_file("models/marts/mart_city_order_counts.sql", """
SELECT s.city, COUNT(*) AS delivered_orders
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_stores') }} s ON o.store_id = s.store_id
WHERE o.status = 'delivered'
GROUP BY s.city
""")
# END SOLUTION

# %%
dbt("build", "--select", "+mart_city_order_counts")
counts = sql("SELECT * FROM analytics_marts.mart_city_order_counts ORDER BY city")
display(counts)
U.check(counts.to_dict("records") == [{"city": "Jeddah", "delivered_orders": 2}], "Jeddah: 2 delivered orders, each counted once")
dbt("build")
print("Your complete project is in:", PROJ)

# %% [markdown]
# ## Checkpoint
#
# 1. Why did `_sources.yml` load nothing, and why did `dbt compile` create no view?
# 2. What does dbt lose when a model names a table instead of using `ref()`?
# 3. View or table: which sees a source change at once, and what does it cost?
# 4. A bug in `stg_orders` hit every historical order. Which command fixes and tests everything downstream?
# 5. A `unique` test failed in `dbt build`. What was skipped, and was the bad data already published?
#
# Next: [Lab 2.3 · The Qafila dbt project](D2_3_dbt_project.ipynb). Docs:
# [materializations](https://docs.getdbt.com/docs/build/materializations) ·
# [incremental](https://docs.getdbt.com/docs/build/incremental-models) ·
# [macros](https://docs.getdbt.com/docs/build/jinja-macros) ·
# [tests](https://docs.getdbt.com/docs/build/data-tests) ·
# [selection](https://docs.getdbt.com/reference/node-selection/syntax).
#
# ## 🖼️ Visual summary
#
# **1 · The project's graph** (from `target/manifest.json`).

# %%
V.dbt_graph(PROJ)

# %% [markdown]
# **2 · The database dbt built**, compared with the one before its first run.

# %%
V.database_map(sql("SELECT table_schema, table_name, table_type FROM information_schema.tables"), before=tables_before)
