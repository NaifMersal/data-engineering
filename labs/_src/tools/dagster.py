# %% [markdown]
# # Dagster, assets on a schedule
#
# Day 4, before Lab 4.1 · CPU only. Every lab so far ran its steps by hand, top to bottom. In production nobody presses
# run: a nightly script does, until it fails halfway, runs twice, or lets a bad file through. You'll write that script,
# watch it break, then declare the same pipeline as **Dagster assets**: the tables you want to exist, what each is
# built from, and the checks it must pass. Dagster works out the order, keeps a record of every run, re-runs one day,
# stops bad data, and starts runs on a clock or when a file lands.
#
# **The data:** a tiny Qafila pipeline on invented rows, few enough to check by eye: three stores, one orders file per
# day from the stores app. At the end the same ideas meet the real dbt project. Lab 4.1 then orchestrates the real
# warehouse.
#
# **Rhythm:** 🤔 predict → run → 🔍 read.
#
# **Exercises (✏️):** uncomment the template (`Ctrl+/`), replace every `___`, delete the two `YOUR CODE HERE` lines,
# then run the cell and the check after it (✅ = right). Answers: `solutions/tools/dagster.ipynb`.
#
# **Contents**
# - [ ] 1 · The problem: a nightly script
# - [ ] 2 · The mental model: assets, not tasks
# - [ ] 3 · Hello world
# - [ ] 4 · A graph of tables, and running part of it
# - [ ] 5 · Every run is recorded
# - [ ] 6 · Resources: the same code in dev and prod
# - [ ] 7 · Partitions: one day at a time, and backfills
# - [ ] 8 · Asset checks: stop bad data spreading
# - [ ] 9 · When runs start: schedules, sensors, retries
# - [ ] 10 · dbt in the graph
# - [ ] 11 · Back to the map

# %% [markdown] 🎓
# 🎓 **Instructor notes** (solution notebook only)
#
# - Sections 1–4 and 8 carry the session: the script breaking, the graph, selection, and a blocking check. 5–7 and 9
#   are short (one idea each); 10 bridges to Lab 4.1, whose task 2 puts the real dbt project in the graph.
# - Don't write Lab 4.1's code here: its exercises are the delete-then-insert partition, the duplicate-events check
#   and the supplier-file sensor with `run_key`. This notebook uses an upsert, a negative-amount check and a cursor.
# - Expect "Airflow?": the most deployed orchestrator. Airflow 3 added assets too, but its unit is still the task;
#   in Dagster the unit is the asset. The ideas here (dependencies, partitions, idempotency, checks) transfer.
# - Everything runs in-process with `dg.materialize`: no server. If there's time, show `dagster dev` on the capstone
#   pipeline (`labs/capstone/`) for the UI: the same graph, runs and checks.

# %%
import os, sys, subprocess
from pathlib import Path

IN_COLAB = "google.colab" in sys.modules
if IN_COLAB:
    REPO_URL = "https://github.com/SDAIAAcademy/data-engineering.git"
    if not Path("/content/data-engineering").exists():
        subprocess.run(["git", "clone", "-q", REPO_URL, "/content/data-engineering"], check=True)
    os.chdir("/content/data-engineering/labs")
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "dagster>=1.10", "dagster-dbt>=0.26",
                    "duckdb>=1.4", "dbt-duckdb>=1.11"], check=True)
else:
    while not Path("de_utils.py").exists() and Path.cwd() != Path.cwd().parent:
        os.chdir("..")  # tools/ and solutions/tools/ notebooks run from labs/
sys.path.insert(0, os.getcwd())

import warnings
from datetime import datetime
from zoneinfo import ZoneInfo

warnings.filterwarnings("ignore")             # Dagster flags some APIs as beta or preview
import dagster as dg
import duckdb
import pandas as pd
from IPython.display import display
import de_utils as U
import de_viz as V

# Rerunning THIS cell starts again from an empty folder (it only deletes this notebook's own outputs).
OUT = U.lab_dir("tools_dagster", reset=True)
DB = OUT / "qafila_toy.duckdb"
INBOX = OUT / "inbox"                         # where the stores app drops one orders file per day
INBOX.mkdir()
print("Output folder:", OUT)


def sql(query, params=None):
    """Run SQL on the toy warehouse. Each call opens and closes its own connection, so dbt can use the file too."""
    with duckdb.connect(str(DB)) as con:
        return con.execute(query, params or []).df()

# %% [markdown]
# You are here: Dagster **orchestrates**. It doesn't move or transform data itself: it runs the tools in every stage
# (a loader, SQL, dbt, a check) in the right order, at the right time, and records what each one produced.

# %%
V.platform_map(tool="dagster")

# %% [markdown]
# Three stores, and three days of orders as the stores app sends them: one CSV per day. Wednesday's file has a
# problem; you'll meet it in §1.

# %%
STORES = pd.DataFrame({"store_id": ["R1", "R2", "J1"], "city": ["Riyadh", "Riyadh", "Jeddah"]})
ORDERS = {
    "2026-09-14": [(101, "R1", 80.50), (102, "R2", 45.00), (103, "J1", 120.00)],
    "2026-09-15": [(104, "R1", 60.00), (105, "J1", 35.25), (106, "J1", 90.00)],
    "2026-09-16": [(107, "R2", -12.00), (108, "R1", 55.00), (109, "J1", 40.00)],
}


def land(day, rows):
    """The stores app drops a day's orders into the inbox."""
    df = pd.DataFrame(rows, columns=["order_id", "store_id", "amount"]).assign(day=day)
    df.to_csv(INBOX / f"orders_{day}.csv", index=False)


for day, rows in ORDERS.items():
    land(day, rows)
print(sorted(p.name for p in INBOX.iterdir()))
pd.read_csv(INBOX / "orders_2026-09-16.csv")

# %% [markdown]
# ## 1 · The problem: a nightly script
#
# The usual first version: three functions, called in order by one `nightly(day)` that cron starts at 01:30. Load the
# day's file, total each store, total each city. It runs on a scratch database.

# %%
SCRIPT_DB = OUT / "by_hand.duckdb"
REPORT_TIMES_OUT = False      # set below to simulate a step failing


def run_sql(query):
    with duckdb.connect(str(SCRIPT_DB)) as con:
        return con.execute(query).df()


def load_orders(day):
    run_sql("CREATE TABLE IF NOT EXISTS orders (order_id INT, store_id VARCHAR, amount DECIMAL(9,2), day DATE)")
    run_sql(f"INSERT INTO orders SELECT * FROM read_csv('{(INBOX / f'orders_{day}.csv').as_posix()}')")


def build_revenue():
    run_sql("CREATE OR REPLACE TABLE store_revenue AS "
            "SELECT day, store_id, sum(amount) AS revenue FROM orders GROUP BY ALL")


def build_report():
    if REPORT_TIMES_OUT:
        raise TimeoutError("the report step timed out")
    run_sql("CREATE OR REPLACE TABLE city_report AS SELECT day, city, sum(revenue) AS revenue "
            f"FROM store_revenue JOIN read_csv('{(OUT / 'stores.csv').as_posix()}') USING (store_id) "
            "GROUP BY ALL ORDER BY day, city")


def nightly(day):
    load_orders(day)
    build_revenue()
    build_report()


STORES.to_csv(OUT / "stores.csv", index=False)
nightly("2026-09-14")
run_sql("SELECT * FROM city_report")

# %% [markdown]
# Monday works. On Tuesday night the report step times out after the load and the revenue step have run. The on-call
# engineer sees the failure in the morning and does the obvious thing: runs Tuesday again.
#
# 🤔 **Predict** · What does Tuesday's report say for Jeddah? Its orders were 35.25 and 90.00.

# %%
REPORT_TIMES_OUT = True
try:
    nightly("2026-09-15")
except TimeoutError as e:
    print("❌ 01:30 run:", e)
REPORT_TIMES_OUT = False
nightly("2026-09-15")                         # the morning re-run
run_sql("SELECT * FROM city_report")

# %% [markdown]
# 🔍 Jeddah shows 250.50 on Tuesday, twice the truth: the re-run loaded Tuesday's file a second time. Nothing failed
# loudly; the report is just wrong. Re-running only the failed step needs someone to know which step failed, and
# that the steps before it must not run again.
#
# On Wednesday the file has a refund written as an order of -12.00, and it goes straight into the totals:

# %%
nightly("2026-09-16")
run_sql("SELECT * FROM city_report WHERE day = '2026-09-16'")

# %% [markdown]
# 🔍 Riyadh's Wednesday is 43.00, not 55.00, and no step looked. Now try to answer: when was `city_report` last built?
# From which files? Is it older than `store_revenue`? The script doesn't know; the answers live in cron's log and in
# people's heads.
#
# | Job | The script | Dagster |
# |---|---|---|
# | Run steps in the right order | the order of lines in `nightly()`, kept by hand | a graph read from the code (§3–4) |
# | Run only what's needed | all or nothing | select an asset and what's downstream (§4) |
# | Know what ran, when, with what result | nothing | every materialization recorded, with metadata (§5) |
# | The same code on a laptop and in production | paths in the code | resources (§6) |
# | Re-run one day, backfill a month | a re-run appends the day again | partitions + idempotent assets (§7) |
# | Stop bad data | it flows into the report | asset checks, blocking (§8) |
# | Start at 01:30, or when a file lands; retry a 429 | cron, and hope | schedules, sensors, retry policies (§9) |
# | The dbt project | one more cron line | dbt models as assets, dbt tests as checks (§10) |

# %% [markdown]
# ## 2 · The mental model: assets, not tasks
#
# ```
#   task-based (cron, Airflow 2)                      asset-based (Dagster)
#
#   load_orders ──► build_revenue ──► build_report    raw/orders ──► store_revenue ──► city_report
#                                                                                     ▲
#   "these steps ran, in this order"                  raw/stores ──────────────────────┘
#                                                     "these tables exist, each built from those,
#                                                      at this time, and their checks passed"
# ```
#
# An **asset** is something you want to exist and keep up to date: a table, a file, a model. You write the function
# that produces it and say what it's built from. Dagster sees the whole graph before running anything.
#
# | You declare | Dagster does |
# |---|---|
# | `@dg.asset` on a function | adds a node to the graph; running it is a **materialization**, recorded with its metadata |
# | a function argument, or `deps=[...]` | an edge: the run order, and what "downstream" means |
# | a selection of assets | runs those, in order, and nothing else |
# | a resource (a database, an API client) | hands it to every asset that asks; swap it for dev or prod |
# | `partitions_def` | one run per day (or month, or store); backfills run many |
# | `@dg.asset_check` | runs right after its asset; if **blocking** and failed, downstream doesn't run |
# | schedules, sensors, retry policies | when runs start, and what happens when a step fails |
#
# You don't write "run A, then B". You write "B is built from A", and the order follows.

# %% [markdown]
# ## 3 · Hello world
#
# Two assets. `cities` takes an argument named `stores`, so it is built from the asset `stores`.

# %%
@dg.asset
def stores():
    return dict(zip(STORES["store_id"], STORES["city"]))


@dg.asset
def cities(stores):
    return sorted(set(stores.values()))


QUIET = {"loggers": {"console": {"config": {"log_level": "CRITICAL"}}}}   # keep the notebook output readable
result = dg.materialize([stores, cities], run_config=QUIET)
print(result.success, result.output_for_node("cities"))

# %% [markdown]
# 🔍 `dg.materialize` runs assets in this process: no server, no UI. Dagster ran `stores` first, saved what it
# returned (by default a pickle file: that's the **IO manager**'s job), and loaded it again to call `cities`.
#
# Every step of a run is an event in its log. Replay it:

# %%
V.asset_run([stores, cities], result)

# %% [markdown]
# Passing Python values between assets suits small things. A data pipeline usually works the other way: each asset
# writes a **table**, and the next one reads the table. Dagster then only needs the order, not the data.

# %% [markdown]
# ## 4 · A graph of tables, and running part of it
#
# Each asset below runs SQL on the toy warehouse and returns a `MaterializeResult` with some **metadata** (here the
# row count) for the record. `deps=` says what it is built from. A `key` with two parts, like `["raw", "orders"]`,
# names the asset `raw/orders`, the same as the table `raw.orders` it writes. `group_name` and `kinds` are labels.

# %%
def rows(table):
    return dg.MaterializeResult(metadata={"rows": int(sql(f"SELECT count(*) AS n FROM {table}")["n"][0])})


@dg.asset(key=["raw", "orders"], group_name="ingest", kinds={"duckdb"})
def raw_orders():
    """Every orders file in the inbox, as sent."""
    sql("CREATE SCHEMA IF NOT EXISTS raw")
    sql(f"""CREATE OR REPLACE TABLE raw.orders AS
            SELECT * FROM read_csv('{(INBOX / 'orders_*.csv').as_posix()}',
                                   columns = {{'order_id': 'INT', 'store_id': 'VARCHAR',
                                               'amount': 'DECIMAL(9,2)', 'day': 'DATE'}})""")
    return rows("raw.orders")


@dg.asset(key=["raw", "stores"], group_name="ingest", kinds={"duckdb"})
def raw_stores():
    """The store list, from the app database."""
    sql("CREATE SCHEMA IF NOT EXISTS raw")
    sql("CREATE OR REPLACE TABLE raw.stores AS SELECT * FROM STORES")
    return rows("raw.stores")


@dg.asset(deps=[raw_orders], group_name="marts", kinds={"duckdb"})
def store_revenue():
    """Revenue per store and day."""
    sql("CREATE OR REPLACE TABLE store_revenue AS "
        "SELECT day, store_id, sum(amount) AS revenue FROM raw.orders GROUP BY ALL")
    return rows("store_revenue")


@dg.asset(deps=[store_revenue, raw_stores], group_name="marts", kinds={"duckdb"})
def city_report():
    """Revenue per city and day."""
    sql("CREATE OR REPLACE TABLE city_report AS SELECT day, city, sum(revenue) AS revenue "
        "FROM store_revenue JOIN raw.stores USING (store_id) GROUP BY ALL ORDER BY day, city")
    return rows("city_report")


@dg.asset(deps=[store_revenue], group_name="marts", kinds={"duckdb"})
def store_ranking():
    """Stores by total revenue."""
    sql("CREATE OR REPLACE TABLE store_ranking AS SELECT store_id, sum(revenue) AS revenue, "
        "rank() OVER (ORDER BY sum(revenue) DESC) AS rank FROM store_revenue GROUP BY ALL")
    return rows("store_ranking")


PIPELINE = [raw_orders, raw_stores, store_revenue, city_report, store_ranking]
V.asset_run(PIPELINE)

# %% [markdown]
# 🔍 Five assets, two sources of data on the left, nothing run yet. The graph came from `deps=` alone.
#
# From here on every run uses one Dagster **instance**: the folder where Dagster keeps its run history (§5). `RUN`
# holds the options every call shares.

# %%
HOME = OUT / "dagster_home"
HOME.mkdir()
(HOME / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")   # the instance's settings file
INSTANCE = dg.DagsterInstance.local_temp(tempdir=str(HOME))
RUN = dict(instance=INSTANCE, run_config=QUIET, raise_on_error=False)

result = dg.materialize(PIPELINE, **RUN)
V.asset_run(PIPELINE, result)

# %%
sql("SELECT * FROM city_report")

# %% [markdown]
# 🔍 The same numbers as the script on Monday, and Tuesday is right this time: `raw/orders` is rebuilt from the files
# on every run (`CREATE OR REPLACE`), so running it twice can't load a file twice. Wednesday's -12.00 is still in;
# that's §8's job.
#
# Now the store list changes: a new store, D1 in Dammam, opens next week. Only the assets built from `raw/stores`
# need to run again.
#
# 🤔 **Predict** · Which assets does `AssetSelection.assets(raw_stores).downstream()` select?

# %%
STORES.loc[len(STORES)] = ["D1", "Dammam"]
selection = dg.AssetSelection.assets(raw_stores).downstream()
result = dg.materialize(PIPELINE, selection=selection, **RUN)
V.asset_run(PIPELINE, result, selection=selection)

# %% [markdown]
# 🔍 Two: `raw/stores` and `city_report`. `store_revenue` doesn't read the store list, so it wasn't touched. That's the
# asset mindset: you ask for data to be up to date, and Dagster works out the steps. Lab 4.1 asks the same question
# of the real warehouse: "the courier events changed, rebuild what uses them".
#
# | Selection | Selects |
# |---|---|
# | `dg.AssetSelection.assets(a, b)` | those assets |
# | `.downstream()` / `.upstream()` | plus everything built from them / everything they're built from |
# | `dg.AssetSelection.groups("marts")`, `.key_prefixes("raw")` | by label or name |
# | `a | b`, `a - b` | union, difference |
#
# ✏️ **Exercise 1** · The finance team wants `city_report` fresh before a meeting. Select `city_report` **and
# everything it is built from**, and materialize that selection. Store the result in `result1`.
# - **Check yourself:** four assets ran; `store_ranking` didn't.

# %%
# Template:
# selection1 = dg.AssetSelection.assets(___).___()
# result1 = dg.materialize(PIPELINE, selection=selection1, **RUN)
# BEGIN SOLUTION
selection1 = dg.AssetSelection.assets(city_report).upstream()
result1 = dg.materialize(PIPELINE, selection=selection1, **RUN)
# END SOLUTION
built1 = {e.asset_key.to_user_string() for e in result1.get_asset_materialization_events()}
print(sorted(built1))

# %%
U.check(result1.success and built1 == {"raw/orders", "raw/stores", "store_revenue", "city_report"},
        "city_report and the three assets it is built from")
U.check("store_ranking" not in built1, "store_ranking was left alone")

# %% [markdown]
# ## 5 · Every run is recorded
#
# The instance keeps every run and every materialization, with its metadata. That's what the script couldn't tell us:
# what was built, when, by which run.

# %%
def last_built(keys):
    out = []
    for key in keys:
        ev = INSTANCE.get_latest_materialization_event(key)
        meta = ev.asset_materialization.metadata if ev else {}
        out.append({"asset": key.to_user_string(),
                    "last built": datetime.fromtimestamp(ev.timestamp).strftime("%H:%M:%S.%f")[:-3] if ev else None,
                    "run": ev.run_id[:8] if ev else None, "rows": meta["rows"].value if "rows" in meta else None})
    return pd.DataFrame(out)


KEYS = [k for a in PIPELINE for k in a.keys]
last_built(KEYS)

# %% [markdown]
# 🤔 **Predict** · Exercise 1 rebuilt `raw/orders` and `store_revenue` but not `store_ranking`. Is `store_ranking`
# still up to date?

# %%
graph = dg.Definitions(assets=PIPELINE).resolve_asset_graph()
built_at = {k: INSTANCE.get_latest_materialization_event(k).timestamp for k in KEYS}
for k in KEYS:
    newer = [p.to_user_string() for p in graph.get(k).parent_keys if built_at[p] > built_at[k]]
    print(f"{k.to_user_string():14}", "stale: built before " + ", ".join(newer) if newer else "up to date")

# %% [markdown]
# 🔍 `store_ranking` is older than the `store_revenue` it reads, so it may be out of date. The data happens to be the
# same here, but nothing has checked. The Dagster UI shows this on every asset, and an automation condition
# (`dg.AutomationCondition.eager()`) can rebuild an asset whenever its parents change. A script can't: it doesn't know
# what it built.
#
# **Break it on purpose.** The record is kept in a folder, so it survives a restart. But it only knows about runs that
# went through Dagster. An analyst "fixes" a number by hand:

# %%
sql("UPDATE store_ranking SET revenue = 0 WHERE store_id = 'J1'")
restarted = dg.DagsterInstance.local_temp(tempdir=str(HOME))   # as if Dagster restarted: the same folder
ev = restarted.get_latest_materialization_event(dg.AssetKey("store_ranking"))
print("runs in the record after a restart:", len(restarted.get_run_records()))
print("store_ranking per the record:", ev.asset_materialization.metadata["rows"].value, "rows, built by run", ev.run_id[:8])
sql("SELECT * FROM store_ranking WHERE store_id = 'J1'")

# %% [markdown]
# 🔍 The record still says `store_ranking` was built by that run, and says nothing about the hand edit. Dagster's
# history is only as complete as the rule "everything that writes these tables runs through Dagster". Checks (§8) look
# at the data itself. Put the table right before moving on:

# %%
result = dg.materialize(PIPELINE, selection=dg.AssetSelection.assets(store_ranking), **RUN)
sql("SELECT * FROM store_ranking")

# %% [markdown]
# ## 6 · Resources: the same code in dev and prod
#
# The assets above use `DB`, a path fixed in the notebook. In production the same assets write to another warehouse,
# with other credentials. A **resource** is an object Dagster hands to every asset that asks for it by name; which one
# it gets is chosen when you run.

# %%
class Warehouse(dg.ConfigurableResource):
    """A DuckDB file. In production this could be a Snowflake or BigQuery client with the same methods."""
    path: str

    def sql(self, query, params=None):
        with duckdb.connect(self.path) as con:
            return con.execute(query, params or []).df()


@dg.asset(group_name="marts", kinds={"duckdb"})
def order_count(warehouse: Warehouse):
    """Orders per day from the inbox files, written to whichever warehouse the run is given."""
    warehouse.sql("CREATE OR REPLACE TABLE order_count AS SELECT day, count(*) AS orders "
                  f"FROM read_csv('{(INBOX / 'orders_*.csv').as_posix()}') GROUP BY day")
    return dg.MaterializeResult(metadata={"warehouse": Path(warehouse.path).name})


for name in ["dev.duckdb", "prod.duckdb"]:
    r = dg.materialize([order_count], resources={"warehouse": Warehouse(path=str(OUT / name))}, **RUN)
    print(name, "→", r.get_asset_materialization_events()[0].materialization.metadata["warehouse"].value)

# %% [markdown]
# 🔍 One asset, two warehouses, no `if env == "prod"` in its code. The run that passes the resource decides. A
# resource is also where secrets belong (read from environment variables, `dg.EnvVar("WAREHOUSE_TOKEN")`), and it
# makes tests easy: give the asset a resource pointing at a scratch file.

# %% [markdown]
# ## 7 · Partitions: one day at a time, and backfills
#
# `raw/orders` rebuilds every file on every run. Fine for three files, not for three years. A **partitioned** asset
# processes one partition (here one day) per run: a bad day is re-run alone, and history is filled in by a
# **backfill**, one run per partition.

# %%
DAYS = dg.DailyPartitionsDefinition(start_date="2026-09-14", end_date="2026-09-17", timezone="Asia/Riyadh")
print(DAYS.get_partition_keys())
sql("CREATE OR REPLACE TABLE orders_daily (order_id INT, store_id VARCHAR, amount DECIMAL(9,2), day DATE)")


@dg.asset(partitions_def=DAYS, group_name="ingest", kinds={"duckdb"})
def orders_daily(context: dg.AssetExecutionContext):
    """One day's orders file, appended."""
    day = context.partition_key
    sql(f"INSERT INTO orders_daily SELECT * FROM read_csv('{(INBOX / f'orders_{day}.csv').as_posix()}')")
    return dg.MaterializeResult(metadata={"rows": int(sql("SELECT count(*) AS n FROM orders_daily WHERE day = ?", [day])["n"][0])})


def backfill(asset, days):
    """One run per partition, one after another: a backfill with concurrency 1."""
    return {day: dg.materialize([asset], partition_key=day, **RUN).success for day in days}


print(backfill(orders_daily, DAYS.get_partition_keys()))
sql("SELECT day, count(*) AS orders FROM orders_daily GROUP BY day ORDER BY day")

# %% [markdown]
# 🔍 Three runs, one per day, each with `context.partition_key` set to its day. The instance records which partitions
# are materialized:

# %%
INSTANCE.get_materialized_partitions(dg.AssetKey("orders_daily"))

# %% [markdown]
# **Break it on purpose.** Retries and backfills are normal: a run times out, someone re-runs Tuesday.
#
# 🤔 **Predict** · We re-run the partition `2026-09-15`. How many orders does Tuesday have afterwards?

# %%
backfill(orders_daily, ["2026-09-15"])
sql("SELECT day, count(*) AS orders FROM orders_daily GROUP BY day ORDER BY day")

# %% [markdown]
# 🔍 Six: §1's bug, now inside Dagster. Dagster runs what you ask it to run; it can't know the asset appends. A
# partitioned asset must be **idempotent**: running a partition twice leaves the same table as running it once.
#
# Three common ways: **delete the partition, then insert it** (Lab 4.1 writes this one), `MERGE` / upsert on a key, or
# overwrite the partition's file. Here, an upsert: `order_id` becomes the primary key and `INSERT OR REPLACE` replaces
# a row that is already there.

# %%
sql("CREATE OR REPLACE TABLE orders_daily (order_id INT PRIMARY KEY, store_id VARCHAR, amount DECIMAL(9,2), day DATE)")


@dg.asset(partitions_def=DAYS, group_name="ingest", kinds={"duckdb"})
def orders_daily(context: dg.AssetExecutionContext):
    """One day's orders file, upserted by order_id."""
    day = context.partition_key
    sql(f"INSERT OR REPLACE INTO orders_daily SELECT * FROM read_csv('{(INBOX / f'orders_{day}.csv').as_posix()}')")
    return dg.MaterializeResult(metadata={"rows": int(sql("SELECT count(*) AS n FROM orders_daily WHERE day = ?", [day])["n"][0])})


print(backfill(orders_daily, DAYS.get_partition_keys()))
print(backfill(orders_daily, ["2026-09-15", "2026-09-15"]))
sql("SELECT day, count(*) AS orders FROM orders_daily GROUP BY day ORDER BY day")

# %% [markdown]
# 🔍 Three a day, however often Tuesday runs. One catch with an upsert: if the stores app re-sends Tuesday **without**
# order 106 (cancelled), the upsert keeps the old 106, because nothing deletes it. Delete-then-insert of the whole
# partition doesn't have that problem. Which one fits depends on what a re-sent file means.
#
# Which date to partition by: the day the data **arrived** (the file's day) for loads, so a re-run reads the same file;
# the day the event **happened** for analytics.

# %% [markdown] 🎓
# 🎓 Ask: "a backfill of 365 days with concurrency 10: what else must be true of the asset?" (Partitions mustn't step
# on each other: each run touches only its day. A `CREATE OR REPLACE` of the whole table in a partitioned asset is the
# classic bug.)

# %% [markdown]
# ## 8 · Asset checks: stop bad data spreading
#
# An **asset check** is a function that looks at an asset's data after it is built and returns pass or fail, with
# metadata. Here: no order may have a negative amount.

# %%
@dg.asset_check(asset=raw_orders, description="An order's amount is never negative")
def no_negative_amounts():
    bad = sql("SELECT order_id, amount FROM raw.orders WHERE amount < 0")
    return dg.AssetCheckResult(passed=bad.empty, metadata={"bad_orders": bad["order_id"].tolist()})


result = dg.materialize(PIPELINE + [no_negative_amounts], **RUN)
V.asset_run(PIPELINE + [no_negative_amounts], result)

# %% [markdown]
# 🔍 The check found order 107 and the failure is on record. But the run **succeeded**, and `store_revenue`,
# `city_report` and `store_ranking` were all built with the -12.00 in them. A check that only reports suits a rule
# someone should look at. For a rule whose failure would put wrong numbers in front of finance, make it **blocking**:
# if it fails, nothing downstream of its asset runs.
#
# ✏️ **Exercise 2** · Write `no_negative_amounts_blocking`: the same check as above, but **blocking**. Then materialize
# `PIPELINE` with it and store the result in `result2`.
# - **Check yourself:** the run fails; `raw/orders` and `raw/stores` are materialized, the three marts are not.

# %%
# Template:
# @dg.asset_check(asset=___, blocking=___, description="An order's amount is never negative")
# def no_negative_amounts_blocking():
#     bad = sql("SELECT order_id, amount FROM raw.orders WHERE amount < 0")
#     return dg.AssetCheckResult(passed=___, metadata={"bad_orders": bad["order_id"].tolist()})
#
#
# result2 = dg.materialize(PIPELINE + [___], **RUN)
# BEGIN SOLUTION
@dg.asset_check(asset=raw_orders, blocking=True, description="An order's amount is never negative")
def no_negative_amounts_blocking():
    bad = sql("SELECT order_id, amount FROM raw.orders WHERE amount < 0")
    return dg.AssetCheckResult(passed=bad.empty, metadata={"bad_orders": bad["order_id"].tolist()})


result2 = dg.materialize(PIPELINE + [no_negative_amounts_blocking], **RUN)
# END SOLUTION
V.asset_run(PIPELINE + [no_negative_amounts_blocking], result2)

# %%
built2 = {e.asset_key.to_user_string() for e in result2.get_asset_materialization_events()}
U.check(not result2.success, "the run failed: a blocking check did not pass")
U.check(built2 == {"raw/orders", "raw/stores"}, "only the two raw assets were materialized")

# %% [markdown]
# 🔍 The marts were **skipped**, so they keep what they held before. Careful: they already hold the -12.00 from the
# non-blocking run above. A blocking check stops bad data from spreading **from now on**; it doesn't undo what an
# earlier run built.
#
# The fix belongs at the source: the stores app re-sends Wednesday with the refund taken out (refunds come from the
# payments feed, not the orders file). Run again:

# %%
land("2026-09-16", [r for r in ORDERS["2026-09-16"] if r[2] > 0])
result = dg.materialize(PIPELINE + [no_negative_amounts_blocking], **RUN)
print("run succeeded:", result.success, "· checks:",
      [(c.check_name, c.passed) for c in result.get_asset_check_evaluations()])
sql("SELECT * FROM city_report WHERE day = '2026-09-16'")

# %% [markdown]
# 🔍 Riyadh's Wednesday is 55.00 again. When a check is blocking, and when it only warns:
#
# | Blocking | Warn only |
# |---|---|
# | the numbers go to finance, customers or a model | a human should look, but the data is still useful |
# | wrong data downstream costs more than late data | late data costs more than slightly wrong data |
# | e.g. negative amounts, duplicate keys, an empty load | e.g. a null rate a bit above normal, a new category |
#
# dbt's tests become asset checks too (§10). Pandera (Day 2) can run inside an asset, before the data is written.

# %% [markdown] 🎓
# 🎓 Ask: "the check is blocking, the data is wrong, the CEO's dashboard is due at 08:00. Who gets woken, and what do
# they see?" That's Day 4's next session: alerts, owners, and what the dashboard says when its data is late.

# %% [markdown]
# ## 9 · When runs start: schedules, sensors, retries
#
# Until now we called `materialize` by hand. A deployed Dagster has a **daemon** that starts runs:
# - a **schedule** on a clock (cron),
# - a **sensor** when something happens: a file lands, another asset changes,
# - and a **retry policy** decides what happens when a step fails.
#
# Each can be tested without waiting for the clock: evaluate one "tick" yourself. First a schedule for the
# partitioned load: every night at 01:30, Riyadh time (the timezone of `DAYS`), load **yesterday's** partition.

# %%
daily_job = dg.define_asset_job("daily_orders", selection=[orders_daily], partitions_def=DAYS)
nightly_schedule = dg.build_schedule_from_partitioned_job(daily_job, hour_of_day=1, minute_of_hour=30)
print("cron:", nightly_schedule.cron_schedule, nightly_schedule.execution_timezone)

tick_time = datetime(2026, 9, 16, 1, 30, tzinfo=ZoneInfo("Asia/Riyadh"))
tick = nightly_schedule.evaluate_tick(dg.build_schedule_context(scheduled_execution_time=tick_time))
print("tick at", tick_time, "→ runs partition", [r.partition_key for r in tick.run_requests])

# %% [markdown]
# 🔍 The 01:30 tick on Wednesday asks for Tuesday's partition: the day that just ended. A schedule built from a
# partitioned job always knows which partition it is for, so a missed night can be re-run as that partition, not as
# "today".
#
# A file doesn't always arrive at 01:30. A **sensor** is a function the daemon calls every 30 seconds or so; it looks
# at the world and returns the runs to start. Between ticks it keeps a **cursor**, a string it writes and gets back
# next time, so it remembers what it has already seen.
#
# ✏️ **Exercise 3** · Complete the sensor `new_orders_files`: list the files in `INBOX` whose name is **greater than
# the cursor** (file names sort by date), yield one `dg.RunRequest(run_key=name)` per new file, and if there were any,
# move the cursor to the last one with `context.update_cursor(...)`.
# - **Check yourself:** the first tick requests three runs, the second none, and the tick after a new file lands
#   requests one.

# %%
pipeline_job = dg.define_asset_job("pipeline", selection=dg.AssetSelection.assets(*PIPELINE))


@dg.sensor(job=pipeline_job, minimum_interval_seconds=30)
def new_orders_files(context: dg.SensorEvaluationContext):
    seen = context.cursor or ""                               # the last file name already handled
    # Template:
    # new = sorted(p.name for p in INBOX.glob("orders_*.csv") if p.name ___ seen)
    # for name in new:
    #     yield dg.RunRequest(run_key=___)
    # if new:
    #     context.update_cursor(new[___])
    # BEGIN SOLUTION
    new = sorted(p.name for p in INBOX.glob("orders_*.csv") if p.name > seen)
    for name in new:
        yield dg.RunRequest(run_key=name)
    if new:
        context.update_cursor(new[-1])
    # END SOLUTION


def sensor_tick(cursor):
    out = new_orders_files.evaluate_tick(dg.build_sensor_context(instance=INSTANCE, cursor=cursor))
    print(f"cursor {cursor!r:28} → runs {[r.run_key for r in out.run_requests]}, new cursor {out.cursor!r}")
    return out


tick1 = sensor_tick(None)
tick2 = sensor_tick(tick1.cursor)
land("2026-09-17", [(110, "R1", 70.00), (111, "J1", 25.50)])
tick3 = sensor_tick(tick2.cursor)

# %%
U.check(len(tick1.run_requests) == 3, "first tick: one run per file already in the inbox")
U.check(len(tick2.run_requests) == 0 and tick2.cursor == tick1.cursor, "second tick: nothing new, cursor unchanged")
U.check([r.run_key for r in tick3.run_requests] == ["orders_2026-09-17.csv"], "a new file: one run")

# %% [markdown]
# 🔍 The cursor is the sensor's memory: without it every tick would see three files and ask for three runs again. The
# `run_key` is a second safety net: the daemon never starts two runs with the same key, so even a sensor that forgets
# its cursor can't process a file twice (Lab 4.1's supplier-file sensor relies on that).
#
# Last, failures. Some are worth retrying (an API's 429 "too many requests", a timeout), most are not (a bug, a bad
# file). A **retry policy** on an asset re-runs its step, with a delay:

# %%
attempts = []


@dg.asset(retry_policy=dg.RetryPolicy(max_retries=3, delay=0.5))
def payments_page():
    """Calls a payments API that answers the first two requests with 429."""
    attempts.append(datetime.now().strftime("%H:%M:%S"))
    if len(attempts) < 3:
        raise ConnectionError("HTTP 429 Too Many Requests")
    return dg.MaterializeResult(metadata={"attempts": len(attempts)})


result = dg.materialize([payments_page], **RUN)
V.asset_run([payments_page], result)

# %% [markdown]
# 🔍 Two failures, two retries, then success, all inside one run. Retries hide flaky failures, so keep them for errors
# that are flaky by nature, and set `backoff=dg.Backoff.EXPONENTIAL` so a struggling API gets more room each time.

# %% [markdown]
# ## 10 · dbt in the graph
#
# Day 2's transformations are dbt models, and dbt has its own graph. `dagster-dbt` reads dbt's `manifest.json` and
# makes **every model an asset** and **every dbt test an asset check**. A dbt source becomes a dependency on the
# asset with the same key, so the Python assets above and the SQL models join into one graph.
#
# A two-model dbt project on the toy warehouse, written here in three files:

# %%
DBT_TOY = OUT / "dbt_toy"
(DBT_TOY / "models").mkdir(parents=True)
(DBT_TOY / "dbt_project.yml").write_text(
    "name: toy\nprofile: toy\nmodels:\n  toy:\n    +materialized: table\n", encoding="utf-8")
(DBT_TOY / "profiles.yml").write_text(
    "toy:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n"
    "      path: \"{{ env_var('QAFILA_DUCKDB') }}\"\n      schema: marts\n", encoding="utf-8")
(DBT_TOY / "models" / "schema.yml").write_text("""
sources:
  - name: raw
    tables: [{name: orders}, {name: stores}]
models:
  - name: stg_orders
    columns:
      - name: order_id
        data_tests: [unique, not_null]
  - name: fct_city_sales
    columns:
      - name: city
        data_tests: [not_null]
""", encoding="utf-8")
(DBT_TOY / "models" / "stg_orders.sql").write_text(
    "select order_id, store_id, amount, day from {{ source('raw', 'orders') }} where amount > 0\n", encoding="utf-8")
(DBT_TOY / "models" / "fct_city_sales.sql").write_text(
    "select o.day, s.city, sum(o.amount) as revenue, count(*) as orders\n"
    "from {{ ref('stg_orders') }} o join {{ source('raw', 'stores') }} s using (store_id)\n"
    "group by all\n", encoding="utf-8")
from dagster_dbt import DbtCliResource, dbt_assets

os.environ["QAFILA_DUCKDB"] = DB.resolve().as_posix()          # profiles.yml reads the warehouse path from here
dbt = DbtCliResource(project_dir=str(DBT_TOY), profiles_dir=str(DBT_TOY))
dbt.cli(["parse"], target_path=Path("target")).wait()           # dbt reads the project, writes target/manifest.json
print(sorted(p.name for p in (DBT_TOY / "target").iterdir() if p.suffix == ".json"))

# %%
@dbt_assets(manifest=DBT_TOY / "target" / "manifest.json")
def toy_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


print("assets:", sorted(k.to_user_string() for k in toy_dbt.keys))
print("checks:", sorted(c.name for c in toy_dbt.check_keys))

# %% [markdown]
# 🤔 **Predict** · The dbt sources are `raw.orders` and `raw.stores`. In the combined graph, what do the dbt models
# hang off?

# %%
WITH_DBT = [raw_orders, raw_stores, toy_dbt]
result = dg.materialize(WITH_DBT, resources={"dbt": dbt}, **RUN)
V.asset_run(WITH_DBT, result)

# %% [markdown]
# 🔍 `raw/orders` and `raw/stores`, the Python assets: dbt's `source('raw', 'orders')` became the key `raw/orders`. One
# graph, from the file in the inbox to the dbt mart. dbt's three tests ran as checks right after their models, and
# they are **blocking** (thick outline): a dbt test with the default severity `error` becomes a blocking check, one
# with `severity: warn` doesn't. Select `dg.AssetSelection.assets(raw_orders).downstream()` and Dagster runs the loader, then `dbt build --select`
# only the models that depend on it.

# %%
sql("SELECT * FROM marts.fct_city_sales ORDER BY day, city")

# %% [markdown]
# The real thing: the Qafila dbt project from Lab 2.3, read the same way. Lab 4.1 puts it in a graph with the courier
# events loader.

# %%
qafila = DbtCliResource(project_dir=str(U.DBT_PROJECT), profiles_dir=str(U.DBT_PROJECT))
qafila.cli(["parse"], target_path=Path("target")).wait()        # parse reads the SQL only: no database is opened


@dbt_assets(manifest=U.DBT_PROJECT / "target" / "manifest.json")
def qafila_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


print(len(qafila_dbt.keys), "assets and", len(qafila_dbt.check_keys), "checks from the Qafila dbt project")
V.asset_run([qafila_dbt])

# %% [markdown]
# 🔍 Every model is an asset and every test a check, with no Dagster code per model. The `raw/...` boxes on the left
# are the dbt sources: external here, because no asset in this graph builds them. In Lab 4.1 you write the loader for
# `raw/delivery_events`, and it joins this graph. The checks are grouped here, one pill per model; a pill with a thin
# outline holds one of the project's three tests marked `severity: warn`, the only non-blocking ones.

# %% [markdown]
# ## 11 · Back to the map
#
# Dagster **orchestrates**: it runs the tools of every stage in order, on time, and keeps the record.

# %%
V.platform_map(tool="dagster")

# %% [markdown]
# In a real project everything above lives in a Python package, collected in one `dg.Definitions` object: the assets,
# checks, jobs, schedules, sensors and resources. `dagster dev` serves the UI on it (the graph, runs, backfills,
# checks), and in production a daemon runs the schedules and sensors. The capstone has one:
# `uv run dagster dev -f capstone/pipeline.py`.

# %%
defs = dg.Definitions(
    assets=PIPELINE + [orders_daily, toy_dbt],
    asset_checks=[no_negative_amounts_blocking],
    jobs=[daily_job, pipeline_job],
    schedules=[nightly_schedule],
    sensors=[new_orders_files],
    resources={"dbt": dbt, "warehouse": Warehouse(path=str(OUT / "dev.duckdb"))},
)
print(len(defs.resolve_asset_graph().get_all_asset_keys()), "assets in this code location")

# %% [markdown]
# | | |
# |---|---|
# | **It does** | a graph of assets from the code · runs a selection in order · records every materialization with metadata · resources for dev and prod · partitions and backfills · asset checks, blocking or not · schedules, sensors, retries · dbt models and tests in the same graph · a UI for all of it |
# | **It doesn't** | move or transform data itself (your SQL, dlt, dbt, Polars do) · make an asset idempotent (you do, §7) · know about changes made outside it (§5) |
# | **Neighbours** | Apache Airflow 3 (the most deployed; tasks, with assets added) · Prefect (Python flows) · Kestra (YAML) · cron (no graph, no record) |
# | **Managed equivalents** | Dagster+ · Astronomer, Amazon MWAA, Google Cloud Composer (Airflow) · Databricks Workflows, Snowflake tasks, dbt platform jobs, Fabric pipelines |
#
# **Cheat sheet**
#
# | Code | Does |
# |---|---|
# | `@dg.asset(key=[...], deps=[...], group_name=..., kinds={...})` | an asset; `deps` are what it's built from |
# | `def b(a): ...` | `b` is built from `a`, and gets `a`'s value (IO manager) |
# | `return dg.MaterializeResult(metadata={...})` | record facts about this materialization |
# | `dg.materialize(assets, selection=..., partition_key=..., resources=...)` | run in this process |
# | `dg.AssetSelection.assets(a).downstream()` / `.upstream()` | a and what's built from it / what it's built from |
# | `instance.get_latest_materialization_event(key)` | when, by which run, with what metadata |
# | `class R(dg.ConfigurableResource)` + an argument `r: R` | a resource, chosen per run |
# | `dg.DailyPartitionsDefinition(start_date=...)` + `context.partition_key` | one run per day; make it idempotent |
# | `@dg.asset_check(asset=a, blocking=True)` → `dg.AssetCheckResult(passed=...)` | a check; blocking stops downstream |
# | `dg.build_schedule_from_partitioned_job(job, hour_of_day=1)` | nightly run of yesterday's partition |
# | `@dg.sensor(job=...)`, `context.cursor`, `dg.RunRequest(run_key=...)` | runs when something happens, at most once per key |
# | `dg.RetryPolicy(max_retries=3, delay=1)` | retry a failed step |
# | `@dbt_assets(manifest=...)` + `dbt.cli(["build"], context=context).stream()` | dbt models as assets, tests as checks |
#
# ## Checkpoint
#
# 1. The script's morning re-run doubled Tuesday. Which two things in §4 and §7 stop that from happening?
# 2. Task-based vs asset-based: what does the orchestrator know in each case?
# 3. `store_ranking` was stale in §5. How did we know, and what would the script have known?
# 4. A blocking check failed. What do the skipped assets contain now?
# 5. The supplier files arrive "sometime on Monday". Schedule or sensor? And what makes the sensor safe to tick every
#    30 seconds?
#
# Next: [Lab 4.1 · Orchestrating Qafila with Dagster](../D4_1_orchestration_dagster.ipynb). Docs:
# [assets](https://docs.dagster.io/guides/build/assets) ·
# [partitions](https://docs.dagster.io/guides/build/partitions-and-backfills) ·
# [asset checks](https://docs.dagster.io/guides/test/asset-checks) ·
# [schedules](https://docs.dagster.io/guides/automate/schedules) ·
# [sensors](https://docs.dagster.io/guides/automate/sensors) ·
# [dbt](https://docs.dagster.io/integrations/libraries/dbt).
#
# ## 🖼️ Visual summary
#
# **The pipeline with dbt in it:** the inbox loader and the store list in Python, the models and their tests from dbt,
# one graph and one run.

# %%
V.asset_run(WITH_DBT + [no_negative_amounts_blocking],
            dg.materialize(WITH_DBT + [no_negative_amounts_blocking], resources={"dbt": dbt}, **RUN))
