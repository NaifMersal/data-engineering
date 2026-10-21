# %% [markdown]
# # Polars, one idea at a time
#
# Day 2, before Lab 2.1 · CPU only. Lab 2.1 cleans bronze into silver with **Polars**, a fast DataFrame library, and
# expects you to chain a filter, a join, a parse and an aggregation in one go. Labs 2.4, 3.1 and the capstone use it
# too. You first answer one question with a hand-written Python loop and watch it slow down, then build Polars up one
# idea at a time.
#
# **The data:** Qafila's own, from the start: a 12-row stores table you can check by eye, then the 45,766 orders of the
# app database and the courier feed. Lab 2.1 then cleans the messy parts (timestamps, duplicates, customers).
#
# **Rhythm:** 🤔 predict → run → 🔍 read. Write your prediction down before you run the cell.
#
# **Exercises (✏️):** uncomment the template (`Ctrl+/`), replace every `___`, delete the two `YOUR CODE HERE` lines,
# then run the cell and the check after it (✅ = right). Answers: `solutions/tools/polars.ipynb`.
#
# **Contents** · 🎯 walked in class (minutes) · ➕ at your own pace, like the ✏️ exercises not done in class · core ≈ 27'
# - [ ] 1 · The problem: a question answered by a loop · 🎯 3'
# - [ ] 2 · The mental model: you write the rules, the engine runs them · 🎯 3'
# - [ ] 3 · Hello world: a DataFrame is a table of typed columns · 🎯 2'
# - [ ] 4 · Expressions and `select` · 🎯 4', Ex. 2 filled in together
# - [ ] 5 · `filter`: keep the rows you want · 🎯 2'
# - [ ] 6 · `with_columns`: add and fix columns · 🎯 2'
# - [ ] 7 · `group_by` + `agg`: one row per group · 🎯 3'
# - [ ] 8 · Joins: combine tables · 🎯 2' one join · ➕ orphans
# - [ ] 9 · `over`: group answers without collapsing rows · ➕
# - [ ] 10 · Lazy mode: describe the query, let Polars plan it · 🎯 5'
# - [ ] 11 · Put it together: one question, one chain · ➕
# - [ ] 12 · Back to the map · 🎯 1'
#
# Know pandas? Polars looks similar but thinks differently: there is no index, nothing is changed in place (every
# method returns a new DataFrame), and you describe columns with **expressions** instead of indexing with `df[...]`.
# §4 is the one to read slowly.

# %% [markdown] 🎓
# 🎓 **Instructor notes** (solution notebook only)
#
# - Core ≈ 27' (🎯 in the contents; walked in class so far in 30–50' with exercises). §1–2 set the frame (6'); §4
#   (expressions and contexts) is the one to slow down on. §9, §11 and the exercises are ➕, except Ex. 2: fill it in
#   together (the room calls out the blanks), because §5, §8 and §11 use `lines`.
# - In class, run the cells of a ➕ section without stopping on them: later cells may use what they build.
# - §1 reads the courier feed 40 times (≈900k events): the loop takes several seconds, Polars well under one. The
#   ratio is the point, not the numbers; quick mode reads it 10 times.
# - Lab 2.1 parses three timestamp formats, picks one copy per `event_id` and cleans customers: don't teach those here.
#   §6's time zones and §10's plans are what it builds on.

# %%
import os, sys, subprocess
from pathlib import Path

IN_COLAB = "google.colab" in sys.modules
if IN_COLAB:
    REPO_URL = "https://github.com/SDAIAAcademy/data-engineering.git"
    if not Path("/content/data-engineering").exists():
        subprocess.run(["git", "clone", "-q", REPO_URL, "/content/data-engineering"], check=True)
    os.chdir("/content/data-engineering/labs")
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "-r", "requirements-colab.txt"], check=True)
else:
    while not Path("de_utils.py").exists() and Path.cwd() != Path.cwd().parent:
        os.chdir("..")  # tools/ and solutions/tools/ notebooks run from labs/
sys.path.insert(0, os.getcwd())

import de_utils as U
U.ensure_data()
print("data:", U.RAW, "| quick mode:", U.QUICK)

# %%
import sqlite3
import polars as pl

pl.Config.set_tbl_rows(8)                                  # print at most 8 rows of a table
app = sqlite3.connect(U.RAW / "qafila_oltp.sqlite")       # Qafila's app database (SQLite)
print("polars", pl.__version__)

# %% [markdown]
# You are here: Polars **transforms** (cleaning bronze into silver, Lab 2.1) and **tests** (checks on DataFrames,
# Lab 2.4). It does in Python, with a DataFrame API, what DuckDB does with SQL.

# %%
import de_viz as V
V.platform_map(tool="polars")

# %% [markdown]
# ## 1 · The problem: a question answered by a loop
#
# The operations team asks: **how many couriers delivered at least one order in each city?** The answer is in the
# courier feed, one JSON object per line. Plain Python can answer it with a loop. To see how the loop grows with the
# data, it reads the feed 40 times, as if it held 40 times more events.

# %%
import json
from collections import defaultdict

FEED = U.RAW / "events" / "delivery_events.jsonl"
COPIES = U.pick(40, 10)                      # read the feed this many times (fewer in quick mode)
times = {}

with U.timer("a Python loop", times):
    couriers = defaultdict(set)               # city → the couriers seen delivering there
    events_read = 0
    for _ in range(COPIES):
        with open(FEED, encoding="utf-8") as f:
            for line in f:
                event = json.loads(line)      # parse all 10 fields of every event...
                events_read += 1
                if event["event_type"] == "delivered":   # ...to keep 2 of them, from 1 event in 4
                    couriers[event["city"]].add(event["courier_id"])

by_loop = {city: len(ids) for city, ids in sorted(couriers.items())}
print(f"{events_read:,} events read →", by_loop)

# %% [markdown]
# 🤔 **Predict** · The same question in Polars, on the same 40 copies. How much faster: twice, ten times?

# %%
with U.timer("Polars", times):
    by_polars = (
        pl.scan_ndjson([FEED] * COPIES)
          .filter(pl.col("event_type") == "delivered")
          .group_by("city")
          .agg(pl.col("courier_id").n_unique().alias("couriers"))
          .sort("city")
          .collect()
    )
print(by_polars.rows())
U.check(dict(by_polars.rows()) == by_loop, "the same answer as the loop")
print(f"Polars was {times['a Python loop'] / times['Polars']:.0f}× faster")

# %% [markdown]
# 🔍 The loop is correct, and on one copy of the feed it takes a fraction of a second. But it grows with the data on
# one core, and **how** it runs is welded to **what** it computes: the rule ("delivered, distinct couriers, per city")
# is spread over a `for`, an `if`, a `dict` and a `set`. Every job the loop does by hand, Polars does for you:
#
# | Job | The loop | Polars (§) |
# |---|---|---|
# | Know what each value is | strings and numbers as `json.loads` returns them | typed columns (§3, §6) |
# | Say what to compute | a `for`, an `if`, a dict of sets | expressions in a context (§4, §5, §7) |
# | Combine sources | look-up dicts you build yourself | `join` (§8) |
# | Group answers next to each row | a second loop | `over` (§9) |
# | Read only what the question needs | parses all 10 fields of every line | projection pushdown (§10) |
# | Drop rows early | wherever you put the `if` | predicate pushdown (§10) |
# | Use every core | one core | parallel by default (§2) |
# | Change the question | rewrite the loop | change one expression (§11) |
#
# ## 2 · The mental model: you write the rules, the engine runs them
#
# ```
#  a DataFrame (eager)        expressions          a plan             execution
#  or a LazyFrame (lazy)  →   in a context    →    optimized     →    on columns, in batches,  →  a DataFrame
#  typed columns              select, filter, …    (pushdowns)        on every core
#  └──────────────── you write this ────────────────┘  └───────────── Polars does this ─────────────┘
# ```
#
# | You declare | Polars does |
# |---|---|
# | the columns you use (`pl.col("city")`) | reads only those columns (**columnar reading**) |
# | a filter (`pl.col("event_type") == "delivered"`) | applies it as early as it can, inside the scan if possible (**predicate pushdown**) |
# | an operation on a column (`n_unique`, `*`, `.str.to_datetime`) | runs it on many values at once, not row by row (**vectorized**) |
# | several independent expressions, or groups | runs them on all CPU cores (**parallel**) |
# | the whole query, lazily | plans it before reading anything, and shows you the plan (`.explain()`) |
#
# This is the same split as SQL in a database: you say **what**, the engine decides **how**. DuckDB (Day 1) gives
# you that split with SQL, Polars with Python expressions; under the hood they share the same ideas, and Lab 2.1 runs
# the same question in both. What neither engine decides for you is the **meaning**: which timestamp is the real
# one, which copy of a duplicate survives, what counts as revenue. Those rules are yours to write.
#
# ## 3 · Hello world: a DataFrame is a table of typed columns
#
# Qafila has 12 dark stores. `pl.read_database` runs a SQL query against a database connection and returns the result
# as a Polars **DataFrame**.

# %%
stores = pl.read_database("SELECT * FROM stores", app)
stores

# %% [markdown]
# Read the header before the rows. Under each column name, Polars prints its **data type**: `str` (text), `f64`
# (a 64-bit decimal number), `i64` (a 64-bit integer)... Every value of a column has that one type. A DataFrame is
# really a set of named, typed columns of the same length. The `shape` is `(rows, columns)`.

# %%
print(stores.shape)
print(stores.columns)
stores.schema

# %% [markdown]
# 🤔 **Predict** · `opened_at` holds dates like `2024-01-01`. What is its type: a date or text?
#
# It's `str`. SQLite has no date type, so the app stores dates as text, and Polars reads what it is given. Most raw
# data arrives like this; turning text into real types is a big part of cleaning it (§6).
#
# One column on its own is a **Series**: a name, a type and the values. Take one out with `df["name"]`. A Series has
# the methods you'd expect: `min`, `max`, `mean`, `n_unique`, `unique`, `to_list`...

# %%
city = stores["city"]
print(type(city).__name__, "|", city.name, "|", city.dtype, "|", city.len(), "values")
city.unique().sort().to_list()

# %% [markdown]
# ✏️ **Exercise 1** · How many stores are in Riyadh? Use the Series `stores["city"]`: comparing a Series with a value,
# like `series == "x"`, gives a Series of `true`/`false`, and `.sum()` counts the `true`s.
# - **Result:** `n_riyadh`, a number.
# - **Check yourself:** it's half of the 12 stores.

# %%
# Template:
# n_riyadh = (stores["___"] == "___").sum()
# BEGIN SOLUTION
n_riyadh = (stores["city"] == "Riyadh").sum()
# END SOLUTION
n_riyadh

# %%
U.check(n_riyadh == 6, "6 of the 12 stores are in Riyadh")

# %% [markdown]
# Now the real tables. The same reader loads all 45,766 orders and 172,545 order lines in well under a second.
# `head(n)` shows the first rows; `glimpse()` prints one line per column, which is easier to read for wide tables.

# %%
orders = pl.read_database("SELECT * FROM orders", app)
items = pl.read_database("SELECT * FROM order_items", app)
customers = pl.read_database("SELECT * FROM customers", app)
print(f"orders {orders.shape}  items {items.shape}  customers {customers.shape}")
orders.glimpse()

# %% [markdown]
# 🔍 `created_at` is `str` again, and `promo_code` is `null` on most rows: **null** is Polars' "no value" (SQL's
# `NULL`), and every type can hold it. `orders.null_count()` counts them per column.
#
# Polars reads files directly too: `pl.read_csv`, `pl.read_parquet`, `pl.read_ndjson` (one JSON object per line), and
# more. Same DataFrame, whatever the source:

# %%
reviews = pl.read_csv(U.RAW / "reviews.csv")
payments = pl.read_ndjson(U.RAW / "api" / "payments.jsonl")
print(f"reviews {reviews.shape}  payments {payments.shape}")
payments.head(3)

# %% [markdown]
# ## 4 · Expressions and `select`
#
# This is the one idea the rest of Polars is built on. `pl.col("delivery_fee")` does **not** fetch a column. It is an
# **expression**: a recipe that says "take the column called `delivery_fee`". You can extend a recipe with operators
# and methods, and nothing is computed yet:

# %%
fee_with_vat = pl.col("delivery_fee") * 1.15
print(fee_with_vat)

# %% [markdown]
# An expression is computed only when you hand it to a DataFrame method that runs it, a **context**. There are four,
# one per section below:
#
# | context | what it does with your expressions | rows out |
# |---|---|---|
# | `df.select(...)` | computes them; keeps **only** them | same, or 1 if they aggregate |
# | `df.with_columns(...)` | computes them; adds them to (or replaces) the existing columns | same |
# | `df.filter(...)` | keeps the rows where the expression is `true` | fewer |
# | `df.group_by(...).agg(...)` | computes them once per group | one per group |
#
# Because the recipe is separate from the data, the same expression works on any DataFrame that has those columns, and
# Polars can look at all your recipes before running them, to run them in parallel or skip work (§10).
#
# `select` with column names picks columns. With expressions, it computes new ones.

# %%
orders.select("order_id", "store_id", fee_with_vat)

# %% [markdown]
# 🤔 **Predict** · The last column was computed as `delivery_fee * 1.15`. What is it **called** in the output? And what
# happens if you also ask for the original `delivery_fee`?

# %%
try:
    orders.select("order_id", "store_id", "delivery_fee", fee_with_vat)
except pl.exceptions.PolarsError as e:
    print(type(e).__name__, "→", str(e).splitlines()[0])

# %% [markdown]
# 🔍 A computed column takes the name of the first column in its expression: `delivery_fee`. Next to the real
# `delivery_fee`, that's two columns with one name, and Polars refuses. Name every computed column yourself with
# `.alias(...)`:

# %%
orders.select("order_id", "store_id", "delivery_fee", fee_with_vat.alias("fee_with_vat"))

# %% [markdown]
# Some expressions **aggregate**: `sum`, `mean`, `min`, `max`, `n_unique`... turn a column into one value.
# `pl.len()` counts rows.
#
# 🤔 **Predict** · How many rows does the first `select` below return? And the second one, which mixes a plain column
# with an aggregate?

# %%
orders.select(pl.len().alias("orders"), pl.col("delivery_fee").sum().alias("fees"), pl.col("customer_id").n_unique())

# %%
orders.select("order_id", "delivery_fee", pl.col("delivery_fee").mean().alias("avg_fee"))

# %% [markdown]
# 🔍 One row when everything aggregates. When one expression returns 45,766 values and another returns one, Polars
# repeats (**broadcasts**) the single value on every row, like SQL's `avg(delivery_fee) OVER ()`.
#
# ✏️ **Exercise 2** (🎯 together in class) · Compute the amount of every order line: `quantity * unit_price - discount`,
# rounded to 2 decimals.
# - **Result:** `lines`, one row per line of `items`, columns `order_id`, `line_no`, `line_total`.
# - **Check yourself:** order 5000001's first line is 2 × 49.20 SAR = 98.4.

# %%
# Template:
# lines = items.select(
#     "order_id", "line_no",
#     (pl.col("___") * pl.col("___") - pl.col("___")).round(2).alias("___"),
# )
# BEGIN SOLUTION
lines = items.select(
    "order_id", "line_no",
    (pl.col("quantity") * pl.col("unit_price") - pl.col("discount")).round(2).alias("line_total"),
)
# END SOLUTION
lines.head(3)

# %%
U.check(lines.columns == ["order_id", "line_no", "line_total"], "three columns, named as asked")
U.check(lines.height == items.height, "one row per order line: nothing aggregated")
U.check(lines["line_total"][0] == 98.4, "order 5000001, line 1: 2 × 49.20 = 98.4 SAR")

# %% [markdown]
# ## 5 · `filter`: keep the rows you want
#
# `filter` takes an expression that gives `true` or `false` per row (a **predicate**) and keeps the `true` rows.

# %%
orders.filter(pl.col("status") == "cancelled")

# %% [markdown]
# Combine predicates with `&` (and), `|` (or) and `~` (not), **not** with Python's `and`/`or`/`not`. Other useful
# predicates: `.is_in([...])`, `.is_between(a, b)`, `.is_null()`, `.str.contains(...)`.
#
# 🤔 **Predict** · The next cell wants cancelled orders with a free delivery. Will it work?

# %%
try:
    orders.filter(pl.col("status") == "cancelled" & pl.col("delivery_fee") == 0)
except TypeError as e:
    print("TypeError →", str(e).splitlines()[0])

# %% [markdown]
# 🔍 In Python, `&` binds **tighter** than `==`, so this reads `pl.col("status") == ("cancelled" & pl.col(...)) == 0`:
# nonsense. Put each comparison in parentheses:

# %%
orders.filter((pl.col("status") == "cancelled") & (pl.col("delivery_fee") == 0))

# %% [markdown]
# Several predicates separated by commas are combined with `&` for you, which is often easier to read:

# %%
orders.filter(pl.col("status").is_in(["cancelled", "failed_delivery"]), pl.col("promo_code").is_not_null())

# %% [markdown]
# ✏️ **Exercise 3** · The app database has a planted problem: some order lines have a quantity of zero or less, which
# no real basket has. Find them in `items`.
# - **Result:** `bad_lines`, the rows of `items` whose `quantity` is `<= 0` (all 6 columns).
# - **Check yourself:** a handful out of 172,545: a bit over a hundred.

# %%
# Template:
# bad_lines = items.filter(pl.col("___") ___ 0)
# BEGIN SOLUTION
bad_lines = items.filter(pl.col("quantity") <= 0)
# END SOLUTION
print(bad_lines.height, "bad lines")
bad_lines.head(3)

# %%
U.check(bad_lines.height == 142, "142 lines with a quantity of zero or less")
U.check(bad_lines.columns == items.columns, "all the columns of items, just fewer rows")

# %% [markdown]
# 🔍 Finding bad rows is easy; deciding what to do with them (drop, fix, quarantine, ask the source team) is a data
# engineering decision. Lab 2.4 builds a quarantine for exactly this.
#
# ## 6 · `with_columns`: add and fix columns
#
# `select` keeps only what you list. `with_columns` keeps **every** column and adds yours; an expression whose output
# name already exists **replaces** that column. That makes it the tool for fixing types.
#
# `created_at` is text. The `.str` methods of an expression work on text; `.str.to_datetime()` parses it:

# %%
orders.with_columns(pl.col("created_at").str.to_datetime()).select("order_id", "created_at").head(3)

# %% [markdown]
# The type is now `datetime[μs]`, and a datetime has its own methods under `.dt`: `.dt.date()`, `.dt.hour()`,
# `.dt.weekday()`... These groups of methods are called **namespaces**: `.str` for text, `.dt` for dates and times,
# `.list` for lists. Only the right type has them.
#
# 🤔 **Predict** · The next cell parses `created_at` and, in the **same** `with_columns`, takes its date. Will it work?

# %%
try:
    orders.with_columns(
        pl.col("created_at").str.to_datetime(),
        pl.col("created_at").dt.date().alias("order_date"),
    )
except pl.exceptions.PolarsError as e:
    print(type(e).__name__, "→", str(e).splitlines()[0])

# %% [markdown]
# 🔍 All the expressions of one context run **in parallel**, on the DataFrame as it was **before** the context. The
# second expression still sees the text `created_at`. When one column depends on another new column, chain two
# contexts:

# %%
orders.with_columns(pl.col("created_at").str.to_datetime()).with_columns(
    pl.col("created_at").dt.date().alias("order_date")
).select("order_id", "created_at", "order_date").head(3)

# %% [markdown]
# One more real-world detail. The app stores `created_at` in **UTC**, but Qafila's customers live
# on Riyadh time, three hours ahead. To ask "at what hour do people order?", say the text is UTC
# (`.dt.replace_time_zone`), then convert (`.dt.convert_time_zone`). Here is a clean `orders` that the rest of the lab
# uses:

# %%
orders = orders.with_columns(
    pl.col("created_at").str.to_datetime().dt.replace_time_zone("UTC").dt.convert_time_zone("Asia/Riyadh"),
    pl.col("updated_at").str.to_datetime().dt.replace_time_zone("UTC").dt.convert_time_zone("Asia/Riyadh"),
)
orders.select("order_id", "created_at", "updated_at").head(3)

# %% [markdown]
# Order 5000001 was placed at 12:14 in Riyadh: the text said `09:14:07`, in UTC.
#
# **Conditions** with `pl.when(...).then(...).otherwise(...)`, Polars' `CASE WHEN`:

# %%
orders.with_columns(
    pl.when(pl.col("delivery_fee") == 0).then(pl.lit("free")).otherwise(pl.lit("paid")).alias("fee_type")
).select("order_id", "delivery_fee", "fee_type").head(3)

# %% [markdown]
# Note `pl.lit("free")`. A plain string in an expression context means a **column name**: `.then("free")` would look for
# a column called `free` and fail. `pl.lit` makes it a literal value.
#
# ✏️ **Exercise 4** · Add two columns to `orders`, from `created_at` (already Riyadh time):
# - `hour`: the hour of the day, 0 to 23 (`.dt.hour()`);
# - `is_weekend`: `true` when the order was placed on a Friday or a Saturday, Saudi Arabia's weekend. `.dt.weekday()`
#   numbers the days from Monday = 1 to Sunday = 7.
# - **Result:** `orders_t`, every column of `orders` plus `hour` and `is_weekend`.
# - **Check yourself:** `orders_t` has 11 columns; the busiest hour is in the evening.

# %%
# Template:
# orders_t = orders.with_columns(
#     pl.col("created_at").dt.___().alias("hour"),
#     pl.col("created_at").dt.weekday().is_in([___, ___]).alias("is_weekend"),
# )
# BEGIN SOLUTION
orders_t = orders.with_columns(
    pl.col("created_at").dt.hour().alias("hour"),
    pl.col("created_at").dt.weekday().is_in([5, 6]).alias("is_weekend"),
)
# END SOLUTION
orders_t.select("order_id", "created_at", "hour", "is_weekend").head(3)

# %%
U.check(orders_t.width == 11 and orders_t.height == orders.height, "two columns added, no rows lost")
U.check(orders_t["hour"].mode().item() == 20, "the busiest hour is 20:00 to 21:00, Riyadh time")
weekend_days = orders_t.filter(pl.col("is_weekend"))["created_at"].dt.strftime("%A").unique().sort().to_list()
U.check(weekend_days == ["Friday", "Saturday"], "weekend = Friday and Saturday")

# %% [markdown]
# ## 7 · `group_by` + `agg`: one row per group
#
# `group_by(keys).agg(expressions)` splits the rows into groups, one per distinct key, and computes each expression
# once per group. It's SQL's `GROUP BY`.

# %%
orders.group_by("status").agg(pl.len().alias("orders"), pl.col("delivery_fee").mean().round(2).alias("avg_fee"))

# %% [markdown]
# 🤔 **Predict** · Run the cell above three or four times. Will the rows always come out in the same order?
#
# 🔍 Probably not. Polars computes the groups in parallel and gives them back in whatever order they finish. If the
# order matters (to read a result, to compare it in a test), say so: `.sort(...)` after the `agg`, or
# `group_by(..., maintain_order=True)`.
#
# Any expression works inside `agg`, including filters and comparisons. One trick worth knowing: the **mean of a
# `true`/`false` column** is the share of `true`s, so `(pl.col("status") == "cancelled").mean()` is a cancellation rate.
#
# ✏️ **Exercise 5** · Compare the stores: for each `store_id`, count its orders and compute its cancellation rate.
# - **Result:** `by_store`, 12 rows, columns `store_id`, `orders`, `cancel_rate` (rounded to 3 decimals), sorted from
#   the highest cancellation rate to the lowest.
# - **Check yourself:** the orders add up to 45,766, and every rate is between 6% and 8%.

# %%
# Template:
# by_store = (
#     orders.group_by("___")
#           .agg(pl.len().alias("orders"),
#                (pl.col("status") == "___").mean().round(3).alias("cancel_rate"))
#           .sort("___", descending=True)
# )
# BEGIN SOLUTION
by_store = (
    orders.group_by("store_id")
          .agg(pl.len().alias("orders"),
               (pl.col("status") == "cancelled").mean().round(3).alias("cancel_rate"))
          .sort("cancel_rate", descending=True)
)
# END SOLUTION
by_store

# %%
U.check(by_store.height == 12 and by_store["orders"].sum() == orders.height, "12 stores, every order counted once")
U.check(by_store["cancel_rate"].is_between(0.06, 0.08).all(), "cancellation rates between 6% and 8%")
U.check(by_store["cancel_rate"].is_sorted(descending=True), "sorted from the highest rate down")

# %% [markdown]
# 🔍 No store stands out: cancellations are spread evenly, so a cancellation problem would be a platform problem, not a
# store problem. That's the kind of sentence a `group_by` lets you write.
#
# ## 8 · Joins: combine tables
#
# Orders know their `store_id`; stores know their `city`. `join` matches rows on a key column:

# %%
orders.join(stores.select("store_id", "city"), on="store_id").select("order_id", "store_id", "city", "status").head(3)

# %% [markdown]
# 🤔 **Predict** · `orders` has 45,766 rows and `items` 172,545. How many rows does `orders.join(items, on="order_id")`
# have?

# %%
orders.join(items, on="order_id").shape

# %% [markdown]
# 🔍 172,545: one row per **line**. An order with three lines matches three rows of `items`, so its order columns are
# repeated three times. Always ask "one row per what?" after a join: summing `delivery_fee` after this join would count
# each fee once per line.
#
# The `how` argument says what to do with rows that don't match:
#
# | `how=` | keeps |
# |---|---|
# | `"inner"` (default) | only the rows that match on both sides |
# | `"left"` | every row of the left table; nulls where the right has no match |
# | `"anti"` | only the left rows that have **no** match on the right (and no right columns) |
# | `"semi"` | only the left rows that **have** a match (and no right columns) |
#
# ✏️ **Exercise 6** · Under Saudi Arabia's PDPL, a customer can ask for their data to be erased. When the app erases a
# customer it deletes their row from `customers`, but their orders stay, now pointing at a `customer_id` that no longer
# exists: **orphan orders**. Find them.
# - **Result:** `orphans`, the rows of `orders` whose `customer_id` is not in `customers` (columns of `orders` only).
# - **Check yourself:** about a thousand orders.

# %%
# Template:
# orphans = orders.join(customers, on="___", how="___")
# BEGIN SOLUTION
orphans = orders.join(customers, on="customer_id", how="anti")
# END SOLUTION
print(orphans.height, "orphan orders from", orphans["customer_id"].n_unique(), "erased customers")

# %%
U.check(orphans.height == 1040, "1,040 orders point at an erased customer")
U.check(orphans.columns == orders.columns, "an anti join keeps only the left table's columns")
U.check(not orphans["customer_id"].is_in(customers["customer_id"].to_list()).any(), "none of their customers exist")

# %% [markdown]
# 🔍 An inner join of orders with customers would **silently** drop these 1,040 orders, and the company's revenue would
# shrink by a couple of percent in every report built on that join. Anti joins are how you find such gaps before your
# users do.
#
# Joins chain like everything else. Revenue per store: lines → their order (for status and store) → their store (for
# the city), then aggregate:

# %%
store_revenue = (
    lines.join(orders.select("order_id", "store_id", "status"), on="order_id")
         .filter(pl.col("status") == "delivered")
         .join(stores.select("store_id", "city"), on="store_id")
         .group_by("city", "store_id")
         .agg(pl.col("line_total").sum().round(0).alias("revenue"))
         .sort("city", "store_id")
)
store_revenue

# %% [markdown]
# ## 9 · `over`: group answers without collapsing rows ➕
#
# `group_by` gives one row per group. Sometimes you want the group's answer **next to each row** instead: "what share of
# its city's revenue does this store make?" That's `.over(...)`, Polars' window function: `pl.col("revenue").sum()
# .over("city")` is SQL's `sum(revenue) OVER (PARTITION BY city)` (`00_sql_windows`, Task 1).
#
# 🤔 **Predict** · How many rows will this return: 3 (one per city) or 12?

# %%
store_revenue.with_columns(pl.col("revenue").sum().over("city").alias("city_revenue"))

# %% [markdown]
# 🔍 Twelve. The sum is computed per city, then written on every store of that city.
#
# ✏️ **Exercise 7** · For each store, compute `share`: its revenue divided by its city's total revenue.
# - **Result:** `shares`, `store_revenue` plus a column `share`, rounded to 3 decimals.
# - **Check yourself:** the shares of each city add up to 1; Dammam's two stores split it about 55/45.

# %%
# Template:
# shares = store_revenue.with_columns(
#     (pl.col("revenue") / pl.col("revenue").sum().over("___")).round(3).alias("share")
# )
# BEGIN SOLUTION
shares = store_revenue.with_columns(
    (pl.col("revenue") / pl.col("revenue").sum().over("city")).round(3).alias("share")
)
# END SOLUTION
shares

# %%
U.check(shares.height == 12, "one row per store: over() collapses nothing")
U.check(shares.group_by("city").agg(pl.col("share").sum())["share"].round(2).eq(1.0).all(), "shares add up to 1 in each city")
U.check(shares.filter(pl.col("store_id") == "RUH-01")["share"].item() > 0.3, "Olaya (RUH-01) is Riyadh's big store")

# %% [markdown]
# ## 10 · Lazy mode: describe the query, let Polars plan it
#
# So far every method ran at once: that's **eager** mode, best for exploring. Polars also has a **lazy** mode: you
# describe the whole query, and nothing is read or computed until `.collect()`. In between, Polars **optimizes** the
# query, the way a database does with SQL.
#
# `pl.scan_ndjson` is the lazy twin of `pl.read_ndjson` (there's a `scan_` for CSV, Parquet, Iceberg...). §1's Polars
# query was lazy. Here it is again on one copy of the courier feed, and this time we look at what Polars did with it:

# %%
events = pl.scan_ndjson(os.path.relpath(FEED))   # a relative path keeps the plan short
query = (
    events.filter(pl.col("event_type") == "delivered")
          .select("city", "courier_id")
          .group_by("city")
          .agg(pl.col("courier_id").n_unique().alias("couriers"))
          .sort("city")
)
type(query).__name__

# %% [markdown]
# 🤔 **Predict** · `query` is a `LazyFrame`. Has the file been read yet?
#
# 🔍 No. A LazyFrame is a **plan**. `.explain()` prints the plan Polars will actually run, after optimizing it.
# Read it from the bottom up: the bottom is the file scan.

# %%
print(query.explain())

# %% [markdown]
# Two things happened to the scan at the bottom:
# - **projection pushdown**: `PROJECT 3/10 COLUMNS`. The file has 10 fields per event; Polars only parses the 3 the query
#   uses (`event_type` for the filter, `city` and `courier_id` after it).
# - **predicate pushdown**: `SELECTION: ...` The filter moved **into** the scan, so non-delivered events are dropped as
#   the file is read, never stored in memory.
#
# You wrote the steps in the order that was clear to you; the optimizer reordered them for speed. On the 35 MB of this
# course the difference is small. On files of many gigabytes it's the difference between seconds and out-of-memory.
# Now run it:

# %%
query.collect()

# %% [markdown]
# On **Parquet** the same two pushdowns go further. A JSON line must still be read and split to find its 3 fields;
# Parquet stores each column apart, so the other 7 are never read from disk. And Parquet keeps min/max statistics per
# block of rows (a row group), so the filter can skip whole blocks without reading them (Lab 1.2 measured this with
# DuckDB).
#
# 🤔 **Predict** · We save the feed as Parquet and run the same query on it with `pl.scan_parquet`. What changes in
# the plan?

# %%
events_parquet = U.lab_dir("tools_polars") / "delivery_events.parquet"
events.collect().write_parquet(events_parquet)
query_parquet = (
    pl.scan_parquet(os.path.relpath(events_parquet))
      .filter(pl.col("event_type") == "delivered")
      .select("city", "courier_id")
      .group_by("city")
      .agg(pl.col("courier_id").n_unique().alias("couriers"))
      .sort("city")
)
print(query_parquet.explain())
U.check(query_parquet.collect().equals(query.collect()), "same answer from the Parquet copy")

# %% [markdown]
# 🔍 Only the scan line changed (`Parquet SCAN`); the plan is the same: projection and selection are pushed into the
# scan whatever the format. What differs is what the scan can then **avoid reading**. DuckDB does the same for SQL
# (`EXPLAIN` in `tools/duckdb` §8): two engines, one idea.

# %% [markdown]
# A DataFrame you already have becomes lazy with `.lazy()`, and a lazy query becomes a DataFrame with `.collect()`. A
# good habit: **eager to explore** a sample, **lazy for the pipeline** you'll run again and again. Start from a
# `scan_` when you can: `pl.read_ndjson(...).lazy()` optimizes what comes after, but the file was already read whole.
#
# ✏️ **Exercise 8** · With a lazy query on `events`, count the **delivered** events per city.
# - **Result:** `delivered_by_city`, a DataFrame (collected), 3 rows, columns `city`, `delivered`, sorted by `city`.
# - **Check yourself:** Riyadh has the most, then Jeddah, then Dammam; about 5,300 in total.

# %%
# Template:
# delivered_by_city = (
#     events.filter(pl.col("___") == "___")
#           .group_by("___")
#           .agg(pl.len().alias("delivered"))
#           .sort("city")
#           .___()
# )
# BEGIN SOLUTION
delivered_by_city = (
    events.filter(pl.col("event_type") == "delivered")
          .group_by("city")
          .agg(pl.len().alias("delivered"))
          .sort("city")
          .collect()
)
# END SOLUTION
delivered_by_city

# %%
eager = pl.read_ndjson(U.RAW / "events" / "delivery_events.jsonl")
expected_counts = eager.filter(pl.col("event_type") == "delivered").group_by("city").agg(pl.len().alias("delivered")).sort("city")
U.check(isinstance(delivered_by_city, pl.DataFrame), "collected into a DataFrame")
U.check(delivered_by_city.equals(expected_counts), "same answer as the eager version: lazy changes how, not what")

# %% [markdown]
# Those counts are a little too high, by the way: the courier feed contains **redelivered** copies of some events. Lab
# 2.1 removes them, with the same lazy chain plus one `unique` step.
#
# ## 11 · Put it together: one question, one chain ➕
#
# *What is the average delivered basket per city, in September?* Everything you need is above: a filter, a join, a
# derived column, an aggregation. This time the template only gives you the steps.
#
# ✏️ **Exercise 9** · Build `basket_by_city` as **one lazy chain** starting from `orders.lazy()`:
# 1. keep the orders that were `delivered` and created in September 2026 (`pl.col("created_at").dt.month() == 9`);
# 2. join `lines.lazy()` (from Exercise 2) to get each order's `line_total`s;
# 3. join `stores.lazy()` to get the `city`;
# 4. group by `city`: `revenue` = sum of `line_total`, `orders` = number of distinct `order_id`s;
# 5. add `avg_basket` = `revenue / orders`, rounded to 2 decimals; round `revenue` to 2 decimals too;
# 6. sort by `city` and collect.
# - **Result:** `basket_by_city`, 3 rows, columns `city`, `revenue`, `orders`, `avg_basket`.
# - **Check yourself:** the average basket is about 160 SAR in every city; the check below recomputes it in SQL with DuckDB.

# %%
# Template:
# basket_by_city = (
#     orders.lazy()
#           .filter(___, ___)
#           .join(lines.lazy(), on="___")
#           .join(stores.lazy().select("store_id", "city"), on="___")
#           .group_by("___")
#           .agg(pl.col("line_total").sum().alias("revenue"),
#                pl.col("order_id").___().alias("orders"))
#           .with_columns(___)
#           .sort("city")
#           .collect()
# )
# BEGIN SOLUTION
basket_by_city = (
    orders.lazy()
          .filter(pl.col("status") == "delivered", pl.col("created_at").dt.month() == 9)
          .join(lines.lazy(), on="order_id")
          .join(stores.lazy().select("store_id", "city"), on="store_id")
          .group_by("city")
          .agg(pl.col("line_total").sum().alias("revenue"),
               pl.col("order_id").n_unique().alias("orders"))
          .with_columns((pl.col("revenue") / pl.col("orders")).round(2).alias("avg_basket"),
                        pl.col("revenue").round(2))
          .sort("city")
          .collect()
)
# END SOLUTION
basket_by_city

# %% [markdown]
# The check runs the same question in **SQL**, with DuckDB, straight on the app database. Two engines, two languages,
# one answer: you'll do this comparison yourself in Lab 2.1.

# %%
import duckdb

con = duckdb.connect()
U.attach_app(con)
sql_answer = con.sql("""
    SELECT s.city,
           round(sum(i.quantity * i.unit_price - i.discount), 2)                          AS revenue,
           count(DISTINCT o.order_id)                                                     AS orders,
           round(sum(i.quantity * i.unit_price - i.discount) / count(DISTINCT o.order_id), 2) AS avg_basket
    FROM app.orders o
    JOIN app.order_items i USING (order_id)
    JOIN app.stores s USING (store_id)
    WHERE o.status = 'delivered'
      AND month(timezone('Asia/Riyadh', o.created_at::TIMESTAMP AT TIME ZONE 'UTC')) = 9
    GROUP BY s.city ORDER BY s.city
""").pl()
U.check(basket_by_city.columns == ["city", "revenue", "orders", "avg_basket"], "columns as asked")
U.check(basket_by_city["orders"].to_list() == sql_answer["orders"].to_list(), "same orders per city as SQL")
U.check((basket_by_city["avg_basket"] - sql_answer["avg_basket"]).abs().max() < 0.02, "same average basket as SQL")

# %% [markdown]
# 🔍 Two details of this question are easy to get wrong, and the check would catch both:
# - **September in which time zone?** An order placed at 01:30 on 1 September in Riyadh is 22:30 on 31 August in UTC.
#   Because `created_at` was converted to Riyadh time in §6, `.dt.month()` uses the customer's calendar.
# - **One row per what?** After the join with `lines` there is one row per line, so `pl.len()` would count lines, not
#   orders. `n_unique` on `order_id` counts each order once.
#
# ## 12 · Back to the map
#
# Polars **transforms** bronze into silver (Lab 2.1) and **tests** DataFrames on the way (Pandera, Lab 2.4), from
# Python.

# %%
V.platform_map(tool="polars")

# %% [markdown]
# | | |
# |---|---|
# | **It does** | DataFrame transformations in Python, fast, on every core · lazy queries with a plan you can read · files in place (CSV, JSON lines, Parquet, Iceberg, databases) · larger-than-memory queries with the streaming engine (`.collect(engine="streaming")`, `sink_parquet`) |
# | **It doesn't** | store tables or run SQL for other users (that's a database: DuckDB, a warehouse) · scale past one machine (Spark, Day 3) · decide your cleaning rules · schedule, ingest or test by itself (Dagster, dlt, Pandera) |
# | **In this course** | bronze → silver cleaning (Lab 2.1) · Pandera checks on DataFrames (Lab 2.4) · reading an Iceberg table (`pl.scan_iceberg`, Lab 3.1) |
# | **Managed equivalents** | none as such: Polars runs inside your job. The same DataFrame code at cluster scale is Spark's DataFrame API (Databricks, EMR, Dataproc); Polars Cloud runs Polars queries remotely |
# | **Neighbours** | DuckDB (the same engine ideas behind SQL; you can pass DataFrames between them) · pandas (smaller data, and the libraries that expect it; watch peak memory) · Spark (distributed, with a cluster's cost) |
#
# **Which engine?** It follows the workload, not a gigabyte threshold. Lab 2.1 runs one question in Polars and in
# DuckDB and gets the same answer; on one machine the choice is mostly about the language your team writes. A
# larger dataset doesn't by itself need a cluster: measure memory, disk spill and the plan first (Lab 2.1's stretch
# runs a big join under a 100 MB memory limit).
#
# ## Cheat sheet: SQL ↔ Polars
#
# | SQL | Polars |
# |---|---|
# | `SELECT a, b * 2 AS c` | `df.select("a", (pl.col("b") * 2).alias("c"))` |
# | `SELECT *, b * 2 AS c` | `df.with_columns((pl.col("b") * 2).alias("c"))` |
# | `WHERE x = 1 AND y > 2` | `df.filter((pl.col("x") == 1) & (pl.col("y") > 2))` |
# | `CASE WHEN x = 0 THEN 'a' ELSE 'b' END` | `pl.when(pl.col("x") == 0).then(pl.lit("a")).otherwise(pl.lit("b"))` |
# | `GROUP BY k` + `count(*)`, `sum(v)` | `df.group_by("k").agg(pl.len(), pl.col("v").sum())` |
# | `count(DISTINCT v)` | `pl.col("v").n_unique()` |
# | `ORDER BY v DESC` | `df.sort("v", descending=True)` |
# | `JOIN t USING (k)` / `LEFT JOIN` | `df.join(t, on="k")` / `df.join(t, on="k", how="left")` |
# | `WHERE k NOT IN (SELECT k FROM t)` | `df.join(t, on="k", how="anti")` |
# | `sum(v) OVER (PARTITION BY k)` | `pl.col("v").sum().over("k")` |
# | `EXPLAIN` | `lazy_frame.explain()` |
#
# ## Checkpoint
#
# 1. The loop in §1 was correct. Why does it get slower, and harder to change, as the data and the questions grow?
# 2. What is the difference between `pl.col("fee") * 2` and `orders["fee"] * 2`?
# 3. Name the four contexts. Which one keeps every column, which one changes the number of rows?
# 4. Why does `with_columns(a_new_column, something_using_a_new_column)` fail, and what's the fix?
# 5. Which join finds the rows of one table that have no match in another?
# 6. What does `.explain()` show that `.collect()` doesn't, and what are projection and predicate pushdown?
#
# **Next: [Lab 2.1 · Bronze to silver](../D2_1_bronze_to_silver.ipynb).** It parses three timestamp formats with the
# `.str`/`.dt` namespaces, keeps one copy per event with `unique`, and writes silver Parquet with `sink_parquet`.
# After it, Lab 2.4 validates DataFrames with Pandera and Lab 3.1 reads an Iceberg table with `pl.scan_iceberg`.
# The [Polars user guide](https://docs.pola.rs/user-guide/) goes deeper on every section of this walkthrough.
