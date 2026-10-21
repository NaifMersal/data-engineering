# %% [markdown]
# # Lab 0 · Polars, one idea at a time
#
# Before Day 1 · self-paced · about 90 min. Labs 1.2, 2.1, 2.4, 3.1 and the capstone use **Polars**, a fast DataFrame
# library, and Lab 2.1 expects you to chain a filter, a join, a parse and an aggregation in one go. This lab builds that
# up one idea at a time, on Qafila's own data: first a 12-row table you can check by eye, then the 45,766 orders.
#
# Every section follows the same rhythm: **🤔 predict** what a cell returns, run it, then read the explanation.
# Write your prediction down before you run the cell: that's where the learning happens.
#
# **How the exercises work.** Each ✏️ exercise asks for **one** result and tells you what a correct one looks like.
# Its code cell starts with a template in comments:
# 1. select the template lines and uncomment them (`Ctrl+/`, or `Cmd+/` on a Mac);
# 2. replace every `___` with the right column, method or value;
# 3. delete the two lines below the template (`# ✏️ YOUR CODE HERE` and `raise NotImplementedError(...)`);
# 4. run the cell, then the check cell after it: each ✅ means that part is right.
#
# **Tasks**
# - [ ] 1 · A DataFrame is a table of typed columns (10')
# - [ ] 2 · Expressions and `select` (10')
# - [ ] 3 · `filter`: keep the rows you want (10')
# - [ ] 4 · `with_columns`: add and fix columns (15')
# - [ ] 5 · `group_by` + `agg`: one row per group (10')
# - [ ] 6 · Joins: combine tables (10')
# - [ ] 7 · `over`: group answers without collapsing rows (5')
# - [ ] 8 · Lazy mode: describe the query, let Polars plan it (10')
# - [ ] 9 · Put it together: one question, one chain (10')
#
# Know pandas? Polars looks similar but thinks differently: there is no index, nothing is changed in place (every
# method returns a new DataFrame), and you describe columns with **expressions** instead of indexing with `df[...]`.
# Task 2 is the one to read slowly.

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
        os.chdir("..")  # solutions/ notebooks run from labs/
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
# ## 1 · A DataFrame is a table of typed columns (10')
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
# data arrives like this; turning text into real types is a big part of cleaning it (Task 4).
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
# ## 2 · Expressions and `select` (10')
#
# This is the one idea the rest of Polars is built on. `pl.col("delivery_fee")` does **not** fetch a column. It is an
# **expression**: a recipe that says "take the column called `delivery_fee`". You can extend a recipe with operators
# and methods, and nothing is computed yet:

# %%
fee_with_vat = pl.col("delivery_fee") * 1.15
print(fee_with_vat)

# %% [markdown]
# An expression is computed only when you hand it to a DataFrame method that runs it, a **context**. There are four,
# one per task of this lab:
#
# | context | what it does with your expressions | rows out |
# |---|---|---|
# | `df.select(...)` | computes them; keeps **only** them | same, or 1 if they aggregate |
# | `df.with_columns(...)` | computes them; adds them to (or replaces) the existing columns | same |
# | `df.filter(...)` | keeps the rows where the expression is `true` | fewer |
# | `df.group_by(...).agg(...)` | computes them once per group | one per group |
#
# Because the recipe is separate from the data, the same expression works on any DataFrame that has those columns, and
# Polars can look at all your recipes before running them, to run them in parallel or skip work (Task 8).
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
# ✏️ **Exercise 2** · Compute the amount of every order line: `quantity * unit_price - discount`, rounded to 2 decimals.
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
# ## 3 · `filter`: keep the rows you want (10')
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
# ## 4 · `with_columns`: add and fix columns (15')
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
# ## 5 · `group_by` + `agg`: one row per group (10')
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
# ## 6 · Joins: combine tables (10')
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
# ## 7 · `over`: group answers without collapsing rows (5')
#
# `group_by` gives one row per group. Sometimes you want the group's answer **next to each row** instead: "what share of
# its city's revenue does this store make?" That's `.over(...)`, Polars' window function: `pl.col("revenue").sum()
# .over("city")` is SQL's `sum(revenue) OVER (PARTITION BY city)` (Lab 0 · SQL windows, Task 1).
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
# ## 8 · Lazy mode: describe the query, let Polars plan it (10')
#
# So far every method ran at once: that's **eager** mode, best for exploring. Polars also has a **lazy** mode: you
# describe the whole query, and nothing is read or computed until `.collect()`. In between, Polars **optimizes** the
# query, the way a database does with SQL.
#
# `pl.scan_ndjson` is the lazy twin of `pl.read_ndjson` (there's a `scan_` for CSV, Parquet, Iceberg...). Here is the
# courier delivery-events feed:

# %%
events = pl.scan_ndjson(U.RAW / "events" / "delivery_events.jsonl")
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
# course the difference is small. On files of many gigabytes, or Parquet files where unused columns are never even read
# from disk (Lab 1.2), it's the difference between seconds and out-of-memory. Now run it:

# %%
query.collect()

# %% [markdown]
# A DataFrame you already have becomes lazy with `.lazy()`, and a lazy query becomes a DataFrame with `.collect()`. A
# good habit: **eager to explore** a sample, **lazy for the pipeline** you'll run again and again.
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
# ## 9 · Put it together: one question, one chain (10')
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
#   Because `created_at` was converted to Riyadh time in Task 4, `.dt.month()` uses the customer's calendar.
# - **One row per what?** After the join with `lines` there is one row per line, so `pl.len()` would count lines, not
#   orders. `n_unique` on `order_id` counts each order once.
#
# ## Checkpoint
#
# 1. What is the difference between `pl.col("fee") * 2` and `orders["fee"] * 2`?
# 2. Name the four contexts. Which one keeps every column, which one changes the number of rows?
# 3. Why does `with_columns(a_new_column, something_using_a_new_column)` fail, and what's the fix?
# 4. Which join finds the rows of one table that have no match in another?
# 5. What does `.explain()` show that `.collect()` doesn't, and what are projection and predicate pushdown?
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
# **Where you'll meet Polars next:** Lab 1.2 scans Parquet lazily and watches pushdown skip data; Lab 2.1 parses three
# timestamp formats with `pl.when` and the `.str`/`.dt` namespaces, deduplicates with `unique`, and writes Parquet with
# `sink_parquet`; Lab 2.4 validates DataFrames with Pandera; Lab 3.1 reads an Iceberg table with `pl.scan_iceberg`.
# The [Polars user guide](https://docs.pola.rs/user-guide/) goes deeper on every section of this lab.
