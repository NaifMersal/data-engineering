# %% [markdown]
# # Lab 0 · SQL window functions, from first principles
#
# Before Day 1 · self-paced · about 90 min. Labs 1.4, 2.1, 3.4 and the capstone lean on window functions: deduplicate a
# feed, keep the latest event per key, close a validity interval with the next change. This lab builds them up from
# nothing, on tables small enough to check by eye, then has you derive the CDC and SCD2 queries yourself.
#
# Every section follows the same rhythm: **🤔 predict** what a query returns, run it, then read the explanation.
# Write your prediction down before you run the cell: that's where the learning happens.
#
# **How the exercises work.** Each ✏️ exercise asks for **one** result and tells you what a correct one looks like.
# Its code cell starts with a template in comments:
# 1. select the template lines and uncomment them (`Ctrl+/`, or `Cmd+/` on a Mac);
# 2. replace every `___` with the right column, function or value;
# 3. delete the two lines below the template (`# ✏️ YOUR CODE HERE` and `raise NotImplementedError(...)`);
# 4. run the cell, then the check cell after it: each ✅ means that part is right.
#
# **Tasks**
# - [ ] 1 · `GROUP BY` collapses rows, `OVER` keeps them (10')
# - [ ] 2 · `PARTITION BY` and `ORDER BY`: each row's window (10')
# - [ ] 3 · Ranking: `ROW_NUMBER`, `RANK`, ties, deduplication (10')
# - [ ] 4 · `LAG` and `LEAD`: looking at the neighbours (10')
# - [ ] 5 · Frames: "everything before me" (10')
# - [ ] 6 · Evaluation order: why `WHERE` can't, and `QUALIFY` can (10')
# - [ ] 7 · Derive SCD2 by hand, then in SQL; break it on purpose (15')
# - [ ] 8 · The same ideas on real data: courier events (15')
#
# A **visual summary** at the end draws every idea of the lab from the tables you built.

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
import de_viz as V
U.ensure_data()
print("data:", U.RAW, "| quick mode:", U.QUICK)

# %%
import duckdb
import pandas as pd

con = duckdb.connect()
con.sql("SET enable_progress_bar = false")
con.sql("SET TimeZone = 'UTC'")   # the sources' timestamps are UTC: read and cast them the same on any machine
RAW = U.RAW.as_posix()

# %% [markdown]
# Two tiny tables. `orders`: eight Qafila orders from three customers.

# %%
con.sql("""
    CREATE TABLE orders AS
    SELECT * FROM (VALUES
        (1, 101, DATE '2026-09-01', 40), (2, 102, DATE '2026-09-01', 25), (3, 101, DATE '2026-09-03', 60),
        (4, 103, DATE '2026-09-04', 15), (5, 102, DATE '2026-09-06', 35), (6, 101, DATE '2026-09-07', 20),
        (7, 103, DATE '2026-09-07', 15), (8, 102, DATE '2026-09-09', 50)
    ) t(order_id, customer_id, created_at, amount)
""")
con.sql("FROM orders").df()

# %% [markdown]
# And `changes`: a change feed for the same customers, in the order it **arrived**, like the Debezium feed of Lab 1.4.
# `lsn` is the position in the database's log (the true order of the changes); `op` is `c`reate, `u`pdate or `d`elete.
# A delete carries no city: the row is gone.

# %%
con.sql("""
    CREATE TABLE changes AS
    SELECT arrival, lsn, customer_id, op, city, changed_at::TIMESTAMP AS changed_at FROM (VALUES
        (1, 10, 101, 'c', 'Riyadh', '2026-09-01 09:00'),
        (2, 12, 102, 'c', 'Jeddah', '2026-09-02 10:00'),
        (3, 15, 101, 'u', 'Dammam', '2026-09-05 12:00'),
        (4, 17, 102, 'u', 'Riyadh', '2026-09-07 16:00'),
        (5, 22, 101, 'u', 'Jeddah', '2026-09-12 08:00'),
        (6, 22, 101, 'u', 'Jeddah', '2026-09-12 08:00'),
        (7, 18, 101, 'u', 'Riyadh', '2026-09-08 13:00'),
        (8, 25, 103, 'c', 'Dammam', '2026-09-14 11:00'),
        (9, 30, 103, 'd', NULL,     '2026-09-20 15:00')
    ) t(arrival, lsn, customer_id, op, city, changed_at)
""")
con.sql("FROM changes").df()

# %% [markdown]
# Look closely before going on: the event at arrival 6 is a **redelivery** of arrival 5 (same `lsn`), and the event at
# arrival 7 is **late** (`lsn` 18 arrived after `lsn` 22). Both are normal in CDC, and both will bite us.
#
# ## 1 · `GROUP BY` collapses rows, `OVER` keeps them (10')
#
# You know aggregates: `GROUP BY customer_id` turns each group of rows into **one** row.

# %%
con.sql("SELECT customer_id, sum(amount) AS total FROM orders GROUP BY customer_id ORDER BY customer_id").df()

# %% [markdown]
# 🤔 **Predict** · The next query uses the same `sum(amount)`, followed by `OVER (PARTITION BY customer_id)`. How many
# rows will it return: 3 or 8? What will the `total` column hold on order 3?

# %%
con.sql("""
    SELECT order_id, customer_id, amount, sum(amount) OVER (PARTITION BY customer_id) AS total
    FROM orders ORDER BY customer_id, order_id
""").df()

# %% [markdown]
# 🔍 Eight rows. A **window function** computes an aggregate over a set of rows related to the current row (its
# **window**) and writes the answer **next to** the row instead of replacing the group with it. `PARTITION BY
# customer_id` says "the window of a row is every row with the same customer". Nothing is physically moved or sorted
# in the table: the engine just knows which rows belong together.
#
# That's what makes windows useful: you can compare a row to its group without a self-join.
#
# ▶ **Step through it.** `V.animate_window` (a helper of this course) takes a table and one window function, written
# exactly as in a `SELECT`, runs it in DuckDB and plays it back: the engine splits the rows into windows, then visits
# each row, and the highlighted rows are the ones `sum` reads for that row. Press **Play**, or click **Next** to go one
# step at a time. (If the buttons do nothing in Jupyter, run the cell again: notebooks only run scripts in cells you
# executed yourself.)

# %%
V.animate_window(con, "orders",
    "sum(amount) OVER (PARTITION BY customer_id) AS total",
    show=["order_id", "customer_id", "amount"],
    step_text="adds up the amounts of every row in its window: {rows}")

# %% [markdown]
# ✏️ **Exercise 1** · For every order, compute `share`: the order's amount divided by its customer's total amount.
# - **Result:** `shares`, 8 rows, columns `order_id`, `customer_id`, `amount`, `share` (rounded to 2 decimals).
# - **Check yourself:** customer 101 spent 120 SAR in total, so order 3 (60 SAR) has `share` = 0.5.

# %%
# Template:
# shares = con.sql("""
#     SELECT order_id, customer_id, amount,
#            round(amount / ___(amount) OVER (PARTITION BY ___), 2) AS share
#     FROM orders
#     ORDER BY order_id
# """).df()
# BEGIN SOLUTION
shares = con.sql("""
    SELECT order_id, customer_id, amount,
           round(amount / sum(amount) OVER (PARTITION BY customer_id), 2) AS share
    FROM orders
    ORDER BY order_id
""").df()
# END SOLUTION
shares

# %%
U.check(len(shares) == 8, "one row per order: the window kept them all")
U.check(shares.loc[shares.order_id == 3, "share"].item() == 0.5, "order 3 is half of customer 101's 120 SAR")
U.check(shares.groupby("customer_id").share.sum().round(1).eq(1.0).all(), "shares add up to 1 per customer")

# %% [markdown]
# ## 2 · `PARTITION BY` and `ORDER BY`: each row's window (10')
#
# `OVER ()` with nothing inside: the window is the whole table. `PARTITION BY` cuts it into groups.
# Now add `ORDER BY` inside the window.
#
# 🤔 **Predict** · Below, the only difference between `total` and `with_order` is `ORDER BY created_at` inside `OVER`.
# Will `with_order` be the same as `total`? If not, what will it be for customer 101?

# %%
con.sql("""
    SELECT order_id, customer_id, created_at, amount,
           sum(amount) OVER ()                                              AS everything,
           sum(amount) OVER (PARTITION BY customer_id)                      AS total,
           sum(amount) OVER (PARTITION BY customer_id ORDER BY created_at)  AS with_order
    FROM orders ORDER BY customer_id, created_at
""").df()

# %% [markdown]
# 🔍 Adding `ORDER BY` turned the total into a **running total** (40, 100, 120 for customer 101). With an `ORDER BY`,
# the default window of a row is no longer the whole partition but "from the start of the partition **up to this
# row**". This catches everyone once; Task 5 shows how to say exactly which rows you want (the **frame**).
#
# Two different `ORDER BY`s live in that query: the one inside `OVER` defines each row's window; the one at the end only
# sorts the output. They don't have to match.
#
# ▶ **Step through it.** Same windows as before, now ordered by date. Watch the highlighted rows grow: each row reads
# its window from the start **up to itself**.

# %%
V.animate_window(con, "orders",
    "sum(amount) OVER (PARTITION BY customer_id ORDER BY created_at) AS running",
    show=["order_id", "customer_id", "created_at", "amount"],
    step_text="adds up the amounts from the start of its window up to itself: {rows}")

# %% [markdown]
# ✏️ **Exercise 2** · For every order, compute `running`: how much the customer has spent so far, up to and including
# this order (a running total per customer, in date order).
# - **Result:** `per_customer`, 8 rows, columns `order_id`, `customer_id`, `created_at`, `amount`, `running`.
# - **Check yourself:** customer 101's three orders show `running` = 40, 100, 120.

# %%
# Template:
# per_customer = con.sql("""
#     SELECT order_id, customer_id, created_at, amount,
#            sum(amount) OVER (PARTITION BY ___ ORDER BY ___) AS running
#     FROM orders
#     ORDER BY customer_id, created_at
# """).df()
# BEGIN SOLUTION
per_customer = con.sql("""
    SELECT order_id, customer_id, created_at, amount,
           sum(amount) OVER (PARTITION BY customer_id ORDER BY created_at) AS running
    FROM orders
    ORDER BY customer_id, created_at
""").df()
# END SOLUTION
per_customer

# %%
U.check(per_customer[per_customer.customer_id == 101].running.tolist() == [40, 100, 120], "running total for 101")
U.check(per_customer.groupby("customer_id").running.max().tolist() == [120, 110, 30], "each running total ends at the customer's total")

# %% [markdown]
# ## 3 · Ranking: `ROW_NUMBER`, `RANK`, ties, deduplication (10')
#
# Ranking functions number the rows of each partition in the window's order. They don't take an argument: the
# `ORDER BY` inside `OVER` is what they rank by.
#
# 🤔 **Predict** · Customer 101 has two events with `lsn` 22 (the redelivery). Ordered by `lsn DESC`, what do
# `row_number`, `rank` and `dense_rank` give those two rows, and the next one?

# %%
con.sql("""
    SELECT customer_id, lsn, arrival, op, city,
           row_number() OVER w AS row_number,
           rank()       OVER w AS rank,
           dense_rank() OVER w AS dense_rank
    FROM changes
    WINDOW w AS (PARTITION BY customer_id ORDER BY lsn DESC)
    ORDER BY customer_id, lsn DESC
""").df()

# %% [markdown]
# 🔍 `row_number` always gives 1, 2, 3...: on a **tie** it breaks it arbitrarily, so which copy gets 1 can change from run
# to run. `rank` gives ties the same number and skips (1, 1, 3); `dense_rank` doesn't skip (1, 1, 2). `WINDOW w AS (...)`
# names a window so several functions can share it.
#
# That arbitrariness is harmless here, because the two copies are identical. It is **not** harmless when the tied rows
# differ: then "the latest row" is a coin flip. Add a tie-breaker to the `ORDER BY` (`lsn DESC, arrival DESC`), or
# deduplicate first.
#
# Deduplication is itself a ranking problem: number the copies of each `lsn` and keep copy 1. A filter on a window
# result needs a subquery here (Task 6 shows a shortcut). We'll use this `dedup` view for the rest of the lab:

# %%
con.sql("""
    CREATE OR REPLACE VIEW dedup AS
    SELECT * EXCLUDE (copy) FROM (
        SELECT *, row_number() OVER (PARTITION BY lsn ORDER BY arrival) AS copy FROM changes
    ) WHERE copy = 1
""")
print(con.sql("SELECT count(*) FROM changes").fetchall()[0][0], "events ->", con.sql("SELECT count(*) FROM dedup").fetchall()[0][0], "after dedup")

# %% [markdown]
# ✏️ **Exercise 3** · Find each customer's **last** change: the row with the highest `lsn` for that customer.
# Number each customer's rows from the highest `lsn` down, then keep row number 1.
# - **Result:** `latest`, 3 rows (one per customer), columns `customer_id`, `lsn`, `op`, `city`, read from `dedup`.
# - **Check yourself:** the `lsn` values are 22, 17 and 30; customer 103's last change is a delete (`op` = `d`).

# %%
# Template:
# latest = con.sql("""
#     SELECT customer_id, lsn, op, city FROM (
#         SELECT *, row_number() OVER (PARTITION BY ___ ORDER BY ___ DESC) AS rn
#         FROM dedup
#     )
#     WHERE rn = ___
#     ORDER BY customer_id
# """).df()
# BEGIN SOLUTION
latest = con.sql("""
    SELECT customer_id, lsn, op, city FROM (
        SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY lsn DESC) AS rn
        FROM dedup
    )
    WHERE rn = 1
    ORDER BY customer_id
""").df()
# END SOLUTION
latest

# %%
U.check(latest.lsn.tolist() == [22, 17, 30], "the highest lsn per customer, not the last arrival")
U.check(dict(zip(latest.customer_id, latest.op)) == {101: "u", 102: "u", 103: "d"}, "customer 103's last event is a delete")

# %% [markdown]
# ▶ **Watch it run.** `row_number` on the raw `changes` (with the redelivery still in): each customer's rows are put
# in `lsn DESC` order (`arrival` breaks the tie), and each row's number is its position. Keeping number 1 leaves the
# latest change per customer.

# %%
V.animate_window(con, "changes",
    "row_number() OVER (PARTITION BY customer_id ORDER BY lsn DESC, arrival) AS rn",
    show=["arrival", "lsn", "customer_id", "op", "city"],
    step_text="counts the rows from the start of its window up to itself: {rows}",
    keep="rn = 1", keep_label="Keep rn = 1 (the latest change of each customer)")

# %% [markdown]
# 🔍 Customer 101's last **arrival** is `lsn` 18 (Riyadh) but the last **change** is `lsn` 22 (Jeddah). Ordering by
# the log position, not by arrival, is what makes the result right.
#
# ## 4 · `LAG` and `LEAD`: looking at the neighbours (10')
#
# `lag(x)` reads `x` from the **previous** row of the window, `lead(x)` from the **next** one. At the edges of a
# partition there is no neighbour, so you get `NULL` (or a default: `lag(x, 1, 'none')`).
#
# 🤔 **Predict** · For customer 102 (two events), what are `prev_city` and `next_lsn` on each row?

# %%
con.sql("""
    SELECT customer_id, lsn, op, city,
           lag(city) OVER w  AS prev_city,
           lead(lsn) OVER w  AS next_lsn
    FROM dedup
    WINDOW w AS (PARTITION BY customer_id ORDER BY lsn)
    ORDER BY customer_id, lsn
""").df()

# %% [markdown]
# 🔍 Each customer is a separate world: customer 102's first row has no `prev_city`, even though customer 101's rows
# come just before it in the output. Without `PARTITION BY`, `lag` would happily read another customer's city.
#
# Here is the key idea of SCD2 in one sentence: **a version is valid from its own change time until the next change of
# the same customer.** "The next change of the same customer" is exactly `lead(...) OVER (PARTITION BY customer_id
# ORDER BY lsn)`.
#
# ✏️ **Exercise 4** · For every change, compute `valid_to`: the `changed_at` of the **next** change of the same
# customer (in `lsn` order). The last change of each customer has no next change, so its `valid_to` is `NULL`.
# - **Result:** `intervals`, 8 rows (one per row of `dedup`), columns `customer_id`, `lsn`, `op`, `city`,
#   `valid_from`, `valid_to`.
# - **Check yourself:** customer 101's Riyadh row (`lsn` 10) has `valid_to` = 2026-09-05 12:00, the time of `lsn` 15.

# %%
# Template:
# intervals = con.sql("""
#     SELECT customer_id, lsn, op, city,
#            changed_at                                                     AS valid_from,
#            lead(___) OVER (PARTITION BY customer_id ORDER BY ___)  AS valid_to
#     FROM dedup
#     ORDER BY customer_id, lsn
# """).df()
# BEGIN SOLUTION
intervals = con.sql("""
    SELECT customer_id, lsn, op, city,
           changed_at                                                     AS valid_from,
           lead(changed_at) OVER (PARTITION BY customer_id ORDER BY lsn)  AS valid_to
    FROM dedup
    ORDER BY customer_id, lsn
""").df()
# END SOLUTION
intervals

# %%
U.check(intervals.valid_to.isna().sum() == 3, "one open interval per customer: the last event")
U.check(intervals.loc[intervals.lsn == 10, "valid_to"].item() == pd.Timestamp("2026-09-05 12:00"), "Riyadh ends when Dammam starts")
U.check(intervals[(intervals.customer_id == 103) & (intervals.op == "c")].valid_to.notna().item(), "the delete closes 103's version")

# %% [markdown]
# ▶ **Watch it run.** `lead` reads exactly one row: the **next** one in the same window. The last row of each window
# has no next row, so it gets `NULL`.

# %%
V.animate_window(con, "dedup",
    "lead(changed_at) OVER (PARTITION BY customer_id ORDER BY lsn) AS valid_to",
    show=["arrival", "lsn", "customer_id", "op", "city", "changed_at"],
    step_text="reads changed_at from the next row of its window: {rows}")

# %% [markdown]
# Notice the last row: the delete itself has an open interval, but it isn't a version (there's no city). We'll drop it,
# and **when** we drop it matters (Task 6).
#
# ## 5 · Frames: "everything before me" (10')
#
# The **frame** says which rows of the partition an aggregate sees, relative to the current row:
# `ROWS BETWEEN <start> AND <end>`, where each end is `UNBOUNDED PRECEDING`, `n PRECEDING`, `CURRENT ROW`,
# `n FOLLOWING` or `UNBOUNDED FOLLOWING`. (Ranking functions and `lag`/`lead` ignore frames.)
#
# To **see** a frame, use `list(order_id) OVER (...)`. `list` is an aggregate like `sum`, but instead of adding the
# values up it collects them into a list. So `list(order_id)` over a frame returns the ids of exactly the rows in that
# frame, e.g. `[1, 2, 3]`: the rows `sum` or `max` would see with the same `OVER (...)`.
#
# 🤔 **Predict** · Orders 6 and 7 share a date. Which ids will each column list on order 6's row?

# %%
frames = con.sql("""
    SELECT order_id, created_at, amount,
           list(order_id) OVER (ORDER BY created_at, order_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rows_running,
           list(order_id) OVER (ORDER BY created_at)                                                            AS default_frame,
           list(order_id) OVER (ORDER BY created_at, order_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS before_me,
           list(order_id) OVER (ORDER BY created_at, order_id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)         AS neighbours
    FROM orders ORDER BY created_at, order_id
""").df()
frames

# %% [markdown]
# 🔍 The default frame with an `ORDER BY` is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, and `RANGE` includes
# the current row's **peers** (rows with the same `ORDER BY` value). So order 6's default frame already contains order 7,
# and a "running total" by date jumps by both orders at once. When you mean rows, write `ROWS` and give the `ORDER BY` a
# tie-breaker.
#
# `before_me` is the frame Lab 1.4 needs: "the largest `lsn` that arrived **before** this event". If an event's `lsn`
# is smaller than that, it arrived out of order.
#
# ✏️ **Exercise 5** · Count the events of `changes` that arrived **out of order**: their `lsn` is smaller than the
# largest `lsn` among all the events that arrived **before** them. The template computes that largest earlier `lsn` as
# `max_before`; you only fill in the frame.
# - **Result:** `n_out_of_order`, a number.
# - **Check yourself:** it's 1 (the late `lsn` 18). The redelivered `lsn` 22 isn't smaller than 22, so it doesn't count.

# %%
# Template:
# n_out_of_order = con.sql("""
#     SELECT count(*) FROM (
#         SELECT lsn, max(lsn) OVER (ORDER BY arrival ROWS BETWEEN ___ AND ___) AS max_before
#         FROM changes
#     )
#     WHERE lsn < max_before
# """).fetchall()[0][0]
# BEGIN SOLUTION
n_out_of_order = con.sql("""
    SELECT count(*) FROM (
        SELECT lsn, max(lsn) OVER (ORDER BY arrival ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS max_before
        FROM changes
    )
    WHERE lsn < max_before
""").fetchall()[0][0]
# END SOLUTION
U.check(n_out_of_order == 1, f"{n_out_of_order} late event (lsn 18); the redelivered 22 is a duplicate, not late")

# %% [markdown]
# ▶ **Watch it run.** No `PARTITION BY`: the whole table is one window, in arrival order. The frame "everything
# before me" grows by one row at each step, and `max` reads only those rows. The first row has nothing before it.

# %%
V.animate_window(con, "changes",
    "max(lsn) OVER (ORDER BY arrival ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS max_before",
    show=["arrival", "lsn", "customer_id", "op"],
    step_text="takes the largest lsn of the rows that arrived before it: {rows}",
    keep="lsn < max_before", keep_label="Keep lsn < max_before (the events that arrived out of order)")

# %% [markdown]
# ## 6 · Evaluation order: why `WHERE` can't, and `QUALIFY` can (10')
#
# Task 3 needed a subquery to keep `rn = 1`. Why not just `WHERE row_number() OVER (...) = 1`? Try it:

# %%
try:
    con.sql("SELECT * FROM dedup WHERE row_number() OVER (PARTITION BY customer_id ORDER BY lsn DESC) = 1").df()
except duckdb.Error as e:
    print(type(e).__name__, "→", str(e).splitlines()[0])

# %% [markdown]
# 🔍 A `SELECT` is evaluated, **logically**, in this order: `FROM` → `WHERE` → `GROUP BY` → `HAVING` → **window
# functions** → **`QUALIFY`** → `SELECT` / `DISTINCT` → `ORDER BY` → `LIMIT`. `WHERE` runs before any window exists, so it
# can't use one. `QUALIFY` (DuckDB, Snowflake, BigQuery, Databricks; not PostgreSQL) is a `WHERE` that runs **after** the
# windows. (The optimizer may execute things in another order, but the result is always as if it followed this one.)
#
# ✏️ **Exercise 6** · Get the same result as Exercise 3 (each customer's last change) **without a subquery**: put the
# window function straight into a `QUALIFY` clause.
# - **Result:** `latest_q`, the same 3 rows and columns as `latest`.

# %%
# Template:
# latest_q = con.sql("""
#     SELECT customer_id, lsn, op, city
#     FROM dedup
#     QUALIFY row_number() OVER (PARTITION BY ___ ORDER BY ___ DESC) = ___
#     ORDER BY customer_id
# """).df()
# BEGIN SOLUTION
latest_q = con.sql("""
    SELECT customer_id, lsn, op, city
    FROM dedup
    QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY lsn DESC) = 1
    ORDER BY customer_id
""").df()
# END SOLUTION
U.check(latest_q.equals(latest), "same result as the subquery version")

# %% [markdown]
# The order matters for more than syntax. We want versions without the delete markers. Filtering them with `WHERE` and
# with `QUALIFY` look interchangeable.
#
# 🤔 **Predict** · In which of the two queries below is customer 103 (deleted on 20 September) still **current**
# (`valid_to` is `NULL`)?

# %%
too_early = con.sql("""
    SELECT customer_id, city, changed_at AS valid_from,
           lead(changed_at) OVER (PARTITION BY customer_id ORDER BY lsn) AS valid_to
    FROM dedup WHERE op <> 'd'                       -- filter BEFORE the window
    ORDER BY customer_id, valid_from
""").df()
after = con.sql("""
    SELECT customer_id, city, changed_at AS valid_from,
           lead(changed_at) OVER (PARTITION BY customer_id ORDER BY lsn) AS valid_to
    FROM dedup QUALIFY op <> 'd'                     -- filter AFTER the window
    ORDER BY customer_id, valid_from
""").df()
pd.concat({"WHERE (too early)": too_early[too_early.customer_id == 103],
           "QUALIFY (after)": after[after.customer_id == 103]})

# %% [markdown]
# 🔍 With `WHERE`, the delete is gone before `lead` runs, so `lead` finds no next row and customer 103 looks like they
# still live in Dammam today. With `QUALIFY`, `lead` sees the delete, closes the version on 20 September, and only then
# is the marker removed. Same filter, different place in the evaluation order, different history.
#
# ## 7 · Derive SCD2 by hand, then in SQL; break it on purpose (15')
#
# Time to put it together, the way Lab 1.4 will need it. First by hand, from the deduplicated changes in `lsn` order:

# %%
con.sql("SELECT customer_id, lsn, op, city, changed_at FROM dedup ORDER BY customer_id, lsn").df()

# %% [markdown]
# ✏️ **Exercise 7a (by hand, no SQL)** · Below is the full history: one row per **version** (every change that isn't
# a delete), with its city and its start time. Fill in each `valid_to`, the time the version **ends**, using the table
# above. Two rules:
# - a version ends when the **next** change of the same customer starts (whatever that change is, even a delete);
# - if the customer has no next change, the version is still valid: write `None`.
#
# - **Result:** `expected`, a list of 7 tuples `(customer_id, city, valid_from, valid_to)`.
# - **Check yourself:** exactly two versions end in `None` (customer 103 was deleted, so theirs does not).

# %%
# Template: replace each ___ with a time in quotes, like "2026-09-05 12:00", or with None (no quotes).
# expected = [
#     (101, "Riyadh", "2026-09-01 09:00", ___),
#     (101, "Dammam", "2026-09-05 12:00", ___),
#     (101, "Riyadh", "2026-09-08 13:00", ___),
#     (101, "Jeddah", "2026-09-12 08:00", ___),
#     (102, "Jeddah", "2026-09-02 10:00", ___),
#     (102, "Riyadh", "2026-09-07 16:00", ___),
#     (103, "Dammam", "2026-09-14 11:00", ___),
# ]
# BEGIN SOLUTION
expected = [
    (101, "Riyadh", "2026-09-01 09:00", "2026-09-05 12:00"),
    (101, "Dammam", "2026-09-05 12:00", "2026-09-08 13:00"),
    (101, "Riyadh", "2026-09-08 13:00", "2026-09-12 08:00"),
    (101, "Jeddah", "2026-09-12 08:00", None),
    (102, "Jeddah", "2026-09-02 10:00", "2026-09-07 16:00"),
    (102, "Riyadh", "2026-09-07 16:00", None),
    (103, "Dammam", "2026-09-14 11:00", "2026-09-20 15:00"),
]
# END SOLUTION
U.check(len(expected) == 7 and sum(v is None for *_, v in expected) == 2, "7 versions, 2 still current")

# %% [markdown]
# ✏️ **Exercise 7b (now in SQL)** · Build the same history from `dedup` with one query. `valid_to` comes from `lead`
# (Task 4). The delete markers (`op = 'd'`) must be removed **after** the window, so that the delete still closes
# customer 103's version: choose the clause that does that (Task 6).
# - **Result:** `history`, 7 rows, columns `customer_id`, `city`, `valid_from`, `valid_to`, `is_current`.
# - **Check yourself:** it matches your `expected` list from 7a, and `is_current` is true on exactly 2 rows.

# %%
# Template:
# history = con.sql("""
#     SELECT customer_id, city,
#            changed_at                  AS valid_from,
#            lead(changed_at) OVER w         AS valid_to,
#            lead(changed_at) OVER w IS NULL AS is_current
#     FROM dedup
#     WINDOW w AS (PARTITION BY ___ ORDER BY ___)
#     ___ op <> 'd'                -- WHERE or QUALIFY? (Task 6)
#     ORDER BY customer_id, valid_from
# """).df()
# BEGIN SOLUTION
history = con.sql("""
    SELECT customer_id, city,
           changed_at                  AS valid_from,
           lead(changed_at) OVER w         AS valid_to,
           lead(changed_at) OVER w IS NULL AS is_current
    FROM dedup
    WINDOW w AS (PARTITION BY customer_id ORDER BY lsn)
    QUALIFY op <> 'd'
    ORDER BY customer_id, valid_from
""").df()
# END SOLUTION
history

# %%
def as_tuples(df):
    """The history as a list of (customer_id, city, valid_from, valid_to) tuples, to compare with `expected`."""
    fmt = lambda t: None if pd.isna(t) else t.strftime("%Y-%m-%d %H:%M")
    return [(r.customer_id, r.city, fmt(r.valid_from), fmt(r.valid_to)) for r in df.itertuples()]

U.check(as_tuples(history) == expected, "SQL matches your hand-derived history")
U.check(history.is_current.sum() == 2, "two current versions: customer 103 was deleted")

# %% [markdown]
# Now break it. To change one thing at a time, here is the same query as a function. Its three arguments are the three
# things the experiments change: the input table, the window's `ORDER BY`, and whether to deduplicate first.

# %%
def history_variant(source="changes", order_by="lsn", dedup=True):
    rows = f"(FROM {source} QUALIFY row_number() OVER (PARTITION BY lsn ORDER BY arrival) = 1)" if dedup else source
    return con.sql(f"""
        SELECT customer_id, city, changed_at AS valid_from, lead(changed_at) OVER w AS valid_to
        FROM {rows}
        WINDOW w AS (PARTITION BY customer_id ORDER BY {order_by})
        QUALIFY op <> 'd'
        ORDER BY customer_id, valid_from
    """).df()

U.check(as_tuples(history_variant()) == expected, "with the defaults, it gives your history")

# %% [markdown]
# Each experiment changes one thing. 🤔 **Predict** first, then run.
#
# 🧪 **A · Replay the whole feed twice** (a connector restart redelivers everything). Does the history change?

# %%
con.sql("CREATE OR REPLACE VIEW replayed AS SELECT * FROM changes UNION ALL SELECT * FROM changes")
U.check(as_tuples(history_variant("replayed")) == expected, "replaying the feed changes nothing: the pipeline is idempotent")

# %% [markdown]
# 🧪 **B · Reverse the arrival order** (last event first). Does it change?

# %%
con.sql("CREATE OR REPLACE VIEW reversed AS SELECT * REPLACE (100 - arrival AS arrival) FROM changes")
U.check(as_tuples(history_variant("reversed")) == expected, "arrival order doesn't matter: lsn decides")

# %% [markdown]
# 🧪 **C · Order the window by arrival instead of `lsn`.** What goes wrong for customer 101?

# %%
wrong_order = history_variant(order_by="arrival")
wrong_order[wrong_order.customer_id == 101]

# %% [markdown]
# 🔍 The late `lsn` 18 (Riyadh) is treated as the newest change: customer 101 "moves back" to Riyadh **after** Jeddah,
# the Jeddah version ends before it starts, Dammam's version now overlaps the Riyadh one (two cities at once), and the
# current city is wrong. An older update applied after a newer one is called **state regression**; ordering by the log
# position is the fix.
#
# 🧪 **D · Skip the deduplication.** What appears for customer 101?

# %%
no_dedup = history_variant(dedup=False)
no_dedup[no_dedup.customer_id == 101]

# %% [markdown]
# 🔍 Two Jeddah rows: the first one is valid from 08:00 **to** 08:00, a zero-length version that will double-count in
# some joins and break uniqueness tests (Day 2).
#
# Now you have every piece of Lab 1.4: dedup by `lsn`, latest per key with `QUALIFY`, validity intervals with `lead`,
# deletes dropped after the window, out-of-order detection with a frame.
#
# ## 8 · The same ideas on real data: courier events (15')
#
# The courier app sends one event per delivery step: `assigned → picked_up → out_for_delivery → delivered` (or
# `cancelled`, or `failed`). The feed has redelivered events (same `event_id`, sent twice) and late arrivals.
# The table `courier` below has one row per event received; `event_ts` (given) is when the step happened, in UTC.

# %%
con.sql(f"""
    CREATE TABLE courier AS
    SELECT event_id, order_id, store_id, city, event_type, ingest_time::TIMESTAMP AS ingest_time,
           coalesce(make_timestamp(try_cast(event_time AS BIGINT) * 1000),
                    try_cast(event_time AS TIMESTAMPTZ) AT TIME ZONE 'UTC') AS event_ts
    FROM read_json_auto('{RAW}/events/delivery_events.jsonl')
""")
U.attach_app(con)
con.sql("FROM courier WHERE order_id = (SELECT min(order_id) FROM courier) ORDER BY event_ts").df()

# %% [markdown]
# ✏️ **Exercise 8a** · Remove the redelivered copies: keep **one** row per `event_id` (the first one received, by
# `ingest_time`). This is Task 3's deduplication, written with `QUALIFY`.
# - **Result:** the view `courier_clean`, one row per `event_id`. The check counts it for you.

# %%
# Template:
# con.sql("""
#     CREATE OR REPLACE VIEW courier_clean AS
#     SELECT * FROM courier
#     QUALIFY row_number() OVER (PARTITION BY ___ ORDER BY ingest_time) = 1
# """)
# BEGIN SOLUTION
con.sql("""
    CREATE OR REPLACE VIEW courier_clean AS
    SELECT * FROM courier
    QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingest_time) = 1
""")
# END SOLUTION
n_raw, n_ids, n_clean = con.sql("""
    SELECT (SELECT count(*) FROM courier), (SELECT count(DISTINCT event_id) FROM courier), (SELECT count(*) FROM courier_clean)
""").fetchall()[0]
print(f"{n_raw:,} events received -> {n_clean:,} after dedup")
U.check(n_clean == n_ids < n_raw, f"{n_raw - n_clean} redelivered copies removed")

# %% [markdown]
# ✏️ **Exercise 8b** · Find the **status** of every order: the `event_type` of its **latest** event, by when it
# happened (`event_ts`). This is Exercise 6 on real data.
# - **Result:** `status`, one row per `order_id`, columns `order_id`, `event_type`, read from `courier_clean`.
# - **Check yourself:** every order the app marks as `delivered` ends with a `delivered` event.

# %%
# Template:
# status = con.sql("""
#     SELECT order_id, event_type
#     FROM courier_clean
#     QUALIFY row_number() OVER (PARTITION BY ___ ORDER BY ___ DESC) = 1
# """).df()
# BEGIN SOLUTION
status = con.sql("""
    SELECT order_id, event_type
    FROM courier_clean
    QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY event_ts DESC) = 1
""").df()
# END SOLUTION
status.event_type.value_counts()

# %%
delivered = set(con.sql("SELECT order_id FROM app.orders WHERE status = 'delivered'").df().order_id)
U.check(len(status) == status.order_id.nunique(), "one status per order")
U.check(set(status[status.order_id.isin(delivered)].event_type) == {"delivered"}, "orders the app says are delivered end in 'delivered'")

# %% [markdown]
# ✏️ **Exercise 8c** · For every event, compute `minutes`: how long the order stayed at that step, i.e. the time from
# this event to the **next** event of the same order. The last event of an order has no next event, so `NULL`.
# This is Exercise 4 on real data.
# - **Result:** `stages`, one row per event of `courier_clean`, columns `order_id`, `store_id`, `event_type`,
#   `minutes`. `date_diff('second', a, b) / 60` (given) turns two timestamps into minutes.
# - **Check yourself:** final events (`delivered`, `cancelled`, `failed`) have `minutes` = `NULL`.

# %%
# Template:
# stages = con.sql("""
#     SELECT order_id, store_id, event_type,
#            date_diff('second', event_ts, lead(___) OVER (PARTITION BY ___ ORDER BY ___)) / 60 AS minutes
#     FROM courier_clean
# """).df()
# BEGIN SOLUTION
stages = con.sql("""
    SELECT order_id, store_id, event_type,
           date_diff('second', event_ts, lead(event_ts) OVER (PARTITION BY order_id ORDER BY event_ts)) / 60 AS minutes
    FROM courier_clean
""").df()
# END SOLUTION
stages.head()

# %% [markdown]
# With `stages` you can answer an operations question: at which store do orders wait longest for a courier to pick
# them up (median minutes in the `assigned` step)?

# %%
pickup_wait = stages[stages.event_type == "assigned"].groupby("store_id").minutes.median().sort_values(ascending=False)
print(pickup_wait.round(1).head(4).to_string())
U.check(stages[stages.event_type.isin(["delivered", "cancelled", "failed"])].minutes.isna().all(), "final events have no next event")
U.check(pickup_wait.index[0] == "RUH-01", "RUH-01 is the busy store: pickups wait longest")

# %% [markdown]
# 🧪 **One more experiment** · 🤔 **Predict**: if you take each order's latest event by **arrival** (`ingest_time`)
# instead of by when it happened (`event_ts`), will any order get a different status?

# %%
by_arrival = con.sql("""
    SELECT order_id, event_type FROM courier_clean
    QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY ingest_time DESC) = 1
""").df()
n_wrong = (status.merge(by_arrival, on="order_id", suffixes=("", "_arrival"))
                 .query("event_type != event_type_arrival").shape[0])
print(n_wrong, "orders get the wrong status when ordered by arrival")

# %% [markdown]
# 🔍 A courier's phone that was offline in a basement car park sends its events late, so an older step can **arrive**
# after a newer one. Same lesson as experiment C: order by when it happened, never by when it arrived.
#
# ## Checkpoint
#
# 1. In one sentence each: what does `PARTITION BY` decide, what does the `ORDER BY` inside `OVER` decide, and what does
#    the frame decide?
# 2. Why is `row_number() ... = 1` dangerous on tied rows that differ, and what are two ways to make it safe?
# 3. Why must the delete markers be removed **after** `lead`, and which clause does that?
# 4. Your history query runs twice on the same feed. Why is the result identical, and which line makes it so?
#
# ## 🖼️ Visual summary
#
# The pictures below are drawn from the tables and queries of this lab. Use them as a reference sheet.
#
# **1 · `GROUP BY` vs `OVER`.** Grouping replaces each customer's rows with one row; a window keeps every row and writes
# the group's answer next to it (outlined: the column the window computed).

# %%
V.table_steps([
    {"title": "orders", "df": con.sql("SELECT order_id, customer_id, amount FROM orders ORDER BY order_id").df()},
    {"title": "GROUP BY customer_id: 3 rows",
     "df": con.sql("SELECT customer_id, sum(amount) AS total FROM orders GROUP BY ALL ORDER BY 1").df(), "new": ["total"]},
    {"title": "sum(amount) OVER (PARTITION BY customer_id): 8 rows",
     "df": con.sql("""SELECT order_id, customer_id, amount, sum(amount) OVER (PARTITION BY customer_id) AS total
                      FROM orders ORDER BY customer_id, order_id""").df(), "new": ["total"], "gap_by": "customer_id"},
], color_by="customer_id")

# %% [markdown]
# **2 · A window function, step by step: latest change per customer.** Partition, order inside each partition, number
# the rows, then `QUALIFY` keeps number 1. Nothing is physically re-sorted; the steps are how to think about it.

# %%
step = con.sql("""
    SELECT arrival, lsn, customer_id, op, city,
           row_number() OVER w AS rn, rank() OVER w AS rank
    FROM changes WINDOW w AS (PARTITION BY customer_id ORDER BY lsn DESC)
    ORDER BY customer_id, lsn DESC, arrival
""").df()
V.table_steps([
    {"title": "1 · changes, as they arrived", "df": con.sql("SELECT arrival, lsn, customer_id, op, city FROM changes ORDER BY arrival").df()},
    {"title": "2 · PARTITION BY customer_id ORDER BY lsn DESC", "df": step.drop(columns=["rn", "rank"]), "gap_by": "customer_id"},
    {"title": "3 · row_number() and rank()", "df": step, "new": ["rn", "rank"], "gap_by": "customer_id"},
    {"title": "4 · QUALIFY rn = 1", "df": step, "new": ["rn"], "faded": step.rn > 1, "gap_by": "customer_id"},
], color_by="customer_id")

# %% [markdown]
# **3 · Frames.** For one current row (order 6), which rows each frame lets the aggregate see. `RANGE` (the default)
# pulls in order 7 because it has the same date.

# %%
V.frames(frames[["order_id", "created_at", "amount", "rows_running", "default_frame", "before_me", "neighbours"]],
         "order_id", ["rows_running", "default_frame", "before_me", "neighbours"], current=6,
         titles=["ROWS BETWEEN\nUNBOUNDED PRECEDING\nAND CURRENT ROW", "default frame\n(RANGE: includes\nthe row's peers)",
                 "ROWS BETWEEN\nUNBOUNDED PRECEDING\nAND 1 PRECEDING", "ROWS BETWEEN\n1 PRECEDING\nAND 1 FOLLOWING"])

# %% [markdown]
# **4 · Logical evaluation order.** Windows are computed after `WHERE`, `GROUP BY` and `HAVING`; `QUALIFY` filters on them.

# %%
V.evaluation_order()

# %% [markdown]
# **5 · SCD2 validity intervals.** Each version runs until the next change of the same customer (`lead` by `lsn`).
# Top: the correct history. Below it, the three ways to get it wrong from Tasks 6 and 7.

# %%
V.timelines([
    ("Correct: dedup, lead by lsn, deletes dropped after the window (QUALIFY)", history),
    ("Deletes dropped too early (WHERE): customer 103 never leaves", too_early),
    ("Ordered by arrival: lsn 18 overwrites Jeddah, and Dammam overlaps Riyadh (state regression)", wrong_order),
    ("No dedup: a zero-length Jeddah version", no_dedup),
], until="2026-09-24")
