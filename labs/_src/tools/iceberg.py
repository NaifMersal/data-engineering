# %% [markdown]
# # Iceberg · open table formats, on a lightboard
#
# Day 3, before Lab 3.1 · CPU only. A folder of Parquet files breaks as soon as a load dies halfway or one row must
# change. You watch it break, then build an Iceberg table commit by commit and replay what each commit wrote on disk.
#
# **The data:** Qafila's **payments** feed; Lab 3.1 then builds the **orders** table. Same ideas, different table.
#
# **Rhythm:** 🤔 predict → run → 🔍 read. After the code, a **board** (a step-by-step player) replays what just happened
# on disk, one piece at a time, the way you would draw it on a lightboard: click **Next ▶** or **▶ Play**. Except for
# the history board, everything on a board is read from the table's own metadata files.
#
# **Exercises (✏️):** uncomment the template (`Ctrl+/`), replace every `___`, delete the two `YOUR CODE HERE` lines,
# then run the cell and the check after it (✅ = right). Answers: `solutions/tools/iceberg.ipynb`.
#
# | | Section | The one idea |
# |---|---|---|
# | | **A · Why Iceberg had to exist** | |
# | 0 | How we got here | databases → data lakes → Hive folders → a metadata layer |
# | 1 | A folder is not a table | readers see half-finished loads, no safe updates |
# | | **B · How it works, layer by layer** | |
# | 2 | The catalog | one pointer per table |
# | 3 | One commit, bottom-up | data files → manifests → manifest list → metadata file → pointer swap; a crash before the swap shows nothing |
# | 4 | Reading | follow the pointers down, skip files from metadata alone |
# | | **C · What the layers buy you** | |
# | 5 | Delete one row | copy-on-write: a new file, a new snapshot |
# | 6 | Time travel | every snapshot is a version you can read |
# | 7 | Two writers | optimistic concurrency: one compare-and-swap; conflicting rewrites fail |
# | 8 | Rename a column | columns are tracked by id |
# | 9 | Another engine | DuckDB follows the same pointers |
# | 10 | Back to the map | the formats, the catalogs, and what Lab 3.1 adds |
#
# The boards are HTML players: run the notebook in Jupyter, VS Code or Colab (GitHub's preview won't show them).

# %% [markdown] 🎓
# 🎓 **Instructor notes** (solution notebook only)
#
# - Walked as a lightboard: ask each 🤔 **before** running the cell, then step through the board with **Next ▶** and
#   talk over each step. About 25' in class without the exercises; the three ✏️ are for the students' own pace.
# - Stop at ① on the commit board (§3) and ask "if the job died right here, what would a reader see?".
# - Lab 3.1 creates the partitioned table, upserts refunds, erases a customer, time-travels per snapshot, evolves the
#   schema **and the partitioning**, and reads from DuckDB and Polars: don't do those here.

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
import json
import sqlite3

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.transforms import DayTransform

import de_viz as V

pd.set_option("display.max_colwidth", 80)
DEMO = U.lab_dir("tools_iceberg", reset=True)
con = duckdb.connect()
con.sql("SET enable_progress_bar = false")
con.sql("SET TimeZone = 'UTC'")   # the sources' timestamps are UTC: read and cast them the same on any machine

payments = con.sql(f"""
    SELECT payment_id, order_id, amount, method, status, created_at::TIMESTAMP AS created_at
    FROM read_json('{(U.RAW / "api" / "payments.jsonl").as_posix()}') ORDER BY created_at
""").to_arrow_table()

def day(d):
    """Payments created on 2026-08-<d> (UTC)."""
    return con.sql(f"SELECT * FROM payments WHERE created_at::DATE = DATE '2026-08-{d:02d}'").to_arrow_table()

print(f"{payments.num_rows:,} payments")
payments.slice(0, 3).to_pandas()

# %% [markdown]
# You are here: Iceberg **stores**. It turns a folder of Parquet files into a table (bronze, silver or gold) that many
# engines can read and change safely.

# %%
V.platform_map(tool="iceberg")

# %% [markdown]
# # Part A · Why Iceberg had to exist
#
# ## 0 · How we got here
#
# Before any code, the story in six strokes. 🤔 Start with a question for the room: *a database always knows what's
# in a table. Why doesn't a folder of files?*

# %%
V.lakehouse_history()

# %% [markdown]
# ## 1 · A folder of Parquet files is not a table
#
# The Day 1 way: one Parquet file per day in a folder, and readers glob the folder. A nightly job loads four more days
# and dies after writing two of them.
#
# 🤔 What does a dashboard reading the folder see now? Can it tell that something is missing?

# %%
plain = DEMO / "plain_payments"
plain.mkdir()
for d in range(1, 5):
    pq.write_table(day(d), plain / f"day={d:02d}.parquet")

for d in range(5, 9):
    if d == 7:
        print("💥 the load job died after writing days 5 and 6")
        break
    pq.write_table(day(d), plain / f"day={d:02d}.parquet")

con.sql(f"SELECT created_at::DATE AS day, count(*) AS payments FROM '{plain.as_posix()}/*.parquet' GROUP BY ALL ORDER BY day").df()

# %% [markdown]
# Six days, no error, no warning. The crash itself isn't the problem: jobs die in every system, Iceberg included
# (section 3 kills one). The problem is that in a folder, **writing** a file *is* **publishing** it, so readers see
# whatever the job managed to write, and nothing records what it was supposed to write.
#
# Then finance asks us to mark **one** payment as refunded, and Parquet files are
# immutable: there's no `UPDATE`.
#
# 🤔 What's the smallest thing we can rewrite? And what does a reader see while we do it?
#
# The board replays the folder from what's on disk (the red slots are the files the job never wrote).

# %%
V.plain_folder(plain, loads=[[f"day={d:02d}" for d in range(1, 5)], [f"day={d:02d}" for d in range(5, 9)]],
               rewrite="day=03")

# %% [markdown]
# Readers that see half-finished loads, no safe updates, no safe concurrent writers, and no single place that says
# what the table contains. A **table format** fixes all of these the same way: a metadata layer that lists exactly which files make
# up the table at each version.
#
# # Part B · How it works, layer by layer
#
# Iceberg splits "the table" into layers, and each layer answers one question:
#
# | Layer | Answers | How many |
# |---|---|---|
# | **catalog** | *where is the table now?* | one row per table: the path of the current metadata file |
# | **metadata file** (`*.metadata.json`) | *what is the table, and what was it?* | one per commit: schema, partition spec, the list of snapshots, the earlier metadata files |
# | **manifest list** (`snap-*.avro`) | *which manifests make up this snapshot?* | one per snapshot |
# | **manifest** (`*-m0.avro`) | *which data files, and what's in them?* | file paths, row counts, min/max per column |
# | **data files** (`*.parquet`) | the rows | never edited, only added or dropped |
#
# Two counting rules for the rest of the demo:
#
# - **Every commit writes a new metadata file.** Nothing is edited in place; the catalog pointer moves to the new file.
# - **Only commits that change data create a snapshot.** A snapshot is a version of the table's *data*. Changing the
#   schema or the partitioning writes a metadata file, but no snapshot.
#
# We build it in that order: the catalog first, then one commit from the bottom up.
#
# ## 2 · The catalog is one pointer
#
# PyIceberg's `SqlCatalog` keeps the catalog in a SQLite file; the "object store" is a local folder. In production the
# catalog is a REST service (Polaris, Unity Catalog, Glue, Lakekeeper...) and the folder is S3 or ADLS.
#
# The table gets a **partition spec**: the rule for grouping rows into files, written as *source column + transform*.
# Ours is `day(created_at)`, so every day's payments land in their own files. On write, Iceberg applies the transform to
# each row (we never add a `created_day` column ourselves); on read, it turns a filter on `created_at` into a filter on
# the partition, which is how section 4 skips files. The spec lives in the metadata file, not in folder names, so it can
# change later without rewriting data (Lab 3.1 does that). Other transforms: `month`, `hour`, `bucket(N)`,
# `truncate(W)`, `identity`.
#
# 🤔 We create an empty table, partitioned by day. What gets written: data files, metadata, or both?

# %%
LAKE = DEMO / "lakehouse"
catalog = U.get_catalog(LAKE)
catalog.create_namespace("qafila")
tbl = catalog.create_table("qafila.payments", schema=payments.schema)
with tbl.update_spec() as spec:
    spec.add_field("created_at", DayTransform(), "created_day")   # hidden partitioning: derived from created_at


def pointer():
    """What the catalog stores for qafila.payments: just the location of the current metadata file."""
    row = sqlite3.connect(LAKE / "catalog.db").execute(
        "SELECT table_namespace, table_name, metadata_location FROM iceberg_tables").fetchone()
    return f"{row[0]}.{row[1]} → …/{Path(row[2]).name}"

pointer()

# %% [markdown]
# That's the **entire** catalog entry: a table name and a path. On the board: two metadata files (one for
# `create_table`, one for the partition spec), nothing below them, and the pointer.

# %%
V.iceberg_commit(None, tbl.metadata_location, title="Creating a table writes only metadata")

# %% [markdown]
# Why two metadata files? Because that cell made **two commits**:
#
# 1. `create_table` wrote `00000`: the schema, no partitioning, no snapshots. The catalog row was created pointing at it.
# 2. `update_spec` is a commit of its own. Iceberg never edits a metadata file, so it wrote `00001` (a full copy of
#    `00000` plus the day partitioning) and moved the pointer from `00000` to `00001`.
#
# Neither commit added data, so there's no snapshot yet (rule 2).
#
# 🤔 Two commits so far. How many rows does the catalog hold for this table?

# %%
rows = sqlite3.connect(LAKE / "catalog.db").execute(
    "SELECT table_name, metadata_location, previous_metadata_location FROM iceberg_tables").fetchall()
pd.DataFrame([(t, Path(cur).name, Path(prev).name) for t, cur, prev in rows],
             columns=["table_name", "metadata_location", "previous_metadata_location"])

# %% [markdown]
# One. A commit **updates** the table's row (and remembers the previous file); it never adds a row. The history lives
# in the metadata files, as section 3 shows.
#
# ## 3 · One commit, bottom-up
#
# 🤔 We append four days of payments. How many files appear on disk? In which order must the writer create them so
# that a reader never sees half of the load?

# %%
def show_files():
    root = LAKE / "qafila" / "payments"
    for p in sorted(root.rglob("*")):
        if p.is_file():
            print(f"{str(p.relative_to(root)):92s} {p.stat().st_size / 1e3:6.1f} kB")

v_empty = tbl.metadata_location
tbl.append(pa.concat_tables([day(d) for d in range(1, 5)]))
show_files()
print("\ncatalog:", pointer())

# %% [markdown]
# Two kinds of files:
# - `data/created_day=…/` holds **Parquet**, one folder per day. We never wrote `created_day` ourselves: Iceberg
#   derived it.
# - `metadata/` holds one `*.metadata.json` per commit, one `snap-*.avro` **manifest list** per snapshot, and the
#   `*-m0.avro` **manifest** that lists the data files.
#
# Now replay that commit in the order the writer did it, from the bottom up. Stop at ① and ask: *if the job died
# right here, what would a reader see?*

# %%
V.iceberg_commit(v_empty, tbl.metadata_location, title="Append four days, bottom-up")

# %% [markdown]
# The metadata file is plain JSON. The parts that matter:

# %%
meta = json.loads(Path(tbl.metadata_location).read_text())
print("current-snapshot-id:", meta["current-snapshot-id"])
print("schema fields:", [(f["id"], f["name"], f["type"]) for f in meta["schemas"][-1]["fields"]])
print("partition spec:", meta["partition-specs"][-1]["fields"])
for s in meta["snapshots"]:
    print("snapshot", s["snapshot-id"], s["summary"]["operation"], f"+{s['summary']['added-records']} rows",
          "→", Path(s["manifest-list"]).name)
print("earlier metadata files:", [Path(m["metadata-file"]).name[:5] for m in meta["metadata-log"]])

# %% [markdown]
# Notice the field **ids** (1, 2, 3...). Iceberg tracks columns by id, not by name. Section 8 uses that.
#
# This file is also the table's **history**. There's no separate log: each commit copies the list of snapshots and the
# list of earlier metadata files from the previous metadata file, then adds its own entry. So far: three commits
# (`00000`, `00001`, `00002`), one snapshot, because only the append changed data. The catalog only needs the newest
# file; everything older is reachable from it. (Delta Lake keeps the same information as a numbered log,
# `_delta_log/000…0.json`, `000…1.json`, …, and Hudi as a "timeline". Same idea, different file layout.)
#
# A snapshot records **which files** make up the table, not which rows changed. Its summary says what kind of change
# and how much; to see exactly what changed, compare two snapshots' manifests. That's what every board does.
#
# ### What if the load dies before the pointer swap?
#
# Back to section 1's failure: the nightly load of days 5 to 8 dies. This writer gets further than the folder job did:
# it writes **all** the Parquet files, then dies just before committing. A PyIceberg transaction lets us stop right
# there: `append` writes the data files immediately, and only `commit_transaction()` writes the metadata and swaps the
# pointer.
#
# 🤔 The new files sit in the table's own `data/` folder. What does a reader see?

# %%
txn = tbl.transaction()
txn.append(pa.concat_tables([day(d) for d in range(5, 9)]))   # the Parquet files are written now
print("💥 the load job died before committing")                 # txn.commit_transaction() never runs
del txn

on_disk = len(list((LAKE / "qafila" / "payments" / "data").rglob("*.parquet")))
print(f"Parquet files on disk: {on_disk} | files in the table: {len(tbl.inspect.files())}")
print("catalog:", pointer())

reader = catalog.load_table("qafila.payments").scan().to_arrow()
con.sql("SELECT created_at::DATE AS day, count(*) AS payments FROM reader GROUP BY ALL ORDER BY day").df()

# %% [markdown]
# Still four days, with no half-finished state in between. The new files are on disk, but no manifest lists them, so
# for every reader they don't exist: they're **orphans**, and a "remove orphan files" maintenance job deletes them
# later. The crash happened just like in section 1. What changed is that **writing** a file and **publishing** it are
# now separate steps, and publishing is one atomic pointer swap.
#
# ## 4 · Readers follow pointers and skip files
#
# First, rerun the load that died: days 5 to 8. Nothing needs cleaning up before the retry.
#
# 🤔 S1's manifest already lists days 1 to 4. Does the second commit rewrite it?

# %%
v_s1 = tbl.metadata_location
tbl.append(pa.concat_tables([day(d) for d in range(5, 9)]))
print("catalog:", pointer())
V.iceberg_commit(v_s1, tbl.metadata_location, title="A second append reuses what's there")

# %% [markdown]
# No: snapshot S2's manifest list points to S1's manifest **as it is**, plus one new manifest. An append only adds.
#
# The manifests store per-file statistics: row counts and min/max per column. A reader uses them to skip whole files
# before opening any Parquet.

# %%
files = tbl.inspect.files().to_pandas()
pd.DataFrame({
    "file": files.file_path.str.split("/").str[-2],
    "rows": files.record_count,
    "min created_at": [m["created_at"]["lower_bound"] for m in files.readable_metrics],
    "max created_at": [m["created_at"]["upper_bound"] for m in files.readable_metrics],
}).sort_values("file", ignore_index=True)

# %% [markdown]
# 🤔 A query asks for payments on 2 August between 18:00 and 20:00. It filters on `created_at`, never on
# `created_day`. How many of the eight files does the reader open? How many manifests?

# %%
evening = "created_at >= '2026-08-02T18:00:00' and created_at < '2026-08-02T20:00:00'"
planned = list(tbl.scan(row_filter=evening).plan_files())
print(f"files in the table: {len(files)} | files planned for the query: {len(planned)}")
print([t.file.file_path.split("/")[-2] for t in planned])
V.iceberg_read(tbl, evening)

# %% [markdown]
# No directory listing, no `WHERE created_day = ...` in the query. That's **hidden partitioning**: the user filters on
# the real column and Iceberg maps it to partitions. The board's decisions are PyIceberg's own evaluators, the same
# ones `plan_files()` used.
#
# ✏️ **Exercise 1** · Write `two_days`, a row filter for the payments of **5 and 6 August** (UTC), on `created_at`.
# - **Result:** `two_days`, a string like `evening` above.
# - **Check yourself:** the scan plans exactly 2 of the 8 files, and returns every payment of those two days.

# %%
# Template:
# two_days = "created_at >= '___' and created_at < '___'"
# BEGIN SOLUTION
two_days = "created_at >= '2026-08-05T00:00:00' and created_at < '2026-08-07T00:00:00'"
# END SOLUTION
planned_two = sorted(t.file.file_path.split("/")[-2] for t in tbl.scan(row_filter=two_days).plan_files())
planned_two

# %%
U.check(planned_two == ["created_day=2026-08-05", "created_day=2026-08-06"], "2 files planned: 5 and 6 August")
U.check(tbl.scan(row_filter=two_days).to_arrow().num_rows == day(5).num_rows + day(6).num_rows,
        "every payment of the two days, and nothing else")

# %% [markdown]
#
# # Part C · What the layers buy you
#
# ## 5 · A one-row delete: copy-on-write
#
# A test payment from 3 August should never have been loaded. Delete it.
#
# 🤔 One row goes. How many rows does Iceberg **write**? What happens to the old file?

# %%
victim = day(3)["payment_id"][0].as_py()
v_s2 = tbl.metadata_location
tbl.delete(f"payment_id = '{victim}'")

summary = tbl.inspect.snapshots().to_pandas().iloc[-1]
print("operation:", summary.operation)
pd.Series(dict(summary.summary))[["deleted-data-files", "added-data-files", "deleted-records", "added-records"]]

# %%
V.iceberg_commit(v_s2, tbl.metadata_location, title="Delete one row, copy-on-write")

# %% [markdown]
# PyIceberg removed the whole file for 3 August and wrote a new one **without** that row: **copy-on-write**. Reads stay
# fast, but writes cost a whole file per changed row. Spark and Flink can also write **merge-on-read**: keep the data
# file and add a small delete file (a **deletion vector** in Iceberg v3) that readers merge in. Cheap writes, slower
# reads until compaction.
#
# The snapshot summary above says *what kind* of change and *how much*. Which row went is not recorded anywhere: you
# find it by comparing the removed file with the one that replaced it.
#
# ## 6 · Time travel
#
# The old file wasn't deleted from disk. It's just no longer in the **current** snapshot. Time travel uses the history
# from section 3: pick an older snapshot from the current metadata file's list and follow its manifest list down.
#
# 🤔 Can we still read the test payment? From which snapshots?

# %%
notes = {}
for s in tbl.snapshots():
    t = tbl.scan(snapshot_id=s.snapshot_id, row_filter=f"payment_id = '{victim}'").to_arrow()
    print(f"snapshot {s.snapshot_id}  {s.summary.operation.value:9s}  test payment visible: {t.num_rows == 1}")
    notes[s.snapshot_id] = f"Test payment <code>{victim}</code>: <b>{'visible' if t.num_rows else 'gone'}</b>."
V.iceberg_history(tbl, notes)

# %% [markdown]
# Useful for debugging ("what did the table say when the dashboard broke?") and for reproducible training sets. Bad news
# for the **right to erasure**: deleted rows live on in old snapshots until you expire them (Lab 3.1 ✱, Lab 4.3).
#
# ✏️ **Exercise 2** · Section 5 said that which row a delete removed is recorded nowhere: you find it by comparing two
# snapshots. Do it. `tbl.snapshots()` lists them oldest first: the first append (S1), the second (S2), then the delete.
# - **Result:** `gone`, the set of `payment_id`s in S2 that are not in the current snapshot.
# - **Check yourself:** one payment, the test payment of 3 August.

# %%
# Template:
# s2 = tbl.snapshots()[___].snapshot_id
# ids_then = set(tbl.scan(snapshot_id=___).to_arrow()["payment_id"].to_pylist())
# ids_now = set(tbl.scan().to_arrow()["payment_id"].to_pylist())
# gone = ___
# BEGIN SOLUTION
s2 = tbl.snapshots()[1].snapshot_id
ids_then = set(tbl.scan(snapshot_id=s2).to_arrow()["payment_id"].to_pylist())
ids_now = set(tbl.scan().to_arrow()["payment_id"].to_pylist())
gone = ids_then - ids_now
# END SOLUTION
gone

# %%
U.check(gone == {victim}, "the only payment that left between S2 and now is the test payment")

# %% [markdown]
#
# ## 7 · Two writers at once: optimistic concurrency
#
# Two jobs load the table at the same moment. Both see the same current snapshot. Writer A commits first.
#
# 🤔 What happens to writer B: corruption, blocking, or something else?

# %%
writer_a = catalog.load_table("qafila.payments")
writer_b = catalog.load_table("qafila.payments")
v_start = writer_a.metadata_location

writer_a.append(day(9))
print("A committed:", pointer())
writer_b.append(day(10))    # B's view is stale: the catalog pointer moved since B loaded the table
print("B committed:", pointer())

tbl.refresh()
V.iceberg_race(v_start, writer_a.metadata_location, tbl.metadata_location)

# %% [markdown]
# B wrote its Parquet file without any lock. At commit time it asked the catalog to swap the pointer **from the version
# it read**. The pointer had moved, so the swap failed (the "Commit failed … retrying" line). B re-read the new
# metadata, checked that A's append doesn't conflict with its own, and committed on top. Data files are never shared,
# so only the pointer swap needs to be serialized.
#
# ### When the changes do conflict
#
# Appends never conflict: each one adds its own files and removes nothing. Deletes and overwrites are different. They
# **remove** files, and they were planned against the snapshot the writer read. So on retry the writer checks the
# commits it missed: *did any of them add or remove a file my filter could match?* If so, it **fails** instead of
# committing a change it planned on data that has since changed.
#
# Three races, each starting from the snapshot both writers read. 🤔 Predict each outcome before running:
#
# | | Writer A | Writer B |
# |---|---|---|
# | 1 | deletes payment X from 3 August | deletes payment Y, **also** from 3 August |
# | 2 | deletes a payment from 3 August | deletes a payment from 4 August, and the filter names the day |
# | 3 | deletes a payment from 3 August | deletes a payment from 4 August, filter on `payment_id` only |

# %%
from pyiceberg.exceptions import ValidationException

def race(a, b):
    """A and B load the same snapshot; A commits, then B tries."""
    writer_a, writer_b = catalog.load_table("qafila.payments"), catalog.load_table("qafila.payments")
    a(writer_a)
    try:
        b(writer_b)
        return "both committed"
    except ValidationException:
        return "B failed: conflict"

def on_day(d):
    return f"created_at >= '2026-08-{d:02d}T00:00:00' and created_at < '2026-08-{d + 1:02d}T00:00:00'"

aug3, aug4 = day(3)["payment_id"], day(4)["payment_id"]   # aug3[0] is the test payment section 5 deleted
x, y = aug3[1], aug3[2]
pd.Series({
    "1 · same file": race(lambda A: A.delete(f"payment_id = '{x}'"),
                          lambda B: B.delete(f"payment_id = '{y}'")),
    "2 · different days, filter names the day": race(lambda A: A.delete(f"payment_id = '{aug3[3]}' and {on_day(3)}"),
                                                     lambda B: B.delete(f"payment_id = '{aug4[0]}' and {on_day(4)}")),
    "3 · different days, payment_id only": race(lambda A: A.delete(f"payment_id = '{aug3[4]}'"),
                                               lambda B: B.delete(f"payment_id = '{aug4[1]}'")),
}, name="outcome").to_frame()

# %% [markdown]
# 1. **Conflict.** Both writers rewrite the one 3 August file. A's new file still holds Y; B's new file would still hold
#    X. Had B committed anyway, 3 August would be in the table **twice**. B stops instead, and nothing it wrote is
#    visible: its commit never landed.
# 2. **No conflict.** The filters name the day, so Iceberg proves from the partition values alone that A's new 3 August
#    file can't hold B's 4 August row.
# 3. **Conflict, though the rows are on different days.** B's filter says nothing about the day, so the only evidence
#    is the min/max of `payment_id` in A's new file, and that range *could* include B's payment. The check sees only
#    metadata, so when in doubt it fails. Put the partition column in the filter (race 2) and the false conflict goes
#    away; Spark and Delta give the same advice for `MERGE`.
#
# A failed writer recovers the way any optimistic system does: reload the table and run the job again. (On Day 4 the
# orchestrator does that retry for you.)

# %%
tbl.refresh()
tbl.delete(f"payment_id = '{y}'")            # B's job, run again on the new snapshot
left = tbl.scan(row_filter=f"payment_id in ('{x}', '{y}')").to_arrow().num_rows
print("payments X and Y left in the table:", left)

# %% [markdown]
# ## 8 · Rename a column without touching the data
#
# The app team renames `method` to `payment_method`.
#
# 🤔 How many Parquet files does Iceberg rewrite?

# %%
v_before_rename = tbl.metadata_location
before = set(tbl.inspect.files().to_pandas().file_path)
with tbl.update_schema() as upd:
    upd.rename_column("method", "payment_method")
after = set(tbl.inspect.files().to_pandas().file_path)

print("data files rewritten:", len(before ^ after))
print(tbl.schema())
tbl.scan(row_filter="created_at < '2026-08-01T04:00:00'", limit=3).to_pandas()

# %%
V.iceberg_rename(v_before_rename, tbl.metadata_location)

# %% [markdown]
# Zero. The Parquet files still say `method` inside, but they also carry field id 4, and the new schema says id 4 is
# now called `payment_method`. Rename, reorder, add and drop are all metadata-only. In a folder of Parquet files read
# by **name**, the rename would silently turn the old column into nulls.
#
# ## 9 · Another engine, same table: DuckDB
#
# DuckDB doesn't know PyIceberg. 🤔 Will it see the delete and the rename?

# %%
U.duckdb_iceberg(con)
con.sql(f"""
    SELECT payment_method, count(*) AS payments, round(sum(amount)) AS amount_sar
    FROM iceberg_scan('{tbl.metadata_location}')
    GROUP BY ALL ORDER BY payments DESC
""").df()

# %%
V.iceberg_engines(tbl.metadata_location, [("PyIceberg", "wrote every commit", "catalog"),
                                          ("DuckDB", "iceberg_scan", "metadata")])

# %% [markdown]
# One copy of the data, many engines, one answer to "what's in the table right now". In production a Spark job writes,
# DuckDB or Trino answers ad-hoc questions, and Snowflake reads through a catalog integration.
#
# ✏️ **Exercise 3** · Every metadata file is a whole version of the table. `v_s2` (section 5) is the metadata file from
# just **before** the delete. Count the payments of 3 August in that version, with DuckDB.
# - **Result:** `aug3_then`, a number.
# - **Check yourself:** all of 3 August's payments, test payment included; the current version has fewer (sections 5
#   and 7 deleted some).

# %%
# Template:
# aug3_then = con.sql(f"""
#     SELECT count(*) FROM iceberg_scan('{___}') WHERE created_at::DATE = DATE '2026-08-03'
# """).fetchall()[0][0]
# BEGIN SOLUTION
aug3_then = con.sql(f"""
    SELECT count(*) FROM iceberg_scan('{v_s2}') WHERE created_at::DATE = DATE '2026-08-03'
""").fetchall()[0][0]
# END SOLUTION
aug3_then

# %%
aug3_now = con.sql(f"""
    SELECT count(*) FROM iceberg_scan('{tbl.metadata_location}') WHERE created_at::DATE = DATE '2026-08-03'
""").fetchall()[0][0]
U.check(aug3_then == day(3).num_rows, "the old version still has every payment of 3 August")
U.check(aug3_now < aug3_then, f"the current version has {aug3_then - aug3_now} fewer: the deletes of sections 5 and 7")

# %% [markdown]
# ## 10 · Back to the map
#
# Iceberg **stores**: it turns files on object storage into tables that any engine can read and change safely, in any
# zone (bronze, silver or gold).

# %%
V.platform_map(tool="iceberg")

# %% [markdown]
# | | |
# |---|---|
# | **It does** | atomic commits on files (one pointer swap) · snapshots and time travel · row-level deletes and upserts · schema and partition evolution without rewriting data · file skipping from metadata · many engines on one copy |
# | **It doesn't** | run queries (engines do: DuckDB, Spark, Trino, Snowflake...) · compact files or expire snapshots by itself (maintenance jobs do, Lab 3.1 ✱) · decide who may read what (the catalog does, Day 4) |
# | **In this course** | Lab 3.1 builds `qafila.orders` · Lab 3.4 commits a stream into Iceberg · Lab 4.3 erases a customer for good |
# | **Managed equivalents** | AWS S3 Tables / Glue, Databricks Unity Catalog, Snowflake (managed Iceberg tables), BigQuery BigLake tables: the same format, with the catalog and maintenance run for you |
#
# **Four open table formats.** The same idea, a metadata layer over Parquet files; they differ in where that metadata
# lives.
#
# | Format | From | Metadata lives in | Known for |
# |---|---|---|---|
# | Apache Iceberg | Netflix → Apache | JSON + Avro files, and a catalog | the broadest engine support; the usual "many engines, one copy" choice |
# | Delta Lake | Databricks → Linux Foundation | a numbered log, `_delta_log/*.json` + checkpoints | Databricks' default; UniForm exposes Delta tables as Iceberg |
# | Apache Hudi | Uber → Apache | a timeline of files | record-level upserts and incremental pulls |
# | DuckLake | DuckDB Labs | a SQL database (PostgreSQL, SQLite, DuckDB) | Parquet data with the catalog and metadata in SQL: simple, fast commits |
#
# Which one? Follow your engines and your catalog. Iceberg v3 (2025) shares deletion vectors with Delta and adds a
# `VARIANT` type for semi-structured JSON: the formats are converging.
#
# **The catalog is the control plane.** Every engine must agree on one thing: which metadata file is current. The
# **Iceberg REST catalog** spec lets any engine talk to any catalog over HTTP, and catalogs now also hand out short-lived
# storage credentials and enforce access control, so governance moves there (Day 4). Open source: Apache Polaris,
# Unity Catalog OSS, Lakekeeper, Nessie, Apache Gravitino. In the cloud: AWS Glue / S3 Tables, Databricks Unity
# Catalog, Snowflake Open Catalog, BigLake. Here: PyIceberg's `SqlCatalog` on SQLite, with a local folder playing S3.
#
# **Recap**
#
# | We saw | Because |
# |---|---|
# | a folder hid a failed load | nothing lists which files a folder table should contain |
# | a failed Iceberg load showed nothing | data files are written first and published last, by one pointer swap |
# | the catalog stores one path, in one row | a commit = one atomic pointer swap, written last |
# | two commits, two metadata files, no snapshot | every commit writes metadata; only data changes make a snapshot |
# | the history sits in the metadata file | each commit copies the snapshot list forward and adds to it |
# | a reader opened 1 of 8 files | manifest lists and manifests keep partition ranges and per-file stats |
# | a one-row delete rewrote a file | PyIceberg writes copy-on-write |
# | old snapshots still read the row | files are immutable; snapshots are versions |
# | writer B retried, nobody blocked | optimistic concurrency |
# | two deletes in one file: B failed | on retry, rewrites are checked against the commits they missed |
# | a rename rewrote nothing | columns are tracked by id |
#
# ## Checkpoint
#
# 1. A load dies halfway. What does a reader see with a folder of Parquet files, and with an Iceberg table? Why?
# 2. Name the layers from the catalog down to the data files, and the one question each answers.
# 3. Spark and DuckDB write to the same Iceberg table at the same moment. What happens to each writer?
# 4. A one-row delete rewrote a whole file. What would merge-on-read have written instead, and what does it cost readers?
# 5. A customer is deleted from the table. Is their data gone from storage? What else must happen?
#
# **Next: [Lab 3.1 · An Iceberg lakehouse](../D3_1_iceberg_lakehouse.ipynb)** does the same on `qafila.orders`: build
# the table, upsert refunds, erase a customer, evolve the schema **and the partitioning**, and read it from DuckDB and
# Polars.
