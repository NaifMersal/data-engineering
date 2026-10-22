"""The course's sessions, in one list: writes the session tables of index.html and labs/README.md.

    uv run python _tools/sessions.py

Each table sits between two marker comments (`<!-- sessions:begin -->` ... `<!-- sessions:end -->` in index.html,
`<!-- notebooks:begin -->` ... `<!-- notebooks:end -->` in labs/README.md); everything outside them is hand-written.
"""

from __future__ import annotations

import re
from pathlib import Path

LABS = Path(__file__).resolve().parents[1]
ROOT = LABS.parent
COLAB = "https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/"
PAGES = "https://sdaiaacademy.github.io/data-engineering/"

DAYS = {
    1: ("Data Platforms, Storage & Ingestion", "teal"),
    2: ("Transformation, Modeling & Quality", "cyan"),
    3: ("Lakehouse, Scale & Streaming", "coral"),
    4: ("Production: Orchestration, Reliability, Governance & AI", "purple"),
}

# teach: a tool walkthrough (labs/tools/<name>.ipynb) or a concept deck (slides/<name>.html);
# opens_with: (tool, section) of another walkthrough taught first in the same session
SESSIONS = [
    dict(id="D1_1", title="The modern data platform", stage="Store · query", tool="duckdb",
         topics="OLTP vs. OLAP, an analytical database in a library, files in place, plans",
         lab="D1_1_platform_tour"),
    dict(id="D1_2", title="Files & columnar storage", stage="Store", deck="D1_2_files_parquet",
         topics="Parquet internals, statistics, partitioning, pushdown, Arrow", lab="D1_2_files_parquet"),
    dict(id="D1_3", title="Batch ingestion", stage="Ingest", tool="dlt",
         topics="full vs. incremental, cursors, pagination, retries, schema evolution", lab="D1_3_ingestion_dlt"),
    dict(id="D1_4", title="Change data capture & load strategies", stage="Ingest → silver", deck="D1_4_cdc_scd",
         topics="Debezium-style CDC, MERGE, deletes, SCD Type 2", lab="D1_4_cdc_merge_scd"),
    dict(id="D2_1", title="Bronze to silver", stage="Bronze → silver", tool="polars",
         topics="expressions, joins, windows, lazy plans; the same question in DuckDB and Polars",
         lab="D2_1_bronze_to_silver"),
    dict(id="D2_2", title="Dimensional modeling", stage="Silver → gold", deck="D2_2_modeling",
         topics="grain, facts and dimensions, star schema, fan-out, surrogate keys", lab="D2_2_dimensional_modeling"),
    dict(id="D2_3", title="dbt: transformation as software", stage="Silver → gold", tool="dbt",
         topics="sources, refs, layers, materializations, incremental models, macros, lineage",
         lab="D2_3_dbt_project"),
    dict(id="D2_4", title="Testing & data quality", stage="Test", tool="pandera", opens_with=("dbt", "§9"),
         topics="dbt tests, warn vs. error, contracts, unit tests; Pandera schemas, quarantine",
         lab="D2_4_data_quality"),
    dict(id="D3_1", title="Open table formats & the lakehouse", stage="Store (lakehouse)", tool="iceberg",
         topics="Iceberg metadata, snapshots, time travel, concurrency, schema and partition evolution",
         lab="D3_1_iceberg_lakehouse"),
    dict(id="D3_2", title="Distributed processing with Spark", stage="Transform at scale", tool="spark",
         topics="partitions, lazy plans, shuffles, joins, skew and salting, AQE", lab="D3_2_spark",
         runs_on="Colab (Java 17+)"),
    dict(id="D3_3", title="Streaming fundamentals", stage="Ingest (stream)", deck="D3_3_streaming",
         topics="logs and offsets, delivery guarantees, event time, windows, watermarks",
         lab="D3_3_streaming_fundamentals", demo="D3_3_streaming_demo"),
    dict(id="D3_4", title="Real-time pipelines into the lakehouse", stage="Stream → lakehouse",
         deck="D3_4_stream_lakehouse",
         topics="exactly-once micro-batches, small files and compaction, near-real-time SLAs",
         lab="D3_4_stream_to_lakehouse", demo="D3_4_stream_lakehouse_demo"),
    dict(id="D4_1", title="Orchestration", stage="Orchestrate", tool="dagster",
         topics="assets, partitions, asset checks, schedules, sensors, dbt in the graph",
         lab="D4_1_orchestration_dagster"),
    dict(id="D4_2", title="Observability & data contracts", stage="Observe · test",
         deck="D4_2_contracts_observability",
         topics="freshness and volume monitors, ODCS, breaking changes, lineage", lab="D4_2_contracts_observability"),
    dict(id="D4_3", title="Governance, security & privacy", stage="Govern", deck="D4_3_governance_privacy",
         topics="PDPL, NDMO classification, Saudi PII, pseudonymization, masking, erasure",
         lab="D4_3_governance_privacy"),
    dict(id="D4_4", title="Data engineering for AI + capstone kickoff", stage="Serve", deck="D4_4_data_for_ai",
         topics="semantic layer, embeddings, vector search, safe text-to-SQL", lab="D4_4_data_for_ai",
         runs_on="CPU (optional LLM endpoint)"),
]

TOOL_TITLES = {
    "duckdb": "DuckDB, an analytical database in a library",
    "dlt": "dlt, from a hand-written loader to an incremental pipeline",
    "polars": "Polars, one idea at a time",
    "dbt": "dbt, from an empty folder to a tested report",
    "pandera": "Pandera, checks at the door",
    "iceberg": "Iceberg, open table formats on a lightboard",
    "spark": "Spark, live and visual",
    "dagster": "Dagster, assets on a schedule",
}


def esc(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def a(href: str, text: str) -> str:
    return f'<a href="{href}">{text}</a>'


def html_rows(day: int) -> str:
    title, colour = DAYS[day]
    rows = []
    for s in [s for s in SESSIONS if s["id"].startswith(f"D{day}_")]:
        if "tool" in s:
            t = s["tool"]
            teach = (f'<span title="tool walkthrough, no slides">📓</span> {a(COLAB + "tools/" + t + ".ipynb", t)}'
                     f' <span class="alt">{a("labs/tools/html/" + t + ".html", "read")}</span>')
            if "opens_with" in s:
                o, sec = s["opens_with"]
                teach = teach.replace("</span> ", f'</span> {a(COLAB + "tools/" + o + ".ipynb", o)} {esc(sec)} → ', 1)
        else:
            teach = f'<span title="concept deck">🖥️</span> {a("slides/" + s["deck"] + ".html", "slides")}'
            if "demo" in s:
                teach += f' <span class="alt">{a(COLAB + "demos/" + s["demo"] + ".ipynb", "demo")}</span>'
        lab = f'<a href="{COLAB}{s["lab"]}.ipynb" title="{s["lab"]}.ipynb">Lab</a>'
        if s.get("runs_on", "").startswith("Colab"):
            lab += ' <span class="alt">Colab</span>'
        rows.append(
            f'      <li><div><b>{s["id"]} · {esc(s["title"])}</b><small>{esc(s["topics"])}</small></div>'
            f'<span>{teach}</span><span>{lab}</span>'
            f'<span>{a(COLAB + "solutions/" + s["lab"] + ".ipynb", "Solution")}</span></li>')
    return (f'  <section class="day" style="--c:var(--{colour})">\n'
            f'    <h3><span>Day {day}</span>{esc(title)}</h3>\n'
            f'    <ol>\n' + "\n".join(rows) + "\n    </ol>\n  </section>")


def markdown_rows() -> str:
    lines = ["| Session | Taught from | Lab | Runs on |", "|---|---|---|---|"]
    for s in SESSIONS:
        if "tool" in s:
            t = s["tool"]
            teach = f"📓 [`tools/{t}.ipynb`]({COLAB}tools/{t}.ipynb): {TOOL_TITLES[t]}"
            if "opens_with" in s:
                o, sec = s["opens_with"]
                teach = f"📓 [`tools/{o}.ipynb`]({COLAB}tools/{o}.ipynb) {sec}, then " + teach[2:]
        else:
            teach = f"🖥️ [slides]({PAGES}slides/{s['deck']}.html)"
            if "demo" in s:
                teach += f" + [`demos/{s['demo']}.ipynb`]({COLAB}demos/{s['demo']}.ipynb)"
        runs = s.get("runs_on", "CPU")
        if s["id"] == "D3_2":
            runs = "**Colab** (or Java 17+)"
        lines.append(f"| {s['id']} · {s['title']} | {teach} | [`{s['lab']}.ipynb`]({COLAB}{s['lab']}.ipynb) | {runs} |")
    return "\n".join(lines)


def replace_between(path: Path, begin: str, end: str, content: str) -> None:
    text = path.read_bytes().decode("utf-8")   # not read_text: it would turn CRLF into LF
    pattern = re.compile(re.escape(begin) + r".*?" + re.escape(end), re.S)
    assert len(pattern.findall(text)) == 1, f"markers not found once in {path}"
    eol = "\r\n" if "\r\n" in text else "\n"
    block = (begin + "\n" + content + "\n" + end).replace("\r\n", "\n").replace("\n", eol)
    path.write_bytes(pattern.sub(lambda _: block, text).encode("utf-8"))
    print("wrote", path.relative_to(ROOT))


if __name__ == "__main__":
    replace_between(ROOT / "index.html", "<!-- sessions:begin -->", "<!-- sessions:end -->",
                    "\n\n".join(html_rows(d) for d in DAYS))
    replace_between(LABS / "README.md", "<!-- notebooks:begin -->", "<!-- notebooks:end -->", markdown_rows())
