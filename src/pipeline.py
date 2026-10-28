"""End-to-end ELT pipeline.

    landed files -> Pydantic validation (-> DLQ) -> Bronze -> Silver -> Gold -> analytics + DQ report

Modes
    full         (default) reset the warehouse and rebuild every layer from all landed files
    incremental  process only files not seen before (tracked by content hash in ``etl_batches``);
                 Silver/Gold are MERGED, surrogate keys stay stable, late-arriving entities are
                 matched to earlier orphan transactions

A full refresh is implemented as "reset + the incremental logic from empty", so both modes share
one code path (and the tests prove they converge to the same warehouse).

Usage
    python -m src.pipeline                       # full refresh from data/raw
    python -m src.pipeline --generate            # (re)generate mock data first
    python -m src.pipeline --mode incremental    # only new files
    python -m src.pipeline --log-format json --strict
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from src import quality, warehouse
from src.config import DEFAULT_MAX_INVALID_RATIO, Paths, default_paths
from src.generate_data import generate
from src.ingestion import discover_batches, process_batch
from src.logging_config import configure_logging, new_run_id, stage

log = logging.getLogger("pipeline")
MODES = ("full", "incremental")
PRINT_ROWS = 12


def run_pipeline(paths: Paths, mode: str = "full",
                 max_invalid_ratio: float = DEFAULT_MAX_INVALID_RATIO) -> dict:
    """Run all stages; return run metadata, row counts, query results and DQ results."""
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}, got {mode!r}")
    run_id = new_run_id()
    started = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    log.info("run started", extra={"mode": mode, "db_path": str(paths.db_path), "raw_dir": str(paths.raw_dir)})

    batches = discover_batches(paths.raw_dir)
    if not batches:
        raise FileNotFoundError(f"no entities*.csv / transactions*.json files found in {paths.raw_dir}")

    con = warehouse.connect(paths.db_path)
    try:
        if mode == "full":
            warehouse.reset_warehouse(con)
        warehouse.init_schema(con)
        already_loaded = warehouse.processed_hashes(con)

        loaded, skipped = [], []
        for batch in batches:
            if batch.sha256 in already_loaded:
                skipped.append(batch.path.name)
                log.info("batch skipped (already loaded)", extra={"file": batch.path.name, "batch_id": batch.batch_id})
                continue
            with stage(log, "ingest", source=batch.source, file=batch.path.name):
                result, rows_read = process_batch(batch, paths.dlq_dir)
            with stage(log, "load_bronze", source=batch.source, file=batch.path.name):
                warehouse.load_bronze_batch(con, batch, result, rows_read)
            loaded.append(batch.path.name)

        with stage(log, "transform"):
            transformed = warehouse.transform_pending(con)
        with stage(log, "analytics"):
            queries = warehouse.run_all_queries(con, paths.output_dir)
        with stage(log, "data_quality"):
            dq = quality.run_checks(con, run_id, max_invalid_ratio)
        counts = warehouse.table_counts(con)
    finally:
        con.close()

    summary = {
        "run_id": run_id, "mode": mode,
        "started_at": started.isoformat(timespec="seconds"),
        "duration_s": round(time.perf_counter() - t0, 2),
        "batches_loaded": loaded, "batches_skipped": skipped,
        "batches_transformed": len(transformed),
        "counts": counts, "data_quality": quality.summarize(dq),
    }
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    (paths.output_dir / "run_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    log.info("run finished", extra={"duration_s": summary["duration_s"], **summary["data_quality"]})
    return {**summary, "queries": queries, "dq": dq}


def format_table(cols: list[str], rows: list[tuple], max_rows: int | None = None) -> str:
    shown = rows if max_rows is None else rows[:max_rows]
    cells = [[str(c) for c in cols]] + [[str(v) for v in r] for r in shown]
    widths = [max(len(r[i]) for r in cells) for i in range(len(cols))]
    line = lambda r: " | ".join(v.ljust(w) for v, w in zip(r, widths))
    out = [line(cells[0]), "-+-".join("-" * w for w in widths)] + [line(r) for r in cells[1:]]
    if max_rows is not None and len(rows) > max_rows:
        out.append(f"... {len(rows) - max_rows} more rows (full result in the CSV under data/output/)")
    return "\n".join(out)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the local medallion ELT pipeline.")
    parser.add_argument("--mode", choices=MODES, default="full")
    parser.add_argument("--generate", action="store_true", help="generate the base mock data first")
    parser.add_argument("--strict", action="store_true", help="exit 1 if any data-quality ERROR check fails")
    parser.add_argument("--max-invalid-ratio", type=float, default=DEFAULT_MAX_INVALID_RATIO,
                        help="DLQ circuit-breaker threshold (default %(default)s)")
    parser.add_argument("--log-format", choices=("text", "json"), default="text")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--raw-dir", type=Path)
    parser.add_argument("--dlq-dir", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--db-path", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_format, args.log_level)

    d = default_paths()
    paths = Paths(args.raw_dir or d.raw_dir, args.dlq_dir or d.dlq_dir,
                  args.output_dir or d.output_dir, args.db_path or d.db_path)
    if args.generate:
        generate(paths)

    result = run_pipeline(paths, mode=args.mode, max_invalid_ratio=args.max_invalid_ratio)

    print(f"\n== Run {result['run_id']} ({result['mode']}) - loaded {len(result['batches_loaded'])} batch(es), "
          f"skipped {len(result['batches_skipped'])} ==")
    print("\n== Row counts ==")
    for name, n in result["counts"].items():
        print(f"{name:28s} {n}")
    for name, (cols, rows) in result["queries"].items():
        print(f"\n== {name} ==")
        print(format_table(cols, rows, PRINT_ROWS) if rows else "(no rows)")

    dq_rows = [(r["check_name"], r["severity"], r["status"], r["failed_rows"], r["total_rows"]) for r in result["dq"]]
    print("\n== Data quality ==")
    print(format_table(["check", "severity", "status", "failed", "total"], dq_rows))
    s = result["data_quality"]
    print(f"\n{s['passed']}/{s['checks']} checks passed, {s['failed_errors']} errors, {s['failed_warnings']} warnings")

    if args.strict and quality.has_errors(result["dq"]):
        log.error("strict mode: data-quality errors present")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
