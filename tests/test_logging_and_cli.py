"""Structured logging, configuration overrides and the command-line entry point."""
import io
import json
import logging
from datetime import datetime

import pytest

from src import config
from src.generate_data import generate
from src.logging_config import JsonFormatter, RunIdFilter, new_run_id, stage
from src.pipeline import main
from tests.helpers import make_paths


def _logger(fmt):
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.addFilter(RunIdFilter())
    handler.setFormatter(fmt)
    logger = logging.getLogger(f"test.{id(stream)}")
    logger.handlers, logger.propagate = [handler], False
    logger.setLevel(logging.DEBUG)
    return logger, stream


def test_json_log_lines_are_valid_json_with_run_id_and_extra_fields():
    run_id = new_run_id()
    logger, stream = _logger(JsonFormatter())
    logger.info("batch loaded", extra={"stage": "load_bronze", "rows": 940})
    record = json.loads(stream.getvalue())
    assert record["message"] == "batch loaded" and record["level"] == "INFO"
    assert record["run_id"] == run_id and record["stage"] == "load_bronze" and record["rows"] == 940


def test_stage_logs_start_finish_and_duration():
    logger, stream = _logger(JsonFormatter())
    with stage(logger, "transform", batches=2):
        pass
    start, finish = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert (start["event"], finish["event"]) == ("start", "finish")
    assert finish["stage"] == "transform" and finish["batches"] == 2 and finish["duration_ms"] >= 0


def test_stage_logs_the_error_and_reraises():
    logger, stream = _logger(JsonFormatter())
    with pytest.raises(ValueError):
        with stage(logger, "transform"):
            raise ValueError("boom")
    last = json.loads(stream.getvalue().splitlines()[-1])
    assert last["level"] == "ERROR" and last["event"] == "error" and "boom" in last["exception"]


def test_paths_are_configurable_through_environment_variables(monkeypatch, tmp_path):
    monkeypatch.setenv("PIPELINE_DATA_DIR", str(tmp_path / "d"))
    monkeypatch.setenv("PIPELINE_DB_PATH", str(tmp_path / "x.duckdb"))
    paths = config.default_paths()
    assert paths.raw_dir == tmp_path / "d" / "raw" and paths.dlq_dir == tmp_path / "d" / "dlq"
    assert paths.db_path == tmp_path / "x.duckdb"
    assert paths.invalid_entities_json.name == "invalid_entities.json"


def _cli_args(paths, *extra):
    return ["--raw-dir", str(paths.raw_dir), "--dlq-dir", str(paths.dlq_dir),
            "--output-dir", str(paths.output_dir), "--db-path", str(paths.db_path), *extra]


def test_cli_runs_end_to_end_and_writes_outputs(tmp_path, capsys):
    paths = make_paths(tmp_path)
    generate(paths, 100, 300, seed=4, now=datetime.now().replace(microsecond=0))
    assert main(_cli_args(paths, "--log-format", "json")) == 0
    out = capsys.readouterr().out
    assert "== q1_monthly_revenue_by_category ==" in out and "checks passed" in out
    assert (paths.output_dir / "q2_top_regions_avg_per_entity.csv").exists()
    assert json.loads((paths.output_dir / "run_summary.json").read_text())["mode"] == "full"


def test_cli_strict_mode_exits_nonzero_when_an_error_check_fails(tmp_path):
    paths = make_paths(tmp_path)
    generate(paths, 100, 300, seed=4, now=datetime.now().replace(microsecond=0))   # ~6% DLQ rows
    assert main(_cli_args(paths, "--max-invalid-ratio", "0.01")) == 0               # reported, not fatal
    assert main(_cli_args(paths, "--max-invalid-ratio", "0.01", "--strict")) == 1


def test_missing_input_files_give_a_clear_error(tmp_path):
    with pytest.raises(FileNotFoundError, match="entities"):
        main(_cli_args(make_paths(tmp_path)))
