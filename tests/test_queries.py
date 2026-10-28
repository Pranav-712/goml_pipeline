"""Verify the analytical SQL against hand-computed answers (not just 'it runs')."""
from datetime import datetime, timedelta

import pytest

from src.pipeline import run_pipeline
from tests.helpers import entity, make_paths, tx, write_entities, write_transactions

TODAY = datetime.now().replace(hour=12, minute=0, second=0, microsecond=0)
LAST_MONTH = TODAY.replace(day=1) - timedelta(days=1)          # always in the previous month
T, L = TODAY.isoformat(), LAST_MONTH.isoformat()


@pytest.fixture(scope="module")
def result(tmp_path_factory):
    paths = make_paths(tmp_path_factory.mktemp("queries"))
    write_entities(paths.raw_dir / "entities.csv", [
        entity(1, "north"), entity(2, " North "), entity(3, "South"), entity(4, "SOUTH"),
        entity(5, "East"), entity(6, "West"), entity(7, "Central"), entity(8, "North-East"),
        entity(9, "West", contact_email="broken@"),               # rejected -> DLQ
    ])
    write_transactions(paths.raw_dir / "transactions.json", [
        tx(1, 1, 100, T, "Retail"), tx(2, 1, 300, T, "Retail"),   # entity 1 avg 200
        tx(3, 2, 50, T, "Food"),                                  # entity 2 avg 50   -> NORTH 125
        tx(4, 3, 400, T, "Food"),                                 # SOUTH 400 (entity 4 has none)
        tx(5, 5, 10, T, "Retail"), tx(6, 5, 30, L, "Retail"),     # EAST avg 20; 30 is LAST month
        tx(7, 6, 500, T, "Travel"),                               # WEST 500
        tx(8, 7, 90, T, "Travel"),                                # CENTRAL 90
        tx(9, 8, 5, T, "Retail"),                                 # NORTH-EAST 5 (6th region)
        tx(10, 1, -5, T),                                         # invalid: negative -> DLQ
        tx(11, 99, 70, T),                                        # orphan: unknown entity
    ])
    return run_pipeline(paths)


def test_dlq_and_orphans_in_fixture(result):
    c = result["counts"]
    assert (c["entities_dlq"], c["transactions_dlq"]) == (1, 1)
    assert c["silver_orphan_transactions"] == 1 and c["fact_transactions"] == 9


def test_q1_current_month_total_per_category(result):
    cols, rows = result["queries"]["q1_monthly_revenue_by_category"]
    assert cols == ["category", "total_transaction_amount"]
    # last month's 30 (Retail) is excluded; the orphan (70) and the DLQ row are excluded.
    assert [(r[0], float(r[1])) for r in rows] == [("Travel", 590.0), ("Food", 450.0), ("Retail", 415.0)]


def test_q2_is_average_of_entity_averages_not_of_transactions(result):
    cols, rows = result["queries"]["q2_top_regions_avg_per_entity"]
    assert cols[:2] == ["region", "average_transaction_amount_per_entity"]
    got = [(r[0], float(r[1])) for r in rows]
    # NORTH: entity averages 200 and 50 -> 125 (a naive per-transaction average would give 150).
    assert got == [("WEST", 500.0), ("SOUTH", 400.0), ("NORTH", 125.0), ("CENTRAL", 90.0), ("EAST", 20.0)]
    assert "NORTH-EAST" not in [g[0] for g in got]               # 6th region is cut by LIMIT 5


def test_q1b_splits_by_currency(result):
    _, rows = result["queries"]["q1b_monthly_revenue_by_category_currency"]
    assert {r[1] for r in rows} == {"USD"} and len(rows) == 3


def test_q2b_ranks_regions_within_currency(result):
    cols, rows = result["queries"]["q2b_top_regions_avg_per_entity_by_currency"]
    assert cols[:3] == ["currency", "region_rank", "region"]
    assert [(r[1], r[2]) for r in rows][:2] == [(1, "WEST"), (2, "SOUTH")]
    assert max(r[1] for r in rows) == 5


def test_q3_month_over_month_growth(result):
    cols, rows = result["queries"]["q3_monthly_revenue_trend_mom"]
    first, current = rows[0], rows[1]
    assert first[cols.index("prev_month_amount")] is None
    assert float(current[cols.index("total_amount")]) == 1455.0 and float(first[cols.index("total_amount")]) == 30.0
    assert float(current[cols.index("mom_growth_pct")]) == 4750.0     # (1455-30)/30


def test_q4_top3_categories_per_region(result):
    cols, rows = result["queries"]["q4_top3_categories_per_region"]
    by_region = {}
    for region, rank, category, n in rows:
        by_region.setdefault(region, []).append((rank, category, n))
    assert all(len(v) <= 3 for v in by_region.values())
    assert by_region["EAST"] == [(1, "Retail", 2)]


def test_q5_top_entities_pareto(result):
    cols, rows = result["queries"]["q5_top10_entities_by_spend_usd"]
    assert rows[0][cols.index("spend_rank")] == 1 and float(rows[0][cols.index("total_spend_usd")]) == 500.0
    assert float(rows[-1][cols.index("cumulative_pct_usd")]) == 100.0  # 7 entities, all within top 10
    assert abs(sum(float(r[cols.index("pct_of_total_usd")]) for r in rows) - 100.0) < 0.05


def test_q6_moving_average_covers_every_calendar_day(result):
    cols, rows = result["queries"]["q6_rolling_7day_revenue_usd"]
    days = (TODAY.date() - LAST_MONTH.date()).days + 1
    assert len(rows) == days
    assert float(rows[0][1]) == 30.0 and float(rows[0][2]) == 30.0     # first window = itself
    assert any(float(r[1]) == 0.0 for r in rows)                       # gap days kept as 0
