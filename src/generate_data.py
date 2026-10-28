"""Mock raw-data generator.

Produces
    data/raw/entities.csv        (Source A)
    data/raw/transactions.json   (Source B)

Entity data quality (per 100 records, both guaranteed by construction):
    * 6 exact duplicates of other (valid) records      -> spec minimum is 5
    * 6 malformed e-mail records (never duplicated)    -> spec minimum is 5
The two issues are placed on *different* records so they can be tested
independently. Region strings also carry messy casing / whitespace so that
Silver standardisation has something to do (these still pass validation).

Transaction data quality: negative amounts, null / missing entity_id, invalid
currency, plus a few well-formed transactions pointing at an entity_id that is
not in the entity file (orphans, to exercise the Silver referential check).

Usage:
    python -m src.generate_data [--entities 1000] [--transactions 5000] [--seed 42]

    # a second, later batch for the incremental-processing demo; its transactions also
    # reference entities that already exist in data/raw/entities*.csv
    python -m src.generate_data --suffix _batch2 --entities 200 --transactions 1000 \\
           --seed 99 --include-existing-entities
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from src.config import Paths, default_paths

FIRST_NAMES = ["Aarav", "Maria", "John", "Priya", "Liam", "Sofia", "Chen", "Fatima",
               "Noah", "Emma", "Arjun", "Olivia", "Mateo", "Ananya", "Lucas", "Mei",
               "Omar", "Isabella", "Ethan", "Zara"]
LAST_NAMES = ["Sharma", "Garcia", "Smith", "Patel", "Brown", "Rossi", "Wang", "Khan",
              "Johnson", "Silva", "Kumar", "Miller", "Lopez", "Nair", "Davis", "Tanaka"]
DOMAINS = ["example.com", "mail.com", "corp.io", "inbox.net"]
REGIONS = ["North", "South", "East", "West", "Central", "North-East", "South-West", "North-West"]
CATEGORIES = ["Retail", "Healthcare", "Travel", "Food", "Electronics", "Services"]
CURRENCIES = ["USD", "EUR", "GBP"]
INVALID_CURRENCIES = ["XXX", "usd", "INR", "", "US$"]

ENTITY_FIELDS = ["entity_id", "first_name", "last_name", "contact_email",
                 "registration_date", "region"]

DUPLICATE_RATE = 0.06  # >= 5 per 100
MALFORMED_EMAIL_RATE = 0.06  # >= 5 per 100


def _uuid4(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def _malformed_email(rng: random.Random, first: str) -> str:
    local = first.lower()
    return rng.choice([
        f"{local}.example.com",   # missing @
        f"{local}@",              # missing domain
        "@example.com",           # missing local part
        f"{local}.example",       # missing @ and TLD
    ])


def _messy_region(rng: random.Random, region: str) -> str:
    return rng.choice([region, region.lower(), region.upper(), f" {region} ", region])


def generate_entities(n: int, rng: random.Random, now: datetime) -> list[dict]:
    """Return ``n`` entity rows (shuffled) containing the planted quality issues."""
    n_dup = math.ceil(n * DUPLICATE_RATE)
    n_bad = math.ceil(n * MALFORMED_EMAIL_RATE)
    n_unique = n - n_dup
    if n_unique <= n_bad + n_dup:
        raise ValueError("entity count too small to plant the required quality issues")

    unique: list[dict] = []
    for i in range(n_unique):
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        unique.append({
            "entity_id": _uuid4(rng),
            "first_name": first,
            "last_name": last,
            "contact_email": f"{first.lower()}.{last.lower()}{i}@{rng.choice(DOMAINS)}",
            "registration_date": (now - timedelta(days=rng.randint(0, 1095),
                                                  seconds=rng.randint(0, 86399))
                                  ).replace(microsecond=0).isoformat(),
            "region": _messy_region(rng, rng.choice(REGIONS)),
        })

    # Planted issue 1: malformed e-mails on distinct records.
    bad_idx = set(rng.sample(range(n_unique), n_bad))
    for i in bad_idx:
        unique[i]["contact_email"] = _malformed_email(rng, unique[i]["first_name"])

    # Planted issue 2: exact duplicates, copied only from records that are NOT malformed.
    clean_idx = [i for i in range(n_unique) if i not in bad_idx]
    duplicates = [dict(unique[i]) for i in rng.sample(clean_idx, n_dup)]

    rows = unique + duplicates
    rng.shuffle(rows)
    return rows


def generate_transactions(n: int, entity_ids: list[str], rng: random.Random,
                          now: datetime) -> list[dict]:
    """Return ``n`` transaction dicts; ~7% are invalid, ~1.5% are valid orphans."""
    region_scale = [rng.uniform(40, 160) for _ in REGIONS]  # varied amount levels
    rows: list[dict] = []
    for _ in range(n):
        amount = round(rng.lognormvariate(math.log(rng.choice(region_scale)), 0.6), 2)
        rows.append({
            "transaction_id": _uuid4(rng),
            "entity_id": rng.choice(entity_ids),
            "category": rng.choice(CATEGORIES),
            "transaction_amount": max(amount, 0.01),
            "currency": rng.choices(CURRENCIES, weights=[6, 3, 1])[0],
            # last 90 days up to now, so the *current calendar month* is populated
            "timestamp": (now - timedelta(seconds=rng.randint(0, 90 * 86400))
                          ).replace(microsecond=0).isoformat(),
        })

    def pick(rate: float) -> list[int]:
        return rng.sample(range(n), max(1, round(n * rate)))

    for i in pick(0.02):                       # negative amounts
        rows[i]["transaction_amount"] = -abs(rows[i]["transaction_amount"])
    for k, i in enumerate(pick(0.02)):         # null / missing entity_id
        if k % 2 == 0:
            rows[i]["entity_id"] = None
        else:
            rows[i].pop("entity_id", None)
    for i in pick(0.02):                       # invalid currency
        rows[i]["currency"] = rng.choice(INVALID_CURRENCIES)
    for i in pick(0.015):                      # valid payload, unknown entity (orphan)
        if rows[i].get("entity_id"):
            rows[i]["entity_id"] = _uuid4(rng)
    return rows


def write_entities(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=ENTITY_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def write_transactions(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(rows, fh, indent=2)


def load_existing_entity_ids(raw_dir: Path, exclude: Path | None = None) -> list[str]:
    """Entity ids already present in raw entity files (used to link a new batch to old data)."""
    ids: set[str] = set()
    for path in sorted(raw_dir.glob("entities*.csv")):
        if exclude is not None and path.resolve() == exclude.resolve():
            continue
        with path.open(newline="", encoding="utf-8-sig") as fh:
            ids.update(r["entity_id"] for r in csv.DictReader(fh) if r.get("entity_id"))
    return sorted(ids)


def generate(paths: Paths, n_entities: int = 1000, n_transactions: int = 5000,
             seed: int = 42, now: datetime | None = None, suffix: str = "",
             include_existing_entities: bool = False) -> None:
    """Write ``entities{suffix}.csv`` and ``transactions{suffix}.json`` into the raw dir.

    The default suffix ("") produces the canonical ``entities.csv`` / ``transactions.json``;
    a suffix such as ``_batch2`` produces a later batch for incremental processing.
    """
    rng = random.Random(seed)
    now = now or datetime.now().replace(microsecond=0)
    entities_path = paths.raw_dir / f"entities{suffix}.csv"
    transactions_path = paths.raw_dir / f"transactions{suffix}.json"

    entities = generate_entities(n_entities, rng, now)
    # Transactions reference every generated entity id (including ones whose
    # e-mail is malformed, which will be rejected -> their transactions become orphans).
    entity_ids = {e["entity_id"] for e in entities}
    if include_existing_entities:
        entity_ids |= set(load_existing_entity_ids(paths.raw_dir, exclude=entities_path))
    transactions = generate_transactions(n_transactions, sorted(entity_ids), rng, now)
    write_entities(entities, entities_path)
    write_transactions(transactions, transactions_path)
    print(f"Wrote {len(entities)} entities  -> {entities_path}")
    print(f"Wrote {len(transactions)} transactions -> {transactions_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate mock raw data.")
    parser.add_argument("--entities", type=int, default=1000)
    parser.add_argument("--transactions", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--raw-dir", type=Path, default=None)
    parser.add_argument("--suffix", default="",
                        help="file-name suffix for an additional batch, e.g. _batch2")
    parser.add_argument("--include-existing-entities", action="store_true",
                        help="let this batch's transactions also reference entities from earlier files")
    args = parser.parse_args()
    paths = default_paths()
    if args.raw_dir:
        paths = Paths(args.raw_dir, paths.dlq_dir, paths.output_dir, paths.db_path)
    generate(paths, args.entities, args.transactions, args.seed, suffix=args.suffix,
             include_existing_entities=args.include_existing_entities)


if __name__ == "__main__":
    main()
