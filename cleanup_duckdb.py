"""Remove unused tables from the DuckDB warehouse."""

from __future__ import annotations

from pathlib import Path
import duckdb

DBT_DIR = Path(__file__).parent / "dbt"
DB_PATH = Path(__file__).parent / "data" / "warehouse.duckdb"


def list_models() -> set[str]:
    """Return model names based on SQL files under ``dbt/models``."""
    model_dir = DBT_DIR / "models"
    return {p.stem for p in model_dir.rglob("*.sql")}


def list_seeds() -> set[str]:
    """Return seed names based on files under ``dbt/seeds``."""
    seed_dir = DBT_DIR / "seeds"
    seeds = set()
    for p in seed_dir.rglob("*.*"):
        if p.suffix.lower() in {".csv", ".tsv", ".parquet"}:
            seeds.add(p.stem)
    return seeds


def used_tables() -> set[str]:
    """Set of table names managed by dbt models or seeds."""
    return list_models() | list_seeds()


def db_tables(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    return {r[0] for r in rows}


def cleanup() -> None:
    con = duckdb.connect(DB_PATH)
    managed = used_tables()
    existing = db_tables(con)
    for table in sorted(existing - managed):
        con.execute(f'DROP TABLE "{table}"')
        print(f"Dropped {table}")
    con.close()


if __name__ == "__main__":
    cleanup()
