#!/usr/bin/env python3
"""
setup_database.py — Execute the consolidated SQL setup script against PostgreSQL.

Usage:
    python scripts/setup_database.py              # uses .env for connection
    python scripts/setup_database.py --dry-run    # prints the SQL without executing
"""

import os
import sys
import argparse
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

SCRIPT_DIR = Path(__file__).resolve().parent
SQL_FILE = SCRIPT_DIR / "setup_database.sql"


def get_db_url() -> str:
    """Build a PostgreSQL connection URL from environment variables."""
    required = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        sys.exit(f"❌ Missing environment variables: {', '.join(missing)}")

    return (
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )


def main():
    parser = argparse.ArgumentParser(description="Set up all database tables and views")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the SQL that would be executed without running it",
    )
    args = parser.parse_args()

    if not SQL_FILE.exists():
        sys.exit(f"❌ SQL file not found: {SQL_FILE}")

    sql = SQL_FILE.read_text()

    if args.dry_run:
        print("── DRY RUN ── SQL that would be executed:\n")
        print(sql)
        return

    # Import sqlalchemy only when actually executing
    from sqlalchemy import create_engine, text

    db_url = get_db_url()
    engine = create_engine(db_url, pool_pre_ping=True)

    print(f"🔌 Connecting to {os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')} ...")

    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
        print("✅ Database setup complete — all tables, indexes, and views created.")
    except Exception as e:
        sys.exit(f"❌ Database setup failed: {e}")


if __name__ == "__main__":
    main()
