"""
Balance Computer — Computes ledger closing balances from transactions.

Primary formula: Closing Balance = Opening Balance + SUM(signed voucher entries)

Tally sign convention:
- In trn_accounting, AMOUNT is signed:
  - Negative amount with ISDEEMEDPOSITIVE=Yes = Debit (e.g., asset increase, expense)
  - Positive amount with ISDEEMEDPOSITIVE=No = Credit (e.g., liability increase, income)
- In mst_ledger, OPENINGBALANCE follows same convention
- Closing Balance = Opening + SUM(all AMOUNT entries for this ledger)

Special Tally-computed ledgers:
- "Profit & Loss A/c": Tally computes this as the balancing figure for the BS.
  It cannot be derived from trn_accounting alone because it includes inventory
  valuation adjustments (closing stock - opening stock).
- Stock-in-Hand ledgers (e.g., "STOCK"): Closing value is computed by Tally
  from inventory movements (trn_inventory), NOT from trn_accounting entries.

Strategy:
- For normal ledgers: Computed CB = Opening + SUM(trn_accounting AMOUNT)
- For Tally-computed ledgers (no transactions, but Tally CB differs from OB):
  Use Tally's CB directly when available, flag as "tally_computed".
- Final COMPUTED_CB = computed value for normal ledgers, Tally CB for special ones.
"""

import logging
import sqlite3

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_cols(conn, table):
    """Return set of column names for a table."""
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {r[1] for r in rows}
    except Exception:
        return set()


def _has_table(conn, table):
    row = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row[0] > 0


def _get_revenue_groups(conn):
    """Get all revenue group names (recursive: includes sub-groups)."""
    if not _has_table(conn, "mst_group"):
        return set()
    rows = conn.execute("""
        WITH RECURSIVE revenue_groups AS (
            SELECT NAME FROM mst_group WHERE ISREVENUE = 'Yes'
            UNION ALL
            SELECT g.NAME FROM mst_group g
            JOIN revenue_groups rg ON g.PARENT = rg.NAME
        )
        SELECT NAME FROM revenue_groups
    """).fetchall()
    return {r[0] for r in rows}


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def compute_all_balances(db_path):
    """Compute closing balances for ALL ledgers from transactions.

    Returns dict: {ledger_name: {
        'opening': float,
        'computed_closing': float,
        'tally_closing': float or None,
        'transaction_sum': float,
        'transaction_count': int,
        'match': bool,  # computed == tally (within Rs 1)
        'difference': float,
        'parent': str,
    }}
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    if not _has_table(conn, "mst_ledger"):
        conn.close()
        return {}

    lcols = _get_cols(conn, "mst_ledger")
    has_cb = "CLOSINGBALANCE" in lcols
    has_ob = "OPENINGBALANCE" in lcols

    # Get all ledgers
    ledgers = conn.execute("SELECT * FROM mst_ledger").fetchall()

    # Get transaction sums per ledger
    txn_data = {}
    if _has_table(conn, "trn_accounting"):
        rows = conn.execute("""
            SELECT LEDGERNAME,
                   SUM(CAST(AMOUNT AS REAL)) as txn_sum,
                   COUNT(*) as txn_count
            FROM trn_accounting
            GROUP BY LEDGERNAME
        """).fetchall()
        for r in rows:
            txn_data[r["LEDGERNAME"]] = {
                "txn_sum": r["txn_sum"] or 0.0,
                "txn_count": r["txn_count"] or 0,
            }

    # Get revenue groups for P&L A/c computation
    revenue_groups = _get_revenue_groups(conn)

    results = {}
    for led in ledgers:
        name = led["NAME"]
        parent = led["PARENT"] if "PARENT" in led.keys() else ""

        # Opening balance
        ob = 0.0
        if has_ob and led["OPENINGBALANCE"]:
            try:
                ob = float(led["OPENINGBALANCE"])
            except (ValueError, TypeError):
                ob = 0.0

        # Tally closing balance
        tally_cb = None
        if has_cb and led["CLOSINGBALANCE"] and led["CLOSINGBALANCE"].strip():
            try:
                tally_cb = float(led["CLOSINGBALANCE"])
            except (ValueError, TypeError):
                tally_cb = None

        # Transaction sum
        td = txn_data.get(name, {"txn_sum": 0.0, "txn_count": 0})
        txn_sum = td["txn_sum"]
        txn_count = td["txn_count"]

        # Computed closing = opening + transactions
        computed = ob + txn_sum

        # Round to avoid floating-point noise (Tally uses 2 decimals)
        computed = round(computed, 2)

        # Detect Tally-computed ledgers:
        # These have no accounting transactions but Tally CB differs from OB.
        # Examples: Stock-in-Hand ledgers, Profit & Loss A/c.
        # For these, Tally's CB is authoritative (derived from inventory or
        # as a balancing figure) and cannot be computed from trn_accounting.
        is_tally_computed = (
            txn_count == 0
            and tally_cb is not None
            and abs(round(tally_cb - ob, 2)) > 0.01
        )

        if is_tally_computed:
            # Use Tally's CB directly — we can't compute this from transactions
            final_cb = tally_cb
            diff = 0.0
            match = True
        else:
            final_cb = computed
            # Match check (within Rs 1 tolerance)
            if tally_cb is not None:
                diff = round(computed - tally_cb, 2)
                match = abs(diff) < 1.0
            else:
                diff = 0.0
                match = True  # no Tally CB to compare against

        results[name] = {
            "opening": ob,
            "computed_closing": final_cb,
            "tally_closing": tally_cb,
            "transaction_sum": txn_sum,
            "transaction_count": txn_count,
            "match": match,
            "difference": diff,
            "parent": parent,
            "tally_computed": is_tally_computed,
        }

    conn.close()
    return results


def update_computed_balances(db_path):
    """Update mst_ledger table with computed closing balances.
    Adds/updates COMPUTED_CB column.
    Also creates _balance_verification table with match results.

    Returns: verification summary dict.
    """
    balances = compute_all_balances(db_path)
    if not balances:
        return {"total_ledgers": 0, "error": "No ledgers found"}

    conn = sqlite3.connect(db_path)

    # Add COMPUTED_CB column if it doesn't exist
    cols = _get_cols(conn, "mst_ledger")
    if "COMPUTED_CB" not in cols:
        conn.execute("ALTER TABLE mst_ledger ADD COLUMN COMPUTED_CB TEXT")

    # Update each ledger
    for name, data in balances.items():
        conn.execute(
            "UPDATE mst_ledger SET COMPUTED_CB = ? WHERE NAME = ?",
            (str(data["computed_closing"]), name),
        )

    # Create verification table
    conn.execute("DROP TABLE IF EXISTS _balance_verification")
    conn.execute("""
        CREATE TABLE _balance_verification (
            NAME TEXT,
            PARENT TEXT,
            OPENING REAL,
            TRANSACTION_SUM REAL,
            TRANSACTION_COUNT INTEGER,
            COMPUTED_CB REAL,
            TALLY_CB REAL,
            DIFFERENCE REAL,
            MATCH TEXT
        )
    """)

    for name, data in balances.items():
        conn.execute(
            """INSERT INTO _balance_verification
               (NAME, PARENT, OPENING, TRANSACTION_SUM, TRANSACTION_COUNT,
                COMPUTED_CB, TALLY_CB, DIFFERENCE, MATCH)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                name,
                data["parent"],
                data["opening"],
                data["transaction_sum"],
                data["transaction_count"],
                data["computed_closing"],
                data["tally_closing"],
                data["difference"],
                "Yes" if data["match"] else "No",
            ),
        )

    # Update metadata
    try:
        import datetime
        conn.execute(
            "INSERT OR REPLACE INTO _metadata (key, value) VALUES (?, ?)",
            ("balance_computed_at", datetime.datetime.now().isoformat()),
        )
    except Exception:
        pass

    conn.commit()
    conn.close()

    # Return verification summary
    return verify_balances(db_path)


def verify_balances(db_path):
    """Run verification and return summary.

    Returns:
    {
        'total_ledgers': int,
        'with_tally_cb': int,
        'without_tally_cb': int,
        'computed_count': int,
        'matched': int,
        'mismatched': int,
        'mismatch_details': [{name, parent, computed, tally, diff}],
        'pl_profit_computed': float,
        'pl_profit_tally': float,
        'bs_balanced': bool,
    }
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    result = {
        "total_ledgers": 0,
        "with_tally_cb": 0,
        "without_tally_cb": 0,
        "computed_count": 0,
        "matched": 0,
        "mismatched": 0,
        "mismatch_details": [],
        "pl_profit_computed": 0.0,
        "pl_profit_tally": 0.0,
        "bs_balanced": False,
    }

    if not _has_table(conn, "_balance_verification"):
        conn.close()
        return result

    # Total ledgers
    row = conn.execute("SELECT COUNT(*) FROM _balance_verification").fetchone()
    result["total_ledgers"] = row[0]

    # With/without Tally CB
    row = conn.execute(
        "SELECT COUNT(*) FROM _balance_verification WHERE TALLY_CB IS NOT NULL"
    ).fetchone()
    result["with_tally_cb"] = row[0]
    result["without_tally_cb"] = result["total_ledgers"] - result["with_tally_cb"]

    # Computed count (have a non-zero computed CB or have transactions)
    row = conn.execute(
        "SELECT COUNT(*) FROM _balance_verification WHERE COMPUTED_CB != 0 OR TRANSACTION_COUNT > 0"
    ).fetchone()
    result["computed_count"] = row[0]

    # Match stats (only where Tally CB exists)
    row = conn.execute(
        "SELECT COUNT(*) FROM _balance_verification WHERE TALLY_CB IS NOT NULL AND MATCH = 'Yes'"
    ).fetchone()
    result["matched"] = row[0]

    row = conn.execute(
        "SELECT COUNT(*) FROM _balance_verification WHERE TALLY_CB IS NOT NULL AND MATCH = 'No'"
    ).fetchone()
    result["mismatched"] = row[0]

    # Mismatch details
    rows = conn.execute("""
        SELECT NAME, PARENT, COMPUTED_CB, TALLY_CB, DIFFERENCE
        FROM _balance_verification
        WHERE TALLY_CB IS NOT NULL AND MATCH = 'No'
        ORDER BY ABS(DIFFERENCE) DESC
    """).fetchall()
    result["mismatch_details"] = [
        {
            "name": r["NAME"],
            "parent": r["PARENT"],
            "computed": r["COMPUTED_CB"],
            "tally": r["TALLY_CB"],
            "diff": r["DIFFERENCE"],
        }
        for r in rows
    ]

    # P&L profit
    pl_row = conn.execute(
        "SELECT COMPUTED_CB, TALLY_CB FROM _balance_verification WHERE NAME = 'Profit & Loss A/c'"
    ).fetchone()
    if pl_row:
        result["pl_profit_computed"] = pl_row["COMPUTED_CB"] or 0.0
        result["pl_profit_tally"] = pl_row["TALLY_CB"] or 0.0

    # Balance sheet check: BS ledgers (non-revenue) should sum to ~0
    # Revenue group ledgers are P&L items; their net is captured in P&L A/c
    # so including both would double-count.
    revenue_groups = set()
    try:
        rg_rows = conn.execute("""
            WITH RECURSIVE revenue_groups AS (
                SELECT NAME FROM mst_group WHERE ISREVENUE = 'Yes'
                UNION ALL
                SELECT g.NAME FROM mst_group g
                JOIN revenue_groups rg ON g.PARENT = rg.NAME
            )
            SELECT NAME FROM revenue_groups
        """).fetchall()
        revenue_groups = {r[0] for r in rg_rows}
    except Exception:
        pass

    if revenue_groups:
        ph = ", ".join(["?"] * len(revenue_groups))
        row = conn.execute(
            f"SELECT COALESCE(SUM(COMPUTED_CB), 0) FROM _balance_verification WHERE PARENT NOT IN ({ph})",
            list(revenue_groups),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT COALESCE(SUM(COMPUTED_CB), 0) FROM _balance_verification"
        ).fetchone()
    total_sum = row[0]
    result["bs_balanced"] = abs(total_sum) < 1.0

    conn.close()
    return result


def get_ledger_balance(db_path, ledger_name):
    """Get a single ledger's computed balance with details."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    cols = _get_cols(conn, "mst_ledger")
    if "COMPUTED_CB" not in cols:
        # Balances not yet computed, compute on the fly
        conn.close()
        balances = compute_all_balances(db_path)
        return balances.get(ledger_name)

    row = conn.execute(
        "SELECT * FROM mst_ledger WHERE NAME = ?", (ledger_name,)
    ).fetchone()
    if not row:
        conn.close()
        return None

    ob = 0.0
    try:
        ob = float(row["OPENINGBALANCE"]) if row["OPENINGBALANCE"] else 0.0
    except (ValueError, TypeError):
        pass

    computed = 0.0
    try:
        computed = float(row["COMPUTED_CB"]) if row["COMPUTED_CB"] else 0.0
    except (ValueError, TypeError):
        pass

    tally_cb = None
    if "CLOSINGBALANCE" in cols and row["CLOSINGBALANCE"] and row["CLOSINGBALANCE"].strip():
        try:
            tally_cb = float(row["CLOSINGBALANCE"])
        except (ValueError, TypeError):
            pass

    conn.close()

    diff = round(computed - tally_cb, 2) if tally_cb is not None else 0.0
    return {
        "opening": ob,
        "computed_closing": computed,
        "tally_closing": tally_cb,
        "transaction_sum": round(computed - ob, 2),
        "match": abs(diff) < 1.0 if tally_cb is not None else True,
        "difference": diff,
    }


# ---------------------------------------------------------------------------
# Convenience: column-aware balance accessor for downstream code
# ---------------------------------------------------------------------------

def get_balance_column(conn):
    """Return the best available balance column name for SQL queries.
    Use COMPUTED_CB if available, else fall back to CLOSINGBALANCE.
    """
    cols = _get_cols(conn, "mst_ledger")
    if "COMPUTED_CB" in cols:
        return "COMPUTED_CB"
    return "CLOSINGBALANCE"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Compute and verify ledger balances")
    parser.add_argument("--db", default="tally_data.db", help="SQLite database path")
    parser.add_argument("--compute", action="store_true", help="Compute and store balances")
    parser.add_argument("--verify", action="store_true", help="Verify balances")
    parser.add_argument("--ledger", help="Get balance for a specific ledger")
    args = parser.parse_args()

    if args.compute:
        print("Computing balances...")
        result = update_computed_balances(args.db)
        print(f"\nVerification Summary:")
        print(f"  Total ledgers:    {result['total_ledgers']}")
        print(f"  With Tally CB:    {result['with_tally_cb']}")
        print(f"  Without Tally CB: {result['without_tally_cb']}")
        print(f"  Matched:          {result['matched']}")
        print(f"  Mismatched:       {result['mismatched']}")
        print(f"  P&L Computed:     {result['pl_profit_computed']:,.2f}")
        print(f"  P&L Tally:        {result['pl_profit_tally']:,.2f}")
        print(f"  BS Balanced:      {result['bs_balanced']}")
        if result["mismatch_details"]:
            print(f"\nMismatches:")
            for m in result["mismatch_details"][:10]:
                print(f"  {m['name']}: computed={m['computed']:,.2f}, tally={m['tally']:,.2f}, diff={m['diff']:,.2f}")

    elif args.verify:
        result = verify_balances(args.db)
        print(json.dumps(result, indent=2, default=str))

    elif args.ledger:
        result = get_ledger_balance(args.db, args.ledger)
        if result:
            print(json.dumps(result, indent=2))
        else:
            print(f"Ledger '{args.ledger}' not found")

    else:
        parser.print_help()
