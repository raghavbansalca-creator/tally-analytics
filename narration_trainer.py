"""
Narration Training Data Pipeline
- Exports all narrations with current regex classifications
- Provides review/correction interface
- Saves verified labels to SQLite table (_narration_training)
- Exports training dataset for LLM fine-tuning
"""

import sqlite3
import json
import os
from datetime import datetime
from collections import defaultdict

from narration_engine import classify_narration

# ── CATEGORY LIST ────────────────────────────────────────────────────────────

CATEGORIES = [
    "Related Party", "Cash Transactions", "Loan/Advance",
    "Capital Expenditure", "Salary/Wages", "Rent Payments",
    "Professional/Consultancy", "Contractor Payments", "Insurance",
    "Provision/Write-off", "Inter-company/Branch", "Reversal/Correction",
    "Year-end Adjustments", "Suspense/Clearing", "Donation/CSR",
    "Foreign/Forex", "GST Payment", "TDS Payment", "Bank Charges",
    "Sales/Revenue", "Purchase/Material", "Debtor Receipt",
    "Creditor Payment", "Utility/Office", "Travel/Conveyance",
    "Uncategorized",
]


# ── TABLE SETUP ──────────────────────────────────────────────────────────────

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS _narration_training (
    guid TEXT PRIMARY KEY,
    narration TEXT,
    voucher_type TEXT,
    party TEXT,
    amount REAL,
    date TEXT,
    regex_category TEXT,
    human_category TEXT,
    status TEXT DEFAULT 'unreviewed',
    notes TEXT,
    reviewed_at TEXT
);
"""

_CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_nt_status ON _narration_training(status);
"""


def _ensure_table(conn):
    """Create the training table if it doesn't exist."""
    conn.execute(_CREATE_TABLE)
    conn.execute(_CREATE_INDEX)
    conn.commit()


def _format_date(date_str: str) -> str:
    if not date_str or len(date_str) < 8:
        return date_str
    try:
        dt = datetime.strptime(date_str[:8], "%Y%m%d")
        return dt.strftime("%d-%b-%Y")
    except (ValueError, TypeError):
        return date_str


def _primary_category(narration: str, party: str = "") -> tuple[str, float]:
    """Run regex classifier and return (primary_category, confidence).

    Confidence heuristic:
      1.0  if exactly one category matched
      0.7  if multiple categories matched (ambiguous)
      0.3  if no category matched (Uncategorized)
    """
    matches = classify_narration(narration)
    # Filter out meta-categories (No Narration, Unusually Short/Long)
    real = [m for m in matches if m["category"] not in
            ("No Narration", "Unusually Short Narration", "Unusually Long Narration")]

    # Also check party-based cash detection
    if party and party.strip().upper() in (
        "CASH", "CASH A/C", "CASH ACCOUNT", "CASH IN HAND",
        "CASH-IN-HAND", "PETTY CASH",
    ):
        has_cash = any(m["category"] == "Cash Transactions" for m in real)
        if not has_cash:
            real.append({"category": "Cash Transactions"})

    if not real:
        return "Uncategorized", 0.3
    elif len(real) == 1:
        return real[0]["category"], 1.0
    else:
        # Pick highest-severity as primary
        sev_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        real_sorted = sorted(real, key=lambda m: sev_order.get(m.get("severity", "LOW"), 9))
        return real_sorted[0]["category"], 0.7


# ── POPULATE / SYNC ──────────────────────────────────────────────────────────

def sync_training_table(db_path: str) -> int:
    """Populate _narration_training from trn_voucher for any new GUIDs.

    Returns the number of new rows inserted.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _ensure_table(conn)

    # Get existing GUIDs
    existing = set(
        r[0] for r in conn.execute("SELECT guid FROM _narration_training").fetchall()
    )

    rows = conn.execute("""
        SELECT
            v.GUID, v.DATE, v.VOUCHERTYPENAME, v.PARTYLEDGERNAME, v.NARRATION,
            COALESCE(
                (SELECT SUM(ABS(CAST(a.AMOUNT AS REAL)))
                 FROM trn_accounting a
                 WHERE a.VOUCHER_GUID = v.GUID AND CAST(a.AMOUNT AS REAL) > 0),
                0
            ) AS amount
        FROM trn_voucher v
        WHERE v.NARRATION IS NOT NULL AND v.NARRATION != ''
    """).fetchall()

    inserted = 0
    for row in rows:
        guid = row["GUID"]
        if guid in existing:
            continue
        narration = row["NARRATION"] or ""
        party = row["PARTYLEDGERNAME"] or ""
        regex_cat, _ = _primary_category(narration, party)
        conn.execute(
            """INSERT INTO _narration_training
               (guid, narration, voucher_type, party, amount, date, regex_category, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'unreviewed')""",
            (
                guid,
                narration,
                row["VOUCHERTYPENAME"] or "",
                party,
                float(row["amount"] or 0),
                row["DATE"] or "",
                regex_cat,
            ),
        )
        inserted += 1

    conn.commit()
    conn.close()
    return inserted


# ── GENERATE BATCH ───────────────────────────────────────────────────────────

def generate_training_batch(
    db_path: str,
    batch_size: int = 50,
    offset: int = 0,
    filter_status: str = "unreviewed",
    filter_category: str = None,
    min_amount: float = None,
    max_amount: float = None,
) -> list[dict]:
    """Get a batch of narrations for review.

    Returns list of dicts with: guid, date, voucher_type, party, amount,
    narration, regex_category, regex_confidence, human_category, status.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _ensure_table(conn)

    conditions = []
    params = []

    if filter_status and filter_status != "all":
        conditions.append("status = ?")
        params.append(filter_status)
    if filter_category:
        conditions.append("regex_category = ?")
        params.append(filter_category)
    if min_amount is not None:
        conditions.append("amount >= ?")
        params.append(min_amount)
    if max_amount is not None:
        conditions.append("amount <= ?")
        params.append(max_amount)

    where = " AND ".join(conditions) if conditions else "1=1"
    sql = f"""
        SELECT guid, narration, voucher_type, party, amount, date,
               regex_category, human_category, status, notes
        FROM _narration_training
        WHERE {where}
        ORDER BY amount DESC
        LIMIT ? OFFSET ?
    """
    params.extend([batch_size, offset])

    results = []
    for row in conn.execute(sql, params).fetchall():
        narration = row["narration"] or ""
        party = row["party"] or ""
        _, confidence = _primary_category(narration, party)
        results.append({
            "guid": row["guid"],
            "date": _format_date(row["date"] or ""),
            "date_raw": row["date"] or "",
            "voucher_type": row["voucher_type"] or "",
            "party": party,
            "amount": row["amount"] or 0,
            "narration": narration,
            "regex_category": row["regex_category"] or "Uncategorized",
            "regex_confidence": confidence,
            "human_category": row["human_category"] or "",
            "status": row["status"] or "unreviewed",
            "notes": row["notes"] or "",
        })

    conn.close()
    return results


# ── SAVE REVIEW ──────────────────────────────────────────────────────────────

def save_review(db_path: str, guid: str, human_category: str, notes: str = "",
                status: str = None):
    """Save a human-reviewed classification.

    If status is None, it is auto-determined:
      - 'verified' if human_category matches regex_category
      - 'corrected' if they differ
    """
    conn = sqlite3.connect(db_path)
    _ensure_table(conn)

    if status is None:
        row = conn.execute(
            "SELECT regex_category FROM _narration_training WHERE guid = ?", (guid,)
        ).fetchone()
        if row and row[0] == human_category:
            status = "verified"
        else:
            status = "corrected"

    conn.execute(
        """UPDATE _narration_training
           SET human_category = ?, status = ?, notes = ?, reviewed_at = ?
           WHERE guid = ?""",
        (human_category, status, notes, datetime.now().isoformat(), guid),
    )
    conn.commit()
    conn.close()


def save_batch_reviews(db_path: str, reviews: list[dict]):
    """Save multiple reviews at once.

    Each dict should have: guid, human_category, notes (optional).
    """
    conn = sqlite3.connect(db_path)
    _ensure_table(conn)

    # Pre-fetch regex categories for auto-status
    guids = [r["guid"] for r in reviews]
    placeholders = ",".join("?" * len(guids))
    rows = conn.execute(
        f"SELECT guid, regex_category FROM _narration_training WHERE guid IN ({placeholders})",
        guids,
    ).fetchall()
    regex_map = {r[0]: r[1] for r in rows}

    now = datetime.now().isoformat()
    for r in reviews:
        guid = r["guid"]
        human_cat = r["human_category"]
        notes = r.get("notes", "")
        regex_cat = regex_map.get(guid, "")
        status = "verified" if human_cat == regex_cat else "corrected"
        conn.execute(
            """UPDATE _narration_training
               SET human_category = ?, status = ?, notes = ?, reviewed_at = ?
               WHERE guid = ?""",
            (human_cat, status, notes, now, guid),
        )

    conn.commit()
    conn.close()


def skip_narration(db_path: str, guid: str):
    """Mark a narration as skipped."""
    conn = sqlite3.connect(db_path)
    _ensure_table(conn)
    conn.execute(
        """UPDATE _narration_training
           SET status = 'skipped', reviewed_at = ?
           WHERE guid = ?""",
        (datetime.now().isoformat(), guid),
    )
    conn.commit()
    conn.close()


# ── STATS ────────────────────────────────────────────────────────────────────

def get_training_stats(db_path: str) -> dict:
    """Get stats: total, reviewed, verified, corrected, skipped, accuracy, categories."""
    conn = sqlite3.connect(db_path)
    _ensure_table(conn)

    total = conn.execute("SELECT COUNT(*) FROM _narration_training").fetchone()[0]
    verified = conn.execute(
        "SELECT COUNT(*) FROM _narration_training WHERE status = 'verified'"
    ).fetchone()[0]
    corrected = conn.execute(
        "SELECT COUNT(*) FROM _narration_training WHERE status = 'corrected'"
    ).fetchone()[0]
    skipped = conn.execute(
        "SELECT COUNT(*) FROM _narration_training WHERE status = 'skipped'"
    ).fetchone()[0]
    unreviewed = conn.execute(
        "SELECT COUNT(*) FROM _narration_training WHERE status = 'unreviewed'"
    ).fetchone()[0]

    reviewed = verified + corrected
    accuracy = (verified / reviewed * 100) if reviewed > 0 else 0.0

    # Distinct categories in regex
    regex_cats = conn.execute(
        "SELECT COUNT(DISTINCT regex_category) FROM _narration_training"
    ).fetchone()[0]

    # Distinct categories in human labels
    human_cats = conn.execute(
        "SELECT COUNT(DISTINCT human_category) FROM _narration_training WHERE human_category IS NOT NULL AND human_category != ''"
    ).fetchone()[0]

    conn.close()
    return {
        "total": total,
        "reviewed": reviewed,
        "verified": verified,
        "corrected": corrected,
        "skipped": skipped,
        "unreviewed": unreviewed,
        "accuracy": round(accuracy, 1),
        "regex_categories": regex_cats,
        "human_categories": human_cats,
    }


def get_count_by_filter(db_path: str, filter_status: str = "unreviewed",
                        filter_category: str = None) -> int:
    """Get total count matching the current filter."""
    conn = sqlite3.connect(db_path)
    _ensure_table(conn)

    conditions = []
    params = []
    if filter_status and filter_status != "all":
        conditions.append("status = ?")
        params.append(filter_status)
    if filter_category:
        conditions.append("regex_category = ?")
        params.append(filter_category)

    where = " AND ".join(conditions) if conditions else "1=1"
    count = conn.execute(
        f"SELECT COUNT(*) FROM _narration_training WHERE {where}", params
    ).fetchone()[0]
    conn.close()
    return count


# ── EXPORT: JSONL (OpenAI fine-tuning format) ────────────────────────────────

SYSTEM_PROMPT = (
    "You are an expert Indian Chartered Accountant. "
    "Classify the given Tally ERP narration into exactly one category. "
    "Categories: " + ", ".join(CATEGORIES) + ". "
    "Respond with only the category name, nothing else."
)


def export_training_data(db_path: str, output_path: str, format: str = "jsonl") -> int:
    """Export verified training data in JSONL format for fine-tuning.

    Each line: {"messages": [{"role": "system", ...}, {"role": "user", "content": narration},
                             {"role": "assistant", "content": category}]}

    Returns number of examples exported.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _ensure_table(conn)

    rows = conn.execute("""
        SELECT narration, human_category, voucher_type, party, amount
        FROM _narration_training
        WHERE status IN ('verified', 'corrected')
          AND human_category IS NOT NULL AND human_category != ''
    """).fetchall()
    conn.close()

    count = 0
    with open(output_path, "w", encoding="utf-8") as f:
        for row in rows:
            narration = row["narration"] or ""
            category = row["human_category"]
            # Build context-rich user message
            parts = []
            if row["voucher_type"]:
                parts.append(f"Voucher Type: {row['voucher_type']}")
            if row["party"]:
                parts.append(f"Party: {row['party']}")
            if row["amount"]:
                parts.append(f"Amount: {row['amount']:.2f}")
            parts.append(f"Narration: {narration}")
            user_content = "\n".join(parts)

            entry = {
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_content},
                    {"role": "assistant", "content": category},
                ]
            }
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            count += 1

    return count


# ── EXPORT: EXCEL ────────────────────────────────────────────────────────────

def export_training_excel(db_path: str, output_path: str) -> int:
    """Export all narrations with classifications to Excel for bulk review.

    Columns: GUID, Date, Voucher Type, Party, Amount, Narration,
             Regex Category, Human Category, Status, Notes

    Returns number of rows exported.
    """
    import pandas as pd

    conn = sqlite3.connect(db_path)
    _ensure_table(conn)

    df = pd.read_sql_query("""
        SELECT guid AS GUID,
               date AS Date,
               voucher_type AS "Voucher Type",
               party AS Party,
               amount AS Amount,
               narration AS Narration,
               regex_category AS "Regex Category",
               human_category AS "Human Category",
               status AS Status,
               notes AS Notes
        FROM _narration_training
        ORDER BY amount DESC
    """, conn)
    conn.close()

    # Format date for display
    df["Date"] = df["Date"].apply(_format_date)

    df.to_excel(output_path, index=False, sheet_name="Training Data")
    return len(df)


# ── IMPORT: REVIEWED EXCEL ───────────────────────────────────────────────────

def import_reviewed_excel(db_path: str, excel_path: str) -> dict:
    """Import reviewed Excel back -- reads Human Category column.

    Returns: {"updated": N, "skipped": N, "errors": []}
    """
    import pandas as pd

    df = pd.read_excel(excel_path, sheet_name="Training Data")

    # Normalize column names
    col_map = {}
    for col in df.columns:
        lower = col.strip().lower().replace(" ", "_")
        if "guid" in lower:
            col_map[col] = "guid"
        elif "human" in lower and "category" in lower:
            col_map[col] = "human_category"
        elif "notes" in lower:
            col_map[col] = "notes"
    df = df.rename(columns=col_map)

    if "guid" not in df.columns or "human_category" not in df.columns:
        return {"updated": 0, "skipped": 0,
                "errors": ["Excel must have GUID and Human Category columns"]}

    conn = sqlite3.connect(db_path)
    _ensure_table(conn)

    # Pre-fetch regex categories
    all_rows = conn.execute(
        "SELECT guid, regex_category FROM _narration_training"
    ).fetchall()
    regex_map = {r[0]: r[1] for r in all_rows}

    updated = 0
    skipped = 0
    errors = []
    now = datetime.now().isoformat()

    for _, row in df.iterrows():
        guid = str(row.get("guid", "")).strip()
        human_cat = str(row.get("human_category", "")).strip()
        notes = str(row.get("notes", "")).strip() if "notes" in df.columns else ""

        if not guid or not human_cat or human_cat == "nan":
            skipped += 1
            continue
        if human_cat not in CATEGORIES:
            errors.append(f"Invalid category '{human_cat}' for GUID {guid[:12]}...")
            skipped += 1
            continue
        if guid not in regex_map:
            skipped += 1
            continue

        regex_cat = regex_map[guid]
        status = "verified" if human_cat == regex_cat else "corrected"
        conn.execute(
            """UPDATE _narration_training
               SET human_category = ?, status = ?, notes = ?, reviewed_at = ?
               WHERE guid = ?""",
            (human_cat, status, notes, now, guid),
        )
        updated += 1

    conn.commit()
    conn.close()
    return {"updated": updated, "skipped": skipped, "errors": errors}


# ── ACCURACY / CONFUSION ────────────────────────────────────────────────────

def compute_accuracy(db_path: str) -> dict:
    """Compare regex vs human labels.

    Returns:
        overall_accuracy: float (0-100)
        per_category: {category: {tp, fp, fn, precision, recall, f1}}
        confusion: {(regex_cat, human_cat): count}
        top_misclassifications: list of (regex_cat, human_cat, count)
        total_reviewed: int
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _ensure_table(conn)

    rows = conn.execute("""
        SELECT regex_category, human_category
        FROM _narration_training
        WHERE status IN ('verified', 'corrected')
          AND human_category IS NOT NULL AND human_category != ''
    """).fetchall()
    conn.close()

    if not rows:
        return {
            "overall_accuracy": 0.0,
            "per_category": {},
            "confusion": {},
            "top_misclassifications": [],
            "total_reviewed": 0,
        }

    total = len(rows)
    correct = sum(1 for r in rows if r["regex_category"] == r["human_category"])
    overall = correct / total * 100 if total else 0

    # Per-category stats
    tp = defaultdict(int)  # true positives
    fp = defaultdict(int)  # false positives (regex said X, human said Y)
    fn = defaultdict(int)  # false negatives (human said X, regex said Y)

    confusion = defaultdict(int)

    for r in rows:
        rc = r["regex_category"]
        hc = r["human_category"]
        confusion[(rc, hc)] += 1
        if rc == hc:
            tp[rc] += 1
        else:
            fp[rc] += 1  # regex wrongly predicted rc
            fn[hc] += 1  # regex missed hc

    all_cats = set(tp.keys()) | set(fp.keys()) | set(fn.keys())
    per_category = {}
    for cat in sorted(all_cats):
        t = tp[cat]
        f_p = fp[cat]
        f_n = fn[cat]
        precision = t / (t + f_p) * 100 if (t + f_p) > 0 else 0
        recall = t / (t + f_n) * 100 if (t + f_n) > 0 else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
        per_category[cat] = {
            "tp": t, "fp": f_p, "fn": f_n,
            "precision": round(precision, 1),
            "recall": round(recall, 1),
            "f1": round(f1, 1),
        }

    # Top misclassifications (where regex != human)
    misclass = [
        (rc, hc, cnt) for (rc, hc), cnt in confusion.items() if rc != hc
    ]
    misclass.sort(key=lambda x: -x[2])

    return {
        "overall_accuracy": round(overall, 1),
        "per_category": per_category,
        "confusion": dict(confusion),
        "top_misclassifications": misclass[:20],
        "total_reviewed": total,
    }
