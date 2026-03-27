"""
TDS Analysis Engine -- Dynamic (works for any company)
Auto-detects TDS ledgers from the database.
Extracts TDS data from Tally SQLite database for section-wise,
party-wise, monthly, and quarterly analysis.
"""

import sqlite3
import os
import re
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), "tally_data.db")


def get_conn():
    return sqlite3.connect(DB_PATH)


def _safe_float(val):
    """Convert a value to float, returning 0.0 on failure."""
    if val is None or val == "":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _has_column(conn, table_name, column_name):
    """Check if a column exists in a table."""
    try:
        cols = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return any(c[1].upper() == column_name.upper() for c in cols)
    except Exception:
        return False


def _format_date(d):
    """Convert YYYYMMDD to DD-MM-YYYY."""
    if d and len(d) == 8:
        return f"{d[6:8]}-{d[4:6]}-{d[0:4]}"
    return d or ""


def _month_label(m):
    """Convert YYYYMM to readable label."""
    month_names = {
        "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
        "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
        "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec",
    }
    if m and len(m) == 6:
        return f"{month_names.get(m[4:6], m[4:6])} {m[0:4]}"
    return m or ""


def format_indian(amount):
    """Format number in Indian numbering system (e.g., 12,34,567.89)."""
    if amount is None:
        return "0.00"
    try:
        amount = float(amount)
    except (ValueError, TypeError):
        return "0.00"

    is_negative = amount < 0
    amount = abs(amount)

    int_part = int(amount)
    dec_part = f"{amount - int_part:.2f}"[1:]

    s = str(int_part)
    if len(s) <= 3:
        result = s
    else:
        result = s[-3:]
        s = s[:-3]
        while s:
            result = s[-2:] + "," + result
            s = s[:-2]

    result = result + dec_part
    if is_negative:
        result = "-" + result

    return result


# ── TDS SECTION REFERENCE ──────────────────────────────────────────────────

TDS_SECTIONS = {
    "192":    {"description": "Salary",                     "rate": None,  "threshold": None},
    "194A":   {"description": "Interest (non-securities)",  "rate": 10.0,  "threshold": 40000},
    "194C":   {"description": "Contractor payments",        "rate": 2.0,   "threshold": 100000},
    "194H":   {"description": "Commission / Brokerage",     "rate": 5.0,   "threshold": 15000},
    "194I(a)":{"description": "Rent - Plant & Machinery",   "rate": 2.0,   "threshold": 240000},
    "194I(b)":{"description": "Rent - Land & Building",     "rate": 10.0,  "threshold": 240000},
    "194I":   {"description": "Rent",                       "rate": 10.0,  "threshold": 240000},
    "194J":   {"description": "Professional / Technical fees","rate": 10.0, "threshold": 30000},
    "194Q":   {"description": "Purchase of goods",          "rate": 0.1,   "threshold": 5000000},
    "194N":   {"description": "Cash withdrawal",            "rate": 2.0,   "threshold": 10000000},
    "194O":   {"description": "E-commerce operator",        "rate": 1.0,   "threshold": 500000},
    "206C":   {"description": "TCS on sales",               "rate": None,  "threshold": None},
    "Other":  {"description": "Other TDS / General",        "rate": None,  "threshold": None},
}


# ── CLASSIFICATION ─────────────────────────────────────────────────────────

def _classify_tds_section(ledger_name):
    """Map a TDS ledger name to its section code using keyword matching."""
    if not ledger_name:
        return "Other"
    upper = ledger_name.upper()

    # Try direct section number match first (e.g., "TDS 194C", "194J Payable")
    section_match = re.search(r'(206C|194[A-Z]?\(?[A-Za-z]?\)?|192)', upper)
    if section_match:
        raw = section_match.group(1).replace("(", "(").replace(")", ")")
        # Normalize: "194IA" -> "194I(a)", "194IB" -> "194I(b)"
        m2 = re.match(r'194I\(?([AB])\)?', raw, re.IGNORECASE)
        if m2:
            sub = m2.group(1).lower()
            return f"194I({sub})"
        # "194C", "194J", etc.
        normalized = raw.upper()
        if normalized in TDS_SECTIONS:
            return normalized
        # Try without parentheses
        cleaned = normalized.replace("(", "").replace(")", "")
        for k in TDS_SECTIONS:
            if k.replace("(", "").replace(")", "") == cleaned:
                return k
        return normalized

    # Keyword-based classification
    if "TCS" in upper or "COLLECTED AT SOURCE" in upper:
        return "206C"
    if "SALARY" in upper or "SALARIES" in upper:
        return "192"
    if "CONTRACTOR" in upper or "CONTRACT" in upper:
        return "194C"
    if "PROFESSIONAL" in upper or "TECHNICAL" in upper or "PROFESSION" in upper:
        return "194J"
    if "COMMISSION" in upper or "BROKERAGE" in upper:
        return "194H"
    if "RENT" in upper:
        if "MACHINE" in upper or "PLANT" in upper or "EQUIPMENT" in upper:
            return "194I(a)"
        return "194I(b)"
    if "INTEREST" in upper:
        return "194A"
    if "CASH WITHDRAWAL" in upper:
        return "194N"
    if "E-COMMERCE" in upper or "ECOMMERCE" in upper:
        return "194O"
    if "PURCHASE" in upper and "GOOD" in upper:
        return "194Q"

    # Rate-based inference: extract percentage from ledger name
    rate_match = re.search(r'@?\s*(\d+\.?\d*)\s*%', upper)
    if rate_match:
        rate = float(rate_match.group(1))
        # Map common TDS rates to sections
        if rate == 0.1:
            return "194Q"  # Purchase of goods
        elif rate == 1.0:
            return "194C"  # Contractor (individual)
        elif rate == 2.0:
            return "194C"  # Contractor (non-individual) or 194I(a) rent
        elif rate == 5.0:
            return "194H"  # Commission/brokerage (or 194IA property)
        elif rate == 7.5:
            return "194J"  # Professional/technical (reduced COVID rate)
        elif rate == 10.0:
            return "194J"  # Professional/technical (standard rate)
        elif rate == 20.0:
            return "194J"  # Without PAN rate
        elif rate == 0.075:
            return "194Q"  # Reduced rate during COVID

    # Payable + parent-based context: if parent suggests provisions and name is generic
    if "PAYABLE" in upper and rate_match:
        return "194Q" if float(rate_match.group(1)) <= 0.1 else "Other"

    return "Other"


def _classify_tds_category(parent, upper_parent):
    """Classify a TDS ledger as receivable, payable, or expense based on parent group.

    - receivable: TDS deducted BY customers on our income (asset — under Deposits, Loans & Advances)
    - payable: TDS deducted BY us on payments (liability — under Provisions, Current Liabilities, Duties)
    - expense: TDS cost (under Indirect/Direct Expenses)
    """
    # Asset-side parents → receivable
    asset_keywords = ["DEPOSIT", "LOAN", "ADVANCE", "ASSET", "RECEIVABLE", "CURRENT ASSET"]
    if any(kw in upper_parent for kw in asset_keywords):
        return "receivable"

    # Liability-side parents → payable
    liability_keywords = ["PROVISION", "PAYABLE", "LIABILIT", "DUTI", "TAX"]
    if any(kw in upper_parent for kw in liability_keywords):
        return "payable"

    # Expense-side parents → expense
    expense_keywords = ["EXPENSE", "INDIRECT", "DIRECT"]
    if any(kw in upper_parent for kw in expense_keywords):
        return "expense"

    return "unknown"


# ── TDS LEDGER DETECTION ───────────────────────────────────────────────────

def _detect_tds_ledgers(conn):
    """Auto-detect TDS ledgers from mst_ledger.
    Returns list of dicts: [{"name": ..., "parent": ..., "section": ...}, ...]
    """
    cache_key = "_tds_ledger_cache"
    if hasattr(_detect_tds_ledgers, cache_key):
        return getattr(_detect_tds_ledgers, cache_key)

    result = []
    try:
        rows = conn.execute(
            "SELECT name, parent FROM mst_ledger ORDER BY name"
        ).fetchall()
    except Exception:
        return result

    for name, parent in rows:
        if not name:
            continue
        upper_name = name.upper()
        upper_parent = (parent or "").upper()

        is_duty = "DUTI" in upper_parent or "TAX" in upper_parent

        is_tds = False
        # TDS ledger detection
        if "TDS" in upper_name or "TAX DEDUCTED" in upper_name:
            is_tds = True
        elif "TCS" in upper_name or "TAX COLLECTED" in upper_name:
            is_tds = True
        elif is_duty and ("194" in upper_name or "192" in upper_name or "206C" in upper_name):
            is_tds = True

        if is_tds:
            section = _classify_tds_section(name)
            # Classify by parent group: receivable (asset), payable (liability), or expense
            category = _classify_tds_category(parent or "", upper_parent)
            result.append({
                "name": name,
                "parent": parent or "",
                "section": section,
                "category": category,
            })

    setattr(_detect_tds_ledgers, cache_key, result)
    return result


def _clear_tds_cache():
    """Clear the cached TDS ledger detection (call after re-sync)."""
    cache_key = "_tds_ledger_cache"
    if hasattr(_detect_tds_ledgers, cache_key):
        delattr(_detect_tds_ledgers, cache_key)


def _tds_ledger_names(conn):
    """Return set of all TDS ledger names."""
    return {ld["name"] for ld in _detect_tds_ledgers(conn)}


def _tds_ledger_section_map(conn):
    """Return dict: ledger_name -> section code."""
    return {ld["name"]: ld["section"] for ld in _detect_tds_ledgers(conn)}


def _get_company_name(conn):
    """Get company name from metadata."""
    try:
        row = conn.execute("SELECT value FROM _metadata WHERE key = 'company_name'").fetchone()
        return row[0] if row else ""
    except Exception:
        return ""


# ── AVAILABLE MONTHS ───────────────────────────────────────────────────────

def get_tds_available_months(conn=None):
    """Return list of (YYYYMM, display_label) for months with TDS transactions."""
    close = False
    if conn is None:
        conn = get_conn()
        close = True

    tds_names = _tds_ledger_names(conn)
    if not tds_names:
        if close:
            conn.close()
        return []

    placeholders = ",".join(["?"] * len(tds_names))
    rows = conn.execute(f"""
        SELECT DISTINCT SUBSTR(v.DATE, 1, 6) as month
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        WHERE a.LEDGERNAME IN ({placeholders})
        ORDER BY month
    """, list(tds_names)).fetchall()

    if close:
        conn.close()

    months = []
    month_names = {
        "01": "January", "02": "February", "03": "March", "04": "April",
        "05": "May", "06": "June", "07": "July", "08": "August",
        "09": "September", "10": "October", "11": "November", "12": "December",
    }
    for (m,) in rows:
        if not m or len(m) < 6:
            continue
        yyyy = m[:4]
        mm = m[4:6]
        label = f"{month_names.get(mm, mm)} {yyyy}"
        months.append((m, label))

    return months


# ── TDS SUMMARY BY SECTION ────────────────────────────────────────────────

def tds_summary_by_section(conn, date_from=None, date_to=None):
    """Total TDS deducted grouped by section.
    Returns list of dicts: [{"section", "description", "parties", "tds_amount"}, ...]
    """
    tds_names = _tds_ledger_names(conn)
    section_map = _tds_ledger_section_map(conn)
    if not tds_names:
        return []

    placeholders = ",".join(["?"] * len(tds_names))
    date_filter = _build_date_filter(date_from, date_to)

    rows = conn.execute(f"""
        SELECT a.LEDGERNAME,
               v.PARTYLEDGERNAME,
               ABS(SUM(CAST(a.AMOUNT AS REAL))) as tds_amount
        FROM trn_accounting a
        JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
        WHERE a.LEDGERNAME IN ({placeholders})
          {date_filter}
        GROUP BY a.LEDGERNAME, v.PARTYLEDGERNAME
    """, list(tds_names)).fetchall()

    # Aggregate by section
    section_data = defaultdict(lambda: {"tds_amount": 0.0, "parties": set()})
    for ledger, party, amt in rows:
        section = section_map.get(ledger, "Other")
        section_data[section]["tds_amount"] += _safe_float(amt)
        if party:
            section_data[section]["parties"].add(party)

    results = []
    for section in sorted(section_data.keys()):
        info = TDS_SECTIONS.get(section, {"description": section, "rate": None, "threshold": None})
        d = section_data[section]
        results.append({
            "section": section,
            "description": info["description"],
            "rate": info.get("rate"),
            "threshold": info.get("threshold"),
            "parties": len(d["parties"]),
            "tds_amount": round(d["tds_amount"], 2),
        })

    return results


# ── TDS PARTY-WISE ─────────────────────────────────────────────────────────

def tds_party_wise(conn, section=None, date_from=None, date_to=None):
    """Party-wise TDS detail.
    Returns list of dicts with party, PAN, section, gross payment, TDS amount, effective rate.

    Only considers TDS PAYABLE ledgers (liability-side) for deduction analysis.
    Excludes TDS receivable (asset) and TDS expense ledgers.
    Excludes Payment/Receipt vouchers (remittance to govt) — only counts
    Journal/Purchase/Sales where TDS is actually deducted.
    """
    all_tds = _detect_tds_ledgers(conn)
    # Only use payable TDS ledgers for deduction analysis
    payable_tds = [ld for ld in all_tds if ld.get("category") == "payable"]
    if not payable_tds:
        # Fallback: if no category info, use all (backwards compatibility)
        payable_tds = all_tds
    tds_names = {ld["name"] for ld in payable_tds}
    section_map = {ld["name"]: ld["section"] for ld in payable_tds}
    if not tds_names:
        return []

    placeholders = ",".join(["?"] * len(tds_names))
    date_filter = _build_date_filter(date_from, date_to)

    # Get vouchers with TDS entries, EXCLUDING Payment/Receipt (remittance to govt)
    # Payment vouchers move TDS from payable to bank — not a new deduction
    rows = conn.execute(f"""
        SELECT DISTINCT v.GUID, v.PARTYLEDGERNAME, v.DATE
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        WHERE a.LEDGERNAME IN ({placeholders})
          AND v.VOUCHERTYPENAME NOT IN ('Payment', 'Receipt', 'Contra')
          {date_filter}
    """, list(tds_names)).fetchall()

    # For each voucher, calculate TDS and gross amounts
    party_data = defaultdict(lambda: {
        "tds_amount": 0.0,
        "gross_payment": 0.0,
        "sections": set(),
        "voucher_count": 0,
    })

    for guid, party, date in rows:
        if not party:
            continue

        entries = conn.execute("""
            SELECT LEDGERNAME, CAST(AMOUNT AS REAL) as amt
            FROM trn_accounting WHERE VOUCHER_GUID = ?
        """, (guid,)).fetchall()

        tds_in_voucher = 0.0
        gross_in_voucher = 0.0
        sections_in_voucher = set()

        for ledger, amt in entries:
            amt = _safe_float(amt)
            if ledger in tds_names:
                tds_in_voucher += abs(amt)
                sec = section_map.get(ledger, "Other")
                sections_in_voucher.add(sec)
            else:
                # Non-TDS entries: consider debit entries as gross payment
                if amt < 0:  # debit = expense/payment
                    gross_in_voucher += abs(amt)

        if section and not (sections_in_voucher & {section}):
            continue

        party_data[party]["tds_amount"] += tds_in_voucher
        party_data[party]["gross_payment"] += gross_in_voucher
        party_data[party]["sections"].update(sections_in_voucher)
        party_data[party]["voucher_count"] += 1

    # Get PAN for parties
    pan_map = _get_party_pan_map(conn)

    # For parties where gross = TDS (Journal reclassification entries),
    # back-calculate gross from the TDS rate using TDS_SECTIONS reference.
    # Also try to get actual purchase totals from trn_accounting.
    results = []
    for party in sorted(party_data.keys()):
        d = party_data[party]
        tds_amt = round(d["tds_amount"], 2)
        gross = round(d["gross_payment"], 2)

        # If gross ≈ TDS (100% effective rate), it's likely a Journal reclassification.
        # Back-calculate from the section's statutory rate, or query actual purchases.
        if gross > 0 and abs(gross - tds_amt) < 1.0:
            # Try to get actual purchase total for this party
            try:
                purchase_row = conn.execute("""
                    SELECT ABS(SUM(CAST(a.AMOUNT AS REAL)))
                    FROM trn_accounting a
                    JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                    WHERE v.PARTYLEDGERNAME = ?
                      AND a.LEDGERNAME = ?
                      AND CAST(a.AMOUNT AS REAL) > 0
                """, (party, party)).fetchone()
                if purchase_row and purchase_row[0] and purchase_row[0] > tds_amt:
                    gross = round(purchase_row[0], 2)
            except Exception:
                pass

            # Fallback: back-calculate from section rate
            if abs(gross - tds_amt) < 1.0:
                for sec in d["sections"]:
                    sec_info = TDS_SECTIONS.get(sec)
                    if sec_info and sec_info["rate"] > 0:
                        gross = round(tds_amt / (sec_info["rate"] / 100), 2)
                        break

        eff_rate = round((tds_amt / gross * 100), 2) if gross > 0 else 0.0
        sections_str = ", ".join(sorted(d["sections"]))
        pan = pan_map.get(party, "")
        has_pan = bool(pan and pan.strip())

        results.append({
            "party": party,
            "pan": pan,
            "has_pan": has_pan,
            "sections": sections_str,
            "gross_payment": gross,
            "tds_amount": tds_amt,
            "effective_rate": eff_rate,
            "voucher_count": d["voucher_count"],
        })

    # Sort by TDS amount descending
    results.sort(key=lambda x: x["tds_amount"], reverse=True)
    return results


# ── TDS MONTHLY TREND ──────────────────────────────────────────────────────

def tds_monthly_trend(conn, date_from=None, date_to=None):
    """Month-wise TDS amounts with section breakdown.
    Returns list of dicts: [{"month", "month_label", "total_tds", "sections": {...}}, ...]
    """
    tds_names = _tds_ledger_names(conn)
    section_map = _tds_ledger_section_map(conn)
    if not tds_names:
        return []

    placeholders = ",".join(["?"] * len(tds_names))
    date_filter = _build_date_filter(date_from, date_to)

    rows = conn.execute(f"""
        SELECT SUBSTR(v.DATE, 1, 6) as month,
               a.LEDGERNAME,
               ABS(SUM(CAST(a.AMOUNT AS REAL))) as tds_amount
        FROM trn_accounting a
        JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
        WHERE a.LEDGERNAME IN ({placeholders})
          {date_filter}
        GROUP BY month, a.LEDGERNAME
    """, list(tds_names)).fetchall()

    monthly = defaultdict(lambda: {"total_tds": 0.0, "sections": defaultdict(float)})
    for month, ledger, amt in rows:
        section = section_map.get(ledger, "Other")
        val = _safe_float(amt)
        monthly[month]["total_tds"] += val
        monthly[month]["sections"][section] += val

    results = []
    for month in sorted(monthly.keys()):
        d = monthly[month]
        results.append({
            "month": month,
            "month_label": _month_label(month),
            "total_tds": round(d["total_tds"], 2),
            "sections": {k: round(v, 2) for k, v in sorted(d["sections"].items())},
        })

    return results


# ── TDS QUARTERLY SUMMARY ─────────────────────────────────────────────────

def _get_quarter(month_str):
    """Return quarter label for a YYYYMM string based on Indian FY.
    Apr-Jun = Q1, Jul-Sep = Q2, Oct-Dec = Q3, Jan-Mar = Q4.
    """
    if not month_str or len(month_str) < 6:
        return "Unknown"
    mm = int(month_str[4:6])
    yyyy = int(month_str[0:4])
    if mm >= 4 and mm <= 6:
        fy_start = yyyy
        return f"Q1 (Apr-Jun {yyyy})"
    elif mm >= 7 and mm <= 9:
        fy_start = yyyy
        return f"Q2 (Jul-Sep {yyyy})"
    elif mm >= 10 and mm <= 12:
        fy_start = yyyy
        return f"Q3 (Oct-Dec {yyyy})"
    else:  # Jan-Mar
        fy_start = yyyy - 1
        return f"Q4 (Jan-Mar {yyyy})"


def _get_fy_label(month_str):
    """Return FY label like 'FY 2025-26' for a given YYYYMM."""
    if not month_str or len(month_str) < 6:
        return ""
    mm = int(month_str[4:6])
    yyyy = int(month_str[0:4])
    if mm >= 4:
        return f"FY {yyyy}-{str(yyyy + 1)[2:]}"
    else:
        return f"FY {yyyy - 1}-{str(yyyy)[2:]}"


def tds_quarterly_summary(conn, date_from=None, date_to=None):
    """Quarterly TDS summary for return filing (24Q/26Q/27Q).
    Returns list of dicts: [{"quarter", "sections": {section: amount}, "total"}, ...]
    """
    tds_names = _tds_ledger_names(conn)
    section_map = _tds_ledger_section_map(conn)
    if not tds_names:
        return []

    placeholders = ",".join(["?"] * len(tds_names))
    date_filter = _build_date_filter(date_from, date_to)

    rows = conn.execute(f"""
        SELECT SUBSTR(v.DATE, 1, 6) as month,
               a.LEDGERNAME,
               ABS(SUM(CAST(a.AMOUNT AS REAL))) as tds_amount
        FROM trn_accounting a
        JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
        WHERE a.LEDGERNAME IN ({placeholders})
          {date_filter}
        GROUP BY month, a.LEDGERNAME
    """, list(tds_names)).fetchall()

    quarterly = defaultdict(lambda: {"sections": defaultdict(float), "total": 0.0, "parties": set()})
    for month, ledger, amt in rows:
        q = _get_quarter(month)
        section = section_map.get(ledger, "Other")
        val = _safe_float(amt)
        quarterly[q]["sections"][section] += val
        quarterly[q]["total"] += val

    # Also get party counts per quarter
    rows2 = conn.execute(f"""
        SELECT SUBSTR(v.DATE, 1, 6) as month,
               v.PARTYLEDGERNAME
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        WHERE a.LEDGERNAME IN ({placeholders})
          {date_filter}
        GROUP BY month, v.PARTYLEDGERNAME
    """, list(tds_names)).fetchall()

    for month, party in rows2:
        q = _get_quarter(month)
        if party:
            quarterly[q]["parties"].add(party)

    results = []
    for q in sorted(quarterly.keys()):
        d = quarterly[q]
        results.append({
            "quarter": q,
            "sections": {k: round(v, 2) for k, v in sorted(d["sections"].items())},
            "total": round(d["total"], 2),
            "party_count": len(d["parties"]),
        })

    return results


# ── TDS THRESHOLD CHECK ────────────────────────────────────────────────────

def tds_threshold_check(conn, date_from=None, date_to=None):
    """Find parties where aggregate payments may exceed TDS thresholds but no TDS was deducted.
    Checks payments under Indirect Expenses groups (contractors, professionals, rent).
    Returns list of dicts with party, total payment, applicable section, threshold, status.
    """
    tds_names = _tds_ledger_names(conn)
    date_filter = _build_date_filter(date_from, date_to)

    # Get all expense groups (Indirect Expenses and sub-groups)
    expense_groups = set()
    try:
        queue = ["Indirect Expenses"]
        while queue:
            parent = queue.pop(0)
            expense_groups.add(parent)
            children = conn.execute(
                "SELECT NAME FROM mst_group WHERE PARENT = ?", (parent,)
            ).fetchall()
            for (child,) in children:
                if child not in expense_groups:
                    queue.append(child)
    except Exception:
        expense_groups = {"Indirect Expenses"}

    # Also get Sundry Creditor groups (for contractor payments like KARIGAR)
    creditor_groups = set()
    try:
        queue = ["Sundry Creditors"]
        while queue:
            parent = queue.pop(0)
            creditor_groups.add(parent)
            children = conn.execute(
                "SELECT NAME FROM mst_group WHERE PARENT = ?", (parent,)
            ).fetchall()
            for (child,) in children:
                if child not in creditor_groups:
                    queue.append(child)
    except Exception:
        creditor_groups = {"Sundry Creditors"}

    all_check_groups = expense_groups | creditor_groups

    if not all_check_groups:
        return []

    # Get ledgers from all groups
    eg_ph = ",".join(["?"] * len(all_check_groups))
    all_ledgers = conn.execute(f"""
        SELECT name, parent FROM mst_ledger
        WHERE parent IN ({eg_ph})
    """, list(all_check_groups)).fetchall()

    # Map ledger names to possible TDS sections
    expense_ledger_sections = {}
    contractor_group_keywords = {"karigar", "kaarigar", "artisan", "labour", "contractor",
                                 "job work", "sub-contract", "fabricat"}
    for name, parent in all_ledgers:
        upper = (name + " " + parent).upper()
        parent_lower = (parent or "").lower()
        if parent in creditor_groups and parent not in expense_groups:
            # Creditor ledger — check if parent group suggests contractor (194C)
            if any(kw in parent_lower for kw in contractor_group_keywords):
                expense_ledger_sections[name] = "194C"
            else:
                # General sundry creditor — check for 194Q (purchase of goods > 50L)
                expense_ledger_sections[name] = "194Q"
        elif "RENT" in upper:
            expense_ledger_sections[name] = "194I"
        elif "PROFESSION" in upper or "CONSULT" in upper or "LEGAL" in upper or "TECHNICAL" in upper:
            expense_ledger_sections[name] = "194J"
        elif "CONTRACT" in upper or "LABOUR" in upper or "LABOR" in upper or "SUB-CONTRACT" in upper:
            expense_ledger_sections[name] = "194C"
        elif "COMMISSION" in upper or "BROKERAGE" in upper:
            expense_ledger_sections[name] = "194H"
        elif "INTEREST" in upper:
            expense_ledger_sections[name] = "194A"
        else:
            # Generic expense -- could be 194C or 194J
            expense_ledger_sections[name] = None

    if not expense_ledger_sections:
        return []

    # Get payment amounts per party per ledger
    # Split into expense-side (negative amounts = debit) and creditor-side (positive = credit)
    # For expense ledgers: debit entries represent payments
    # For creditor ledgers: credit entries represent amounts owed
    exp_names = list(expense_ledger_sections.keys())
    exp_ph = ",".join(["?"] * len(exp_names))

    creditor_ledger_names = {name for name, parent in all_ledgers if parent in creditor_groups}

    rows = conn.execute(f"""
        SELECT v.PARTYLEDGERNAME,
               a.LEDGERNAME,
               SUM(CASE WHEN CAST(a.AMOUNT AS REAL) > 0 THEN CAST(a.AMOUNT AS REAL) ELSE 0 END) as credit_total,
               SUM(CASE WHEN CAST(a.AMOUNT AS REAL) < 0 THEN ABS(CAST(a.AMOUNT AS REAL)) ELSE 0 END) as debit_total
        FROM trn_accounting a
        JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
        WHERE a.LEDGERNAME IN ({exp_ph})
          AND v.PARTYLEDGERNAME IS NOT NULL AND v.PARTYLEDGERNAME != ''
          {date_filter}
        GROUP BY v.PARTYLEDGERNAME, a.LEDGERNAME
    """, exp_names).fetchall()

    # Convert to total_paid using the appropriate side
    processed_rows = []
    for party, ledger, credit_total, debit_total in rows:
        if ledger in creditor_ledger_names:
            # Creditor: credit entries = amounts purchased/owed
            total = _safe_float(credit_total)
        else:
            # Expense: debit entries = amounts spent
            total = _safe_float(debit_total)
        if total > 0:
            processed_rows.append((party, ledger, total))

    # Aggregate per party
    party_payments = defaultdict(lambda: {"total": 0.0, "sections": set(), "expense_ledgers": set()})
    for party, ledger, total in processed_rows:
        sec = expense_ledger_sections.get(ledger)
        party_payments[party]["total"] += _safe_float(total)
        if sec:
            party_payments[party]["sections"].add(sec)
        party_payments[party]["expense_ledgers"].add(ledger)

    # Now check which parties had TDS deducted
    parties_with_tds = set()
    if tds_names:
        tds_ph = ",".join(["?"] * len(tds_names))
        tds_parties = conn.execute(f"""
            SELECT DISTINCT v.PARTYLEDGERNAME
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            WHERE a.LEDGERNAME IN ({tds_ph})
              AND v.PARTYLEDGERNAME IS NOT NULL
              {date_filter}
        """, list(tds_names)).fetchall()
        parties_with_tds = {r[0] for r in tds_parties}

    results = []
    for party, data in sorted(party_payments.items(), key=lambda x: x[1]["total"], reverse=True):
        total = round(data["total"], 2)
        sections = data["sections"]
        has_tds = party in parties_with_tds

        # Determine applicable threshold
        applicable_section = None
        threshold = None
        if sections:
            # Pick the primary section
            for sec in ["194C", "194J", "194H", "194I", "194A"]:
                if sec in sections:
                    applicable_section = sec
                    info = TDS_SECTIONS.get(sec, {})
                    threshold = info.get("threshold")
                    break

        if threshold and total >= threshold and not has_tds:
            status = "BREACH"
        elif threshold and total >= threshold * 0.8 and not has_tds:
            status = "WARNING"
        elif has_tds:
            status = "OK"
        else:
            status = "BELOW_THRESHOLD"

        # Only include if there's a potential issue or meaningful data
        if threshold and total >= threshold * 0.5:
            results.append({
                "party": party,
                "total_payment": total,
                "applicable_section": applicable_section or "Unknown",
                "threshold": threshold,
                "tds_deducted": has_tds,
                "status": status,
                "expense_ledgers": ", ".join(sorted(data["expense_ledgers"])),
            })

    return results


# ── PAN CHECK ──────────────────────────────────────────────────────────────

def tds_pan_check(conn):
    """Check PAN availability for parties with TDS entries.
    Returns list of dicts: [{"party", "pan", "has_pan", "tds_amount"}, ...]
    Higher TDS rate (20%) applies without PAN.
    """
    tds_names = _tds_ledger_names(conn)
    if not tds_names:
        return []

    placeholders = ",".join(["?"] * len(tds_names))

    # Get parties with TDS and their total TDS
    rows = conn.execute(f"""
        SELECT v.PARTYLEDGERNAME,
               ABS(SUM(CAST(a.AMOUNT AS REAL))) as tds_amount
        FROM trn_accounting a
        JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
        WHERE a.LEDGERNAME IN ({placeholders})
          AND v.PARTYLEDGERNAME IS NOT NULL AND v.PARTYLEDGERNAME != ''
        GROUP BY v.PARTYLEDGERNAME
        ORDER BY tds_amount DESC
    """, list(tds_names)).fetchall()

    pan_map = _get_party_pan_map(conn)

    results = []
    for party, tds_amt in rows:
        pan = pan_map.get(party, "")
        has_pan = bool(pan and pan.strip())
        results.append({
            "party": party,
            "pan": pan,
            "has_pan": has_pan,
            "tds_amount": round(_safe_float(tds_amt), 2),
            "status": "OK" if has_pan else "MISSING PAN",
        })

    return results


# ── TDS RATE VERIFICATION ─────────────────────────────────────────────────

def tds_rate_verification(conn, date_from=None, date_to=None):
    """Verify if correct TDS rates were applied for each party/section.
    Computes effective TDS rate and flags mismatches.
    Returns list of dicts.
    """
    party_data = tds_party_wise(conn, date_from=date_from, date_to=date_to)
    section_map = _tds_ledger_section_map(conn)

    results = []
    for pd in party_data:
        sections = pd["sections"].split(", ") if pd["sections"] else []
        eff_rate = pd["effective_rate"]
        gross = pd["gross_payment"]
        tds_amt = pd["tds_amount"]

        expected_rate = None
        for sec in sections:
            info = TDS_SECTIONS.get(sec, {})
            rate = info.get("rate")
            if rate is not None:
                expected_rate = rate
                break

        if expected_rate is not None and eff_rate > 0:
            diff = abs(eff_rate - expected_rate)
            if diff > 2.0:
                status = "MISMATCH"
            elif diff > 0.5:
                status = "MINOR_DIFF"
            else:
                status = "OK"
        elif not pd["has_pan"] and eff_rate > 0:
            # Without PAN, 20% rate should apply
            if abs(eff_rate - 20.0) <= 2.0:
                status = "OK (No PAN - 20%)"
            else:
                status = "CHECK_PAN_RATE"
        else:
            status = "N/A"

        results.append({
            "party": pd["party"],
            "pan": pd["pan"],
            "sections": pd["sections"],
            "gross_payment": gross,
            "tds_amount": tds_amt,
            "effective_rate": eff_rate,
            "expected_rate": expected_rate,
            "status": status,
        })

    return results


# ── PARTY VOUCHER DETAIL ──────────────────────────────────────────────────

def tds_party_vouchers(conn, party_name, date_from=None, date_to=None):
    """Get all TDS vouchers for a specific party.
    Returns list of dicts: [{"date", "voucher_no", "voucher_type", "guid",
                             "tds_amount", "gross_amount", "narration", "section"}, ...]
    """
    tds_names = _tds_ledger_names(conn)
    section_map = _tds_ledger_section_map(conn)
    if not tds_names:
        return []

    placeholders = ",".join(["?"] * len(tds_names))
    date_filter = _build_date_filter(date_from, date_to)

    vouchers = conn.execute(f"""
        SELECT DISTINCT v.GUID, v.DATE, v.VOUCHERNUMBER, v.VOUCHERTYPENAME, v.NARRATION
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        WHERE a.LEDGERNAME IN ({placeholders})
          AND v.PARTYLEDGERNAME = ?
          {date_filter}
        ORDER BY v.DATE, v.VOUCHERNUMBER
    """, list(tds_names) + [party_name]).fetchall()

    results = []
    for guid, date, vchno, vchtype, narration in vouchers:
        entries = conn.execute("""
            SELECT LEDGERNAME, CAST(AMOUNT AS REAL) as amt
            FROM trn_accounting WHERE VOUCHER_GUID = ?
        """, (guid,)).fetchall()

        tds_amt = 0.0
        gross_amt = 0.0
        sections = set()

        for ledger, amt in entries:
            amt = _safe_float(amt)
            if ledger in tds_names:
                tds_amt += abs(amt)
                sections.add(section_map.get(ledger, "Other"))
            elif amt < 0:
                gross_amt += abs(amt)

        results.append({
            "date": _format_date(date),
            "date_raw": date,
            "voucher_no": vchno,
            "voucher_type": vchtype,
            "guid": guid,
            "tds_amount": round(tds_amt, 2),
            "gross_amount": round(gross_amt, 2),
            "narration": narration or "",
            "section": ", ".join(sorted(sections)),
        })

    return results


# ── HELPERS ────────────────────────────────────────────────────────────────

def _build_date_filter(date_from, date_to):
    """Build SQL date filter clause."""
    parts = []
    if date_from:
        parts.append(f"AND v.DATE >= '{date_from}'")
    if date_to:
        parts.append(f"AND v.DATE <= '{date_to}'")
    return " ".join(parts)


def _get_party_pan_map(conn):
    """Return dict: party_name -> PAN from mst_ledger."""
    pan_map = {}
    has_pan_col = _has_column(conn, "mst_ledger", "INCOMETAXNUMBER")
    if has_pan_col:
        try:
            rows = conn.execute(
                "SELECT name, INCOMETAXNUMBER FROM mst_ledger WHERE INCOMETAXNUMBER IS NOT NULL AND INCOMETAXNUMBER != ''"
            ).fetchall()
            for name, pan in rows:
                pan_map[name] = pan
        except Exception:
            pass
    return pan_map
