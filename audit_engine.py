"""
Seven Labs Vision — Audit Red Flag Engine (Layer 3)
Automated audit checks that run on any Tally company data.
Each check returns a list of flagged items with severity and details.

DEFENSIVE: Handles missing columns (NARRATION, CLOSINGBALANCE),
very small companies (10 vouchers), companies with only Journals,
Benford's Law minimum 100 transactions, zero division protection.
"""

import sqlite3
import math
from collections import Counter
from datetime import datetime, timedelta
from defensive_helpers import (
    table_exists, column_exists, get_table_columns,
    safe_float, safe_divide
)
from tally_reports import get_groups_by_nature


def run_all_checks(db_path="tally_data.db"):
    """Run all audit checks and return consolidated results."""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
    except Exception as e:
        return {"_summary": {"total_checks": 0, "total_flags": 0,
                "error": str(e), "risk_score": 0}}

    results = {}

    # Each check is wrapped individually so one failure doesn't stop all
    check_funcs = [
        ("benfords_law", check_benfords_law),
        ("duplicate_invoices", check_duplicate_invoices),
        ("voucher_gaps", check_voucher_gaps),
        ("holiday_entries", check_holiday_entries),
        ("cash_limit_breach", check_cash_limit),
        ("round_amount_entries", check_round_amounts),
        ("negative_cash_balance", check_negative_cash),
        ("debit_creditors", check_debit_balance_creditors),
        ("credit_debtors", check_credit_balance_debtors),
        ("period_end_journals", check_period_end_journals),
        ("large_journal_entries", check_large_journals),
    ]

    for key, func in check_funcs:
        try:
            results[key] = func(conn)
        except Exception as e:
            results[key] = {"check": key, "severity": "Medium", "flag_count": 0,
                           "status": "error", "error": str(e)}

    # Summary
    total_flags = sum(r.get("flag_count", 0) for r in results.values())
    high_flags = sum(1 for r in results.values() if r.get("severity") == "High" and r.get("flag_count", 0) > 0)
    medium_flags = sum(1 for r in results.values() if r.get("severity") == "Medium" and r.get("flag_count", 0) > 0)

    results["_summary"] = {
        "total_checks": len(results),
        "total_flags": total_flags,
        "high_severity_checks": high_flags,
        "medium_severity_checks": medium_flags,
        "risk_score": min(100, high_flags * 15 + medium_flags * 8 + min(total_flags, 50)),
    }

    conn.close()
    return results


# ════════════════════════════════════════════════════════════════════════════
# 1. BENFORD'S LAW ANALYSIS
# ════════════════════════════════════════════════════════════════════════════

def check_benfords_law(conn):
    """
    Test first-digit distribution of all transaction amounts against Benford's Law.
    Significant deviation suggests fabricated or manipulated entries.
    """
    cur = conn.cursor()
    expected = {
        1: 30.1, 2: 17.6, 3: 12.5, 4: 9.7, 5: 7.9,
        6: 6.7, 7: 5.8, 8: 5.1, 9: 4.6
    }

    try:
        cur.execute("""
            SELECT AMOUNT FROM trn_accounting
            WHERE AMOUNT IS NOT NULL AND AMOUNT != '' AND CAST(AMOUNT AS REAL) != 0
        """)
        amounts = []
        for row in cur.fetchall():
            try:
                val = abs(float(row["AMOUNT"]))
                if val >= 1:  # Benford's only works for numbers >= 1
                    amounts.append(val)
            except (ValueError, TypeError):
                continue

        if len(amounts) < 100:
            return {"check": "Benford's Law", "severity": "High", "flag_count": 0,
                    "status": "skipped", "reason": f"Only {len(amounts)} transactions (need 100+)"}

        # Count first digits
        digit_counts = Counter()
        for amt in amounts:
            first_digit = int(str(amt).lstrip('0').lstrip('.')[0])
            if 1 <= first_digit <= 9:
                digit_counts[first_digit] += 1

        total = sum(digit_counts.values())
        observed = {d: (digit_counts.get(d, 0) / total) * 100 for d in range(1, 10)}

        # Chi-square test
        chi_square = 0
        for d in range(1, 10):
            exp = expected[d] / 100 * total
            obs = digit_counts.get(d, 0)
            chi_square += ((obs - exp) ** 2) / exp

        # Degrees of freedom = 8 (9 digits - 1)
        # Critical value at 95% confidence = 15.507
        # Critical value at 99% confidence = 20.090
        is_suspicious = chi_square > 15.507
        is_highly_suspicious = chi_square > 20.090

        # Find most deviant digits
        deviations = []
        for d in range(1, 10):
            dev = observed[d] - expected[d]
            if abs(dev) > 3:  # More than 3 percentage points off
                deviations.append({
                    "digit": d,
                    "expected_pct": round(expected[d], 1),
                    "observed_pct": round(observed[d], 1),
                    "deviation_pct": round(dev, 1),
                    "direction": "over-represented" if dev > 0 else "under-represented"
                })

        return {
            "check": "Benford's Law",
            "severity": "High" if is_highly_suspicious else "Medium" if is_suspicious else "Low",
            "flag_count": len(deviations),
            "status": "fail" if is_suspicious else "pass",
            "chi_square": round(chi_square, 2),
            "critical_value_95": 15.507,
            "is_suspicious": is_suspicious,
            "total_transactions": total,
            "digit_distribution": {d: {"expected": round(expected[d], 1), "observed": round(observed[d], 1)} for d in range(1, 10)},
            "deviations": deviations,
            "description": "First-digit frequency analysis on all transaction amounts. Benford's Law predicts natural data follows a specific pattern. Deviations suggest manipulation."
        }
    except Exception as e:
        return {"check": "Benford's Law", "severity": "High", "flag_count": 0,
                "status": "error", "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
# 2. DUPLICATE INVOICE DETECTION
# ════════════════════════════════════════════════════════════════════════════

def check_duplicate_invoices(conn):
    """Find potential duplicate invoices by matching voucher number + party + amount."""
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT VOUCHERNUMBER, PARTYLEDGERNAME, VOUCHERTYPENAME, DATE,
                   COUNT(*) as cnt, GROUP_CONCAT(GUID, '|') as guids
            FROM trn_voucher
            WHERE VOUCHERNUMBER IS NOT NULL AND VOUCHERNUMBER != ''
            AND VOUCHERTYPENAME NOT IN ('Contra', 'Attendance')
            GROUP BY VOUCHERNUMBER, PARTYLEDGERNAME, VOUCHERTYPENAME
            HAVING COUNT(*) > 1
            ORDER BY cnt DESC
            LIMIT 50
        """)
        duplicates = []
        for row in cur.fetchall():
            duplicates.append({
                "voucher_number": row["VOUCHERNUMBER"],
                "party": row["PARTYLEDGERNAME"],
                "type": row["VOUCHERTYPENAME"],
                "date": row["DATE"],
                "duplicate_count": row["cnt"],
            })

        # Also check same party + same amount + same date (different voucher numbers)
        # NOTE: Don't filter by ISPARTYLEDGER — field may not exist in all companies
        # NOTE: Don't hardcode voucher types — some companies use Journals for sales (e.g. TONOTO)
        cur.execute("""
            SELECT v.PARTYLEDGERNAME, v.DATE, v.VOUCHERTYPENAME,
                   a.AMOUNT, COUNT(*) as cnt
            FROM trn_voucher v
            JOIN trn_accounting a ON v.GUID = a.VOUCHER_GUID
            WHERE v.PARTYLEDGERNAME IS NOT NULL AND v.PARTYLEDGERNAME != ''
            AND a.AMOUNT IS NOT NULL AND ABS(CAST(a.AMOUNT AS REAL)) > 0
            GROUP BY v.PARTYLEDGERNAME, v.DATE, v.VOUCHERTYPENAME, a.AMOUNT
            HAVING COUNT(*) > 1
            ORDER BY cnt DESC
            LIMIT 50
        """)
        amount_dupes = []
        for row in cur.fetchall():
            try:
                amt = float(row["AMOUNT"])
            except:
                amt = 0
            amount_dupes.append({
                "party": row["PARTYLEDGERNAME"],
                "date": row["DATE"],
                "type": row["VOUCHERTYPENAME"],
                "amount": amt,
                "count": row["cnt"],
            })

        total_flags = len(duplicates) + len(amount_dupes)
        return {
            "check": "Duplicate Invoices",
            "severity": "High",
            "flag_count": total_flags,
            "status": "fail" if total_flags > 0 else "pass",
            "exact_duplicates": duplicates,
            "amount_date_party_duplicates": amount_dupes,
            "description": "Detects invoices with same voucher number + party, or same party + date + amount."
        }
    except Exception as e:
        return {"check": "Duplicate Invoices", "severity": "High", "flag_count": 0,
                "status": "error", "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
# 3. VOUCHER NUMBER GAP ANALYSIS
# ════════════════════════════════════════════════════════════════════════════

def check_voucher_gaps(conn):
    """Detect gaps in voucher number sequences — indicates deleted or missing vouchers."""
    cur = conn.cursor()
    try:
        # Get all voucher types and their number series
        cur.execute("""
            SELECT VOUCHERTYPENAME, VOUCHERNUMBER
            FROM trn_voucher
            WHERE VOUCHERNUMBER IS NOT NULL AND VOUCHERNUMBER != ''
            ORDER BY VOUCHERTYPENAME, VOUCHERNUMBER
        """)

        by_type = {}
        for row in cur.fetchall():
            vtype = row["VOUCHERTYPENAME"]
            vnum = row["VOUCHERNUMBER"]
            if vtype not in by_type:
                by_type[vtype] = []
            by_type[vtype].append(vnum)

        gaps = []
        for vtype, numbers in by_type.items():
            # Extract numeric parts
            numeric_nums = []
            for n in numbers:
                # Try to extract number from patterns like "T000001", "SL/001", "RV-0001"
                digits = ''.join(c for c in n if c.isdigit())
                if digits:
                    numeric_nums.append((int(digits), n))

            if len(numeric_nums) < 3:
                continue

            numeric_nums.sort()
            type_gaps = []
            for i in range(1, len(numeric_nums)):
                diff = numeric_nums[i][0] - numeric_nums[i-1][0]
                if diff > 1 and diff <= 10:  # Gap of up to 10
                    type_gaps.append({
                        "from_number": numeric_nums[i-1][1],
                        "to_number": numeric_nums[i][1],
                        "missing_count": diff - 1,
                        "from_int": numeric_nums[i-1][0],
                        "to_int": numeric_nums[i][0],
                    })

            if type_gaps:
                total_missing = sum(g["missing_count"] for g in type_gaps)
                gaps.append({
                    "voucher_type": vtype,
                    "total_gaps": len(type_gaps),
                    "total_missing": total_missing,
                    "details": type_gaps[:10],  # Top 10 gaps
                })

        total_flags = sum(g["total_missing"] for g in gaps)
        return {
            "check": "Voucher Number Gaps",
            "severity": "Medium",
            "flag_count": total_flags,
            "status": "fail" if total_flags > 0 else "pass",
            "gaps_by_type": gaps,
            "description": "Detects gaps in sequential voucher numbering. Missing numbers may indicate deleted transactions."
        }
    except Exception as e:
        return {"check": "Voucher Number Gaps", "severity": "Medium", "flag_count": 0,
                "status": "error", "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
# 4. SUNDAY / HOLIDAY ENTRY DETECTION
# ════════════════════════════════════════════════════════════════════════════

def check_holiday_entries(conn):
    """Flag transactions recorded on Sundays (and optionally other holidays)."""
    cur = conn.cursor()
    try:
        has_narration = column_exists(conn, "trn_voucher", "NARRATION")
        narration_col = ", NARRATION" if has_narration else ""
        cur.execute(f"""
            SELECT GUID, DATE, VOUCHERTYPENAME, VOUCHERNUMBER, PARTYLEDGERNAME{narration_col}
            FROM trn_voucher
            WHERE DATE IS NOT NULL AND DATE != ''
        """)

        sunday_entries = []
        for row in cur.fetchall():
            try:
                date_str = str(row["DATE"])
                if len(date_str) == 8:  # YYYYMMDD format
                    dt = datetime.strptime(date_str, "%Y%m%d")
                    if dt.weekday() == 6:  # Sunday
                        sunday_entries.append({
                            "date": dt.strftime("%d-%b-%Y"),
                            "day": "Sunday",
                            "voucher_type": row["VOUCHERTYPENAME"],
                            "voucher_number": row["VOUCHERNUMBER"],
                            "party": row["PARTYLEDGERNAME"] or "",
                            "narration": ((row["NARRATION"] or "")[:100]) if has_narration else "",
                        })
            except:
                continue

        # Indian national holidays (fixed dates — approximate)
        national_holidays = [
            ("0126", "Republic Day"),
            ("0815", "Independence Day"),
            ("1002", "Gandhi Jayanti"),
        ]

        holiday_entries = []
        cur.execute("""
            SELECT GUID, DATE, VOUCHERTYPENAME, VOUCHERNUMBER, PARTYLEDGERNAME
            FROM trn_voucher WHERE DATE IS NOT NULL AND DATE != ''
        """)
        for row in cur.fetchall():
            try:
                date_str = str(row["DATE"])
                mmdd = date_str[4:8]
                for hdate, hname in national_holidays:
                    if mmdd == hdate:
                        holiday_entries.append({
                            "date": datetime.strptime(date_str, "%Y%m%d").strftime("%d-%b-%Y"),
                            "holiday": hname,
                            "voucher_type": row["VOUCHERTYPENAME"],
                            "voucher_number": row["VOUCHERNUMBER"],
                            "party": row["PARTYLEDGERNAME"] or "",
                        })
            except:
                continue

        total = len(sunday_entries) + len(holiday_entries)
        return {
            "check": "Sunday/Holiday Entries",
            "severity": "Medium",
            "flag_count": total,
            "status": "fail" if total > 0 else "pass",
            "sunday_entries": sunday_entries[:50],  # Limit output
            "sunday_count": len(sunday_entries),
            "holiday_entries": holiday_entries[:20],
            "holiday_count": len(holiday_entries),
            "description": "Transactions on Sundays or national holidays may indicate backdating or manipulation."
        }
    except Exception as e:
        return {"check": "Sunday/Holiday Entries", "severity": "Medium", "flag_count": 0,
                "status": "error", "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
# 5. CASH TRANSACTION LIMIT (Section 269ST)
# ════════════════════════════════════════════════════════════════════════════

def check_cash_limit(conn):
    """Flag cash receipts/payments > Rs 2,00,000 per Section 269ST."""
    cur = conn.cursor()
    try:
        # Find cash ledger
        _cash_groups = get_groups_by_nature(conn, 'cash')
        _cash_ph = ",".join(["?"] * len(_cash_groups)) if _cash_groups else "'__NONE__'"
        cur.execute(f"SELECT NAME FROM mst_ledger WHERE PARENT IN ({_cash_ph})", _cash_groups)
        cash_ledgers = [row[0] for row in cur.fetchall()]

        if not cash_ledgers:
            return {"check": "Cash Transaction Limits", "severity": "High", "flag_count": 0,
                    "status": "skipped", "reason": "No cash ledgers found"}

        cash_names = "','".join(cash_ledgers)
        has_narration = column_exists(conn, "trn_voucher", "NARRATION")
        narration_sel = ", v.NARRATION" if has_narration else ""
        cur.execute(f"""
            SELECT a.VOUCHER_GUID, a.LEDGERNAME, a.AMOUNT, a.ISDEEMEDPOSITIVE,
                   v.DATE, v.VOUCHERTYPENAME, v.VOUCHERNUMBER, v.PARTYLEDGERNAME{narration_sel}
            FROM trn_accounting a
            JOIN trn_voucher v ON a.VOUCHER_GUID = v.GUID
            WHERE a.LEDGERNAME IN ('{cash_names}')
            AND ABS(CAST(a.AMOUNT AS REAL)) >= 200000
            ORDER BY ABS(CAST(a.AMOUNT AS REAL)) DESC
        """)

        breaches = []
        for row in cur.fetchall():
            try:
                amt = abs(float(row["AMOUNT"]))
                date_str = str(row["DATE"])
                dt = datetime.strptime(date_str, "%Y%m%d").strftime("%d-%b-%Y") if len(date_str) == 8 else date_str
                breaches.append({
                    "date": dt,
                    "voucher_type": row["VOUCHERTYPENAME"],
                    "voucher_number": row["VOUCHERNUMBER"],
                    "party": row["PARTYLEDGERNAME"] or "",
                    "amount": amt,
                    "direction": "Receipt" if row["ISDEEMEDPOSITIVE"] == "Yes" else "Payment",
                    "narration": ((row["NARRATION"] or "")[:100]) if has_narration else "",
                })
            except:
                continue

        return {
            "check": "Cash Transaction Limits (Sec 269ST)",
            "severity": "High",
            "flag_count": len(breaches),
            "status": "fail" if breaches else "pass",
            "breaches": breaches,
            "threshold": 200000,
            "cash_ledgers": cash_ledgers,
            "description": "Section 269ST prohibits cash receipts of Rs 2,00,000 or more. Penalty equals the cash amount received."
        }
    except Exception as e:
        return {"check": "Cash Transaction Limits", "severity": "High", "flag_count": 0,
                "status": "error", "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
# 6. ROUND AMOUNT ENTRIES
# ════════════════════════════════════════════════════════════════════════════

def check_round_amounts(conn):
    """Flag transactions with suspiciously round amounts (ending in 000, 00000)."""
    cur = conn.cursor()
    try:
        has_narration = column_exists(conn, "trn_voucher", "NARRATION")
        narration_sel = ", v.NARRATION" if has_narration else ""
        # Journal entries with round amounts are most suspicious
        cur.execute(f"""
            SELECT v.GUID, v.DATE, v.VOUCHERTYPENAME, v.VOUCHERNUMBER,
                   v.PARTYLEDGERNAME{narration_sel},
                   a.LEDGERNAME, a.AMOUNT
            FROM trn_accounting a
            JOIN trn_voucher v ON a.VOUCHER_GUID = v.GUID
            WHERE v.VOUCHERTYPENAME = 'Journal'
            AND a.AMOUNT IS NOT NULL AND a.AMOUNT != ''
            AND ABS(CAST(a.AMOUNT AS REAL)) >= 10000
            AND CAST(ABS(CAST(a.AMOUNT AS REAL)) AS INTEGER) % 1000 = 0
            ORDER BY ABS(CAST(a.AMOUNT AS REAL)) DESC
            LIMIT 100
        """)

        round_entries = []
        for row in cur.fetchall():
            try:
                amt = abs(float(row["AMOUNT"]))
                date_str = str(row["DATE"])
                dt = datetime.strptime(date_str, "%Y%m%d").strftime("%d-%b-%Y") if len(date_str) == 8 else date_str

                # Determine "roundness" level
                if amt % 100000 == 0:
                    roundness = "Lakh-round"
                elif amt % 10000 == 0:
                    roundness = "10K-round"
                else:
                    roundness = "1K-round"

                round_entries.append({
                    "date": dt,
                    "voucher_number": row["VOUCHERNUMBER"],
                    "ledger": row["LEDGERNAME"],
                    "amount": amt,
                    "roundness": roundness,
                    "party": row["PARTYLEDGERNAME"] or "",
                    "narration": ((row["NARRATION"] or "")[:100]) if has_narration else "",
                })
            except:
                continue

        return {
            "check": "Round Amount Journal Entries",
            "severity": "Medium",
            "flag_count": len(round_entries),
            "status": "fail" if round_entries else "pass",
            "entries": round_entries,
            "description": "Journal entries with perfectly round amounts (ending in 000) may indicate estimated or fabricated entries."
        }
    except Exception as e:
        return {"check": "Round Amount Entries", "severity": "Medium", "flag_count": 0,
                "status": "error", "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
# 7. NEGATIVE CASH BALANCE DETECTION
# ════════════════════════════════════════════════════════════════════════════

def check_negative_cash(conn):
    """Detect dates where cash balance goes negative — indicates unrecorded receipts."""
    cur = conn.cursor()
    try:
        # Get cash opening balance (recursive sub-groups)
        _cash_groups2 = get_groups_by_nature(conn, 'cash')
        _cash_ph2 = ",".join(["?"] * len(_cash_groups2)) if _cash_groups2 else "'__NONE__'"
        cur.execute(f"SELECT NAME, OPENINGBALANCE FROM mst_ledger WHERE PARENT IN ({_cash_ph2})", _cash_groups2)
        cash_ledgers = []
        for row in cur.fetchall():
            try:
                ob = float(row["OPENINGBALANCE"] or 0)
            except:
                ob = 0
            cash_ledgers.append({"name": row["NAME"], "opening_balance": ob})

        if not cash_ledgers:
            return {"check": "Negative Cash Balance", "severity": "High", "flag_count": 0,
                    "status": "skipped", "reason": "No cash ledgers found"}

        negative_dates = []
        for cl in cash_ledgers:
            cur.execute(f"""
                SELECT v.DATE, a.AMOUNT, a.ISDEEMEDPOSITIVE
                FROM trn_accounting a
                JOIN trn_voucher v ON a.VOUCHER_GUID = v.GUID
                WHERE a.LEDGERNAME = ?
                ORDER BY v.DATE, v.ALTERID
            """, (cl["name"],))

            running_balance = cl["opening_balance"]
            for row in cur.fetchall():
                try:
                    amt = float(row["AMOUNT"])
                    # In Tally, negative amount with ISDEEMEDPOSITIVE=Yes means debit (receipt for cash)
                    # Positive amount means credit (payment from cash)
                    running_balance -= amt  # Tally convention
                    if running_balance < -1:  # Allow small rounding
                        date_str = str(row["DATE"])
                        dt = datetime.strptime(date_str, "%Y%m%d").strftime("%d-%b-%Y") if len(date_str) == 8 else date_str
                        negative_dates.append({
                            "date": dt,
                            "cash_ledger": cl["name"],
                            "balance": round(running_balance, 2),
                        })
                except:
                    continue

        # Deduplicate by date
        seen = set()
        unique_negatives = []
        for nd in negative_dates:
            key = (nd["date"], nd["cash_ledger"])
            if key not in seen:
                seen.add(key)
                unique_negatives.append(nd)

        return {
            "check": "Negative Cash Balance",
            "severity": "High",
            "flag_count": len(unique_negatives),
            "status": "fail" if unique_negatives else "pass",
            "negative_dates": unique_negatives[:30],
            "description": "Cash balance going negative on any date indicates teeming & lading or unrecorded receipts."
        }
    except Exception as e:
        return {"check": "Negative Cash Balance", "severity": "High", "flag_count": 0,
                "status": "error", "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
# 8. DEBIT BALANCE IN CREDITORS
# ════════════════════════════════════════════════════════════════════════════

def _bal_col_ae(conn):
    """Return best balance column: COMPUTED_CB if available, else CLOSINGBALANCE."""
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(mst_ledger)").fetchall()}
        return "COMPUTED_CB" if "COMPUTED_CB" in cols else "CLOSINGBALANCE"
    except Exception:
        return "CLOSINGBALANCE"


def check_debit_balance_creditors(conn):
    """Creditors with debit balance = overpayment, mapping error, or misappropriation."""
    cur = conn.cursor()
    try:
        bc = _bal_col_ae(conn)
        _cr_groups = get_groups_by_nature(conn, 'creditors')
        _cr_ph = ",".join(["?"] * len(_cr_groups)) if _cr_groups else "'__NONE__'"
        cur.execute(f"""
            SELECT NAME, {bc} AS CLOSINGBALANCE FROM mst_ledger
            WHERE PARENT IN ({_cr_ph})
            AND {bc} IS NOT NULL AND {bc} != ''
            AND CAST({bc} AS REAL) > 0
            ORDER BY CAST({bc} AS REAL) DESC
        """, _cr_groups)
        # In Tally, creditors normally have negative (credit) balance
        # Positive = debit balance = unusual
        flagged = []
        for row in cur.fetchall():
            try:
                bal = float(row["CLOSINGBALANCE"])
                if bal > 0:
                    flagged.append({
                        "party": row["NAME"],
                        "debit_balance": round(bal, 2),
                    })
            except:
                continue

        return {
            "check": "Debit Balance in Creditors",
            "severity": "Medium",
            "flag_count": len(flagged),
            "status": "fail" if flagged else "pass",
            "parties": flagged[:30],
            "description": "Creditors with debit balance indicate overpayment, advance to suppliers, or mapping errors."
        }
    except Exception as e:
        return {"check": "Debit Balance in Creditors", "severity": "Medium", "flag_count": 0,
                "status": "error", "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
# 9. CREDIT BALANCE IN DEBTORS
# ════════════════════════════════════════════════════════════════════════════

def check_credit_balance_debtors(conn):
    """Debtors with credit balance = advance received not adjusted, or fictitious debtors."""
    cur = conn.cursor()
    try:
        bc = _bal_col_ae(conn)
        _dr_groups = get_groups_by_nature(conn, 'debtors')
        _dr_ph = ",".join(["?"] * len(_dr_groups)) if _dr_groups else "'__NONE__'"
        cur.execute(f"""
            SELECT NAME, {bc} AS CLOSINGBALANCE FROM mst_ledger
            WHERE PARENT IN ({_dr_ph})
            AND {bc} IS NOT NULL AND {bc} != ''
            AND CAST({bc} AS REAL) < 0
            ORDER BY CAST({bc} AS REAL) ASC
        """, _dr_groups)
        # In Tally, debtors normally have positive (debit) balance
        # Negative = credit balance = unusual
        flagged = []
        for row in cur.fetchall():
            try:
                bal = float(row["CLOSINGBALANCE"])
                if bal < 0:
                    flagged.append({
                        "party": row["NAME"],
                        "credit_balance": round(abs(bal), 2),
                    })
            except:
                continue

        return {
            "check": "Credit Balance in Debtors",
            "severity": "Medium",
            "flag_count": len(flagged),
            "status": "fail" if flagged else "pass",
            "parties": flagged[:30],
            "description": "Debtors with credit balance indicate advance receipts not adjusted or fictitious debtors."
        }
    except Exception as e:
        return {"check": "Credit Balance in Debtors", "severity": "Medium", "flag_count": 0,
                "status": "error", "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
# 10. PERIOD-END JOURNAL ENTRIES
# ════════════════════════════════════════════════════════════════════════════

def check_period_end_journals(conn):
    """Flag journal entries in the last 3 days of each quarter — window dressing risk."""
    cur = conn.cursor()
    try:
        # Quarter-end dates: Jun 28-30, Sep 28-30, Dec 29-31, Mar 29-31
        quarter_end_days = {
            "06": [28, 29, 30],
            "09": [28, 29, 30],
            "12": [29, 30, 31],
            "03": [29, 30, 31],
        }

        has_narration = column_exists(conn, "trn_voucher", "NARRATION")
        narration_sel = ", v.NARRATION" if has_narration else ""
        cur.execute(f"""
            SELECT v.GUID, v.DATE, v.VOUCHERNUMBER, v.PARTYLEDGERNAME{narration_sel},
                   a.LEDGERNAME, a.AMOUNT
            FROM trn_voucher v
            JOIN trn_accounting a ON v.GUID = a.VOUCHER_GUID
            WHERE v.VOUCHERTYPENAME = 'Journal'
            AND v.DATE IS NOT NULL AND v.DATE != ''
            ORDER BY v.DATE
        """)

        flagged = []
        seen_guids = set()
        for row in cur.fetchall():
            try:
                date_str = str(row["DATE"])
                if len(date_str) != 8:
                    continue
                month = date_str[4:6]
                day = int(date_str[6:8])
                if month in quarter_end_days and day in quarter_end_days[month]:
                    guid = row["GUID"]
                    if guid not in seen_guids:
                        seen_guids.add(guid)
                        amt = abs(safe_float(row["AMOUNT"]))
                        dt = datetime.strptime(date_str, "%Y%m%d").strftime("%d-%b-%Y")
                        flagged.append({
                            "date": dt,
                            "voucher_number": row["VOUCHERNUMBER"],
                            "ledger": row["LEDGERNAME"],
                            "amount": round(amt, 2),
                            "party": row["PARTYLEDGERNAME"] or "",
                            "narration": ((row["NARRATION"] or "")[:100]) if has_narration else "",
                        })
            except:
                continue

        return {
            "check": "Period-End Journal Entries",
            "severity": "Medium",
            "flag_count": len(flagged),
            "status": "fail" if flagged else "pass",
            "entries": flagged[:50],
            "description": "Journal entries in the last 3 days of each quarter may indicate window dressing or earnings manipulation."
        }
    except Exception as e:
        return {"check": "Period-End Journal Entries", "severity": "Medium", "flag_count": 0,
                "status": "error", "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
# 11. LARGE JOURNAL ENTRIES
# ════════════════════════════════════════════════════════════════════════════

def check_large_journals(conn):
    """Flag unusually large journal entries (above 3 standard deviations from mean)."""
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT a.AMOUNT
            FROM trn_accounting a
            JOIN trn_voucher v ON a.VOUCHER_GUID = v.GUID
            WHERE v.VOUCHERTYPENAME = 'Journal'
            AND a.AMOUNT IS NOT NULL AND a.AMOUNT != ''
        """)

        amounts = []
        for row in cur.fetchall():
            try:
                amounts.append(abs(float(row["AMOUNT"])))
            except:
                continue

        if len(amounts) < 10:
            return {"check": "Large Journal Entries", "severity": "Medium", "flag_count": 0,
                    "status": "skipped", "reason": f"Too few journal entries ({len(amounts)})"}

        mean = safe_divide(sum(amounts), len(amounts), 0)
        variance = safe_divide(sum((x - mean) ** 2 for x in amounts), len(amounts), 0)
        std_dev = math.sqrt(variance) if variance > 0 else 0
        threshold = mean + 3 * std_dev

        if threshold < 100000:  # Minimum threshold of 1 lakh
            threshold = 100000

        has_narration = column_exists(conn, "trn_voucher", "NARRATION")
        narration_sel = ", v.NARRATION" if has_narration else ""
        cur.execute(f"""
            SELECT v.GUID, v.DATE, v.VOUCHERNUMBER, v.PARTYLEDGERNAME{narration_sel},
                   a.LEDGERNAME, a.AMOUNT
            FROM trn_accounting a
            JOIN trn_voucher v ON a.VOUCHER_GUID = v.GUID
            WHERE v.VOUCHERTYPENAME = 'Journal'
            AND ABS(CAST(a.AMOUNT AS REAL)) > {threshold}
            ORDER BY ABS(CAST(a.AMOUNT AS REAL)) DESC
            LIMIT 30
        """)

        flagged = []
        seen_guids = set()
        for row in cur.fetchall():
            guid = row["GUID"]
            if guid in seen_guids:
                continue
            seen_guids.add(guid)
            try:
                amt = abs(float(row["AMOUNT"]))
                date_str = str(row["DATE"])
                dt = datetime.strptime(date_str, "%Y%m%d").strftime("%d-%b-%Y") if len(date_str) == 8 else date_str
                flagged.append({
                    "date": dt,
                    "voucher_number": row["VOUCHERNUMBER"],
                    "ledger": row["LEDGERNAME"],
                    "amount": round(amt, 2),
                    "party": row["PARTYLEDGERNAME"] or "",
                    "narration": ((row["NARRATION"] or "")[:100]) if has_narration else "",
                    "std_devs_from_mean": round(safe_divide(amt - mean, std_dev, 0), 1),
                })
            except:
                continue

        return {
            "check": "Large Journal Entries",
            "severity": "Medium",
            "flag_count": len(flagged),
            "status": "fail" if flagged else "pass",
            "entries": flagged,
            "threshold": round(threshold, 2),
            "mean": round(mean, 2),
            "std_dev": round(std_dev, 2),
            "description": "Journal entries above 3 standard deviations from the mean may indicate significant manual adjustments."
        }
    except Exception as e:
        return {"check": "Large Journal Entries", "severity": "Medium", "flag_count": 0,
                "status": "error", "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    import json

    db = sys.argv[1] if len(sys.argv) > 1 else "tally_data.db"
    print(f"Running audit checks on: {db}")
    print("=" * 60)

    results = run_all_checks(db)

    summary = results.pop("_summary")
    print(f"\n{'=' * 60}")
    print(f"  AUDIT RED FLAG REPORT")
    print(f"{'=' * 60}")
    print(f"  Total Checks Run:    {summary['total_checks']}")
    print(f"  Total Flags Found:   {summary['total_flags']}")
    print(f"  High Severity:       {summary['high_severity_checks']} checks flagged")
    print(f"  Medium Severity:     {summary['medium_severity_checks']} checks flagged")
    print(f"  Risk Score:          {summary['risk_score']}/100")
    print(f"{'=' * 60}\n")

    for check_name, result in results.items():
        status_icon = "[PASS]" if result.get("status") == "pass" else "[FAIL]" if result.get("status") == "fail" else "[SKIP]" if result.get("status") == "skipped" else "[ERR]"
        severity = result.get("severity", "")
        flags = result.get("flag_count", 0)
        check = result.get("check", check_name)

        print(f"  {status_icon} {check}")
        print(f"         Severity: {severity} | Flags: {flags}")

        if result.get("status") == "fail":
            # Print some details
            if "deviations" in result and result["deviations"]:
                for d in result["deviations"][:3]:
                    print(f"         Digit {d['digit']}: expected {d['expected_pct']}%, observed {d['observed_pct']}% ({d['direction']})")
            if "exact_duplicates" in result:
                for d in result["exact_duplicates"][:3]:
                    print(f"         {d['voucher_number']} - {d['party']} (x{d['duplicate_count']})")
            if "breaches" in result:
                for b in result["breaches"][:3]:
                    print(f"         {b['date']} - Rs {b['amount']:,.0f} - {b['party']} ({b['direction']})")
            if "negative_dates" in result:
                for nd in result["negative_dates"][:3]:
                    print(f"         {nd['date']} - {nd['cash_ledger']} = Rs {nd['balance']:,.0f}")
            if "parties" in result:
                for p in result["parties"][:3]:
                    bal_key = "debit_balance" if "debit_balance" in p else "credit_balance"
                    print(f"         {p['party']} - Rs {p[bal_key]:,.0f}")

        print()
