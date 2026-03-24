"""
Seven Labs Vision — Analytics Engine
Comprehensive business analytics, cash flow, and projections.
Defensive coding: works with ANY company's Tally data.
"""

import sqlite3
import os
from datetime import datetime, timedelta
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), "tally_data.db")

# Import flag-based group classification
from tally_reports import get_groups_by_nature

# ── SHARED DEFENSIVE UTILITIES ────────────────────────────────────────────
_TABLE_COLS = {}


def _get_cols(conn, table):
    """Return set of column names for a table (cached per session)."""
    if table not in _TABLE_COLS:
        try:
            _TABLE_COLS[table] = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        except sqlite3.OperationalError:
            _TABLE_COLS[table] = set()
    return _TABLE_COLS[table]


def _table_exists(conn, table):
    """Check if a table exists in the database."""
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        return row is not None
    except sqlite3.OperationalError:
        return False


def _safe_fetchone_val(cur, default=0):
    """Safely get value from fetchone, returning default if None."""
    row = cur.fetchone()
    if row is None or row[0] is None:
        return default
    return row[0]


def _get_all_groups_under(conn, root_groups):
    """Recursively get all group names under any of the root groups (inclusive).
    E.g., _get_all_groups_under(conn, ['Sales Accounts']) returns
    ['Sales Accounts', 'SALE OFFLINE', 'SALES ONLINE', ...]
    """
    if not _table_exists(conn, "mst_group"):
        return list(root_groups) if isinstance(root_groups, (list, tuple)) else [root_groups]
    if isinstance(root_groups, str):
        root_groups = [root_groups]
    result = []
    queue = list(root_groups)
    while queue:
        current = queue.pop(0)
        if current not in result:
            result.append(current)
            try:
                children = conn.execute(
                    "SELECT NAME FROM mst_group WHERE PARENT = ?", (current,)
                ).fetchall()
            except Exception:
                children = []
            for (child,) in children:
                if child and child not in result:
                    queue.append(child)
    return result


def _group_placeholders(conn, root_groups):
    """Return (placeholders_sql, group_list) for use in IN clauses."""
    groups = _get_all_groups_under(conn, root_groups)
    return ",".join(["?"] * len(groups)), groups


def _nature_placeholders(conn, nature):
    """Return (placeholders_sql, group_list) using Tally's own flag-based classification."""
    groups = get_groups_by_nature(conn, nature)
    if not groups:
        return "'__NONE__'", []
    return ",".join(["?"] * len(groups)), groups


def _detect_receipt_payment_types(conn):
    """Dynamically detect Receipt and Payment voucher types.
    Returns (receipt_types, payment_types) as lists of voucher type names.
    Uses the Tally group hierarchy: Receipt types have parent 'Receipt',
    Payment types have parent 'Payment' in mst_voucher_type.
    Falls back to name-based detection if mst_voucher_type is not available.
    """
    receipt_types = []
    payment_types = []

    if _table_exists(conn, "mst_voucher_type"):
        try:
            rows = conn.execute("SELECT NAME, PARENT FROM mst_voucher_type").fetchall()
            for name, parent in rows:
                if not name:
                    continue
                upper_parent = (parent or "").upper()
                upper_name = name.upper()
                if upper_parent == "RECEIPT" or upper_name == "RECEIPT":
                    receipt_types.append(name)
                elif upper_parent == "PAYMENT" or upper_name == "PAYMENT":
                    payment_types.append(name)
        except sqlite3.OperationalError:
            pass

    # Fallback: check actual voucher data for common names
    if not receipt_types or not payment_types:
        try:
            vch_types = conn.execute(
                "SELECT DISTINCT VOUCHERTYPENAME FROM trn_voucher WHERE VOUCHERTYPENAME IS NOT NULL"
            ).fetchall()
            for (vt,) in vch_types:
                upper = vt.upper()
                if not receipt_types and "RECEIPT" in upper:
                    receipt_types.append(vt)
                if not payment_types and "PAYMENT" in upper:
                    payment_types.append(vt)
        except sqlite3.OperationalError:
            pass

    return receipt_types, payment_types


def clear_col_cache():
    """Clear the column cache (call after re-sync)."""
    _TABLE_COLS.clear()


def get_conn():
    return sqlite3.connect(DB_PATH)


# ── MONTHLY SALES & PURCHASE ────────────────────────────────────────────────

def monthly_sales(conn, date_from=None, date_to=None, voucher_types=None):
    """Monthly sales with count, amount, avg invoice value.

    Optional filters:
        voucher_types: list of voucher type names to include
    """
    date_filter = ""
    params = []
    if date_from:
        date_filter += " AND v.DATE >= ?"
        params.append(date_from)
    if date_to:
        date_filter += " AND v.DATE <= ?"
        params.append(date_to)
    if voucher_types:
        ph = ",".join(["?"] * len(voucher_types))
        date_filter += f" AND v.VOUCHERTYPENAME IN ({ph})"
        params.extend(voucher_types)
    sales_ph, sales_groups = _nature_placeholders(conn, 'sales')
    try:
        return conn.execute(f"""
            SELECT SUBSTR(v.DATE,1,6) as month,
                   COUNT(DISTINCT v.GUID) as vch_count,
                   SUM(ABS(CAST(a.AMOUNT AS REAL))) as sales_amt
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT IN ({sales_ph}){date_filter}
            GROUP BY month ORDER BY month
        """, sales_groups + params).fetchall()
    except sqlite3.OperationalError:
        return []


def monthly_purchases(conn, date_from=None, date_to=None, voucher_types=None):
    """Monthly purchases.

    Optional filters:
        voucher_types: list of voucher type names to include
    """
    date_filter = ""
    params = []
    if date_from:
        date_filter += " AND v.DATE >= ?"
        params.append(date_from)
    if date_to:
        date_filter += " AND v.DATE <= ?"
        params.append(date_to)
    if voucher_types:
        ph = ",".join(["?"] * len(voucher_types))
        date_filter += f" AND v.VOUCHERTYPENAME IN ({ph})"
        params.extend(voucher_types)
    purch_ph, purch_groups = _nature_placeholders(conn, 'purchase')
    try:
        return conn.execute(f"""
            SELECT SUBSTR(v.DATE,1,6) as month,
                   COUNT(DISTINCT v.GUID) as vch_count,
                   SUM(ABS(CAST(a.AMOUNT AS REAL))) as amt
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT IN ({purch_ph}){date_filter}
            GROUP BY month ORDER BY month
        """, purch_groups + params).fetchall()
    except sqlite3.OperationalError:
        return []


def monthly_receipts_payments(conn, date_from=None, date_to=None):
    """Monthly cash inflows (receipts) and outflows (payments).
    Dynamically detects Receipt/Payment voucher types.
    """
    receipt_types, payment_types = _detect_receipt_payment_types(conn)

    date_filter = ""
    params = []
    if date_from:
        date_filter += " AND v.DATE >= ?"
        params.append(date_from)
    if date_to:
        date_filter += " AND v.DATE <= ?"
        params.append(date_to)

    acct_cols = _get_cols(conn, "trn_accounting")
    has_deemed = "ISDEEMEDPOSITIVE" in acct_cols

    receipts = []
    if receipt_types:
        rph = ",".join(["?"] * len(receipt_types))
        deemed_filter = " AND a.ISDEEMEDPOSITIVE = 'Yes'" if has_deemed else ""
        try:
            receipts = conn.execute(f"""
                SELECT SUBSTR(v.DATE,1,6) as month,
                       COUNT(DISTINCT v.GUID) as cnt,
                       SUM(ABS(CAST(a.AMOUNT AS REAL))) as amt
                FROM trn_voucher v
                JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
                WHERE v.VOUCHERTYPENAME IN ({rph}){deemed_filter}{date_filter}
                GROUP BY month ORDER BY month
            """, receipt_types + params).fetchall()
        except sqlite3.OperationalError:
            receipts = []

    payments = []
    if payment_types:
        pph = ",".join(["?"] * len(payment_types))
        deemed_filter = " AND a.ISDEEMEDPOSITIVE = 'Yes'" if has_deemed else ""
        try:
            payments = conn.execute(f"""
                SELECT SUBSTR(v.DATE,1,6) as month,
                       COUNT(DISTINCT v.GUID) as cnt,
                       SUM(ABS(CAST(a.AMOUNT AS REAL))) as amt
                FROM trn_voucher v
                JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
                WHERE v.VOUCHERTYPENAME IN ({pph}){deemed_filter}{date_filter}
                GROUP BY month ORDER BY month
            """, payment_types + params).fetchall()
        except sqlite3.OperationalError:
            payments = []

    return receipts, payments


def monthly_expenses(conn, date_from=None, date_to=None, voucher_types=None):
    """Monthly indirect expenses breakdown.

    Optional filters:
        voucher_types: list of voucher type names to include
    """
    date_filter = ""
    params = []
    if date_from:
        date_filter += " AND v.DATE >= ?"
        params.append(date_from)
    if date_to:
        date_filter += " AND v.DATE <= ?"
        params.append(date_to)
    if voucher_types:
        ph = ",".join(["?"] * len(voucher_types))
        date_filter += f" AND v.VOUCHERTYPENAME IN ({ph})"
        params.extend(voucher_types)
    ie_ph, ie_groups = _nature_placeholders(conn, 'indirect_expense')
    try:
        return conn.execute(f"""
            SELECT SUBSTR(v.DATE,1,6) as month,
                   a.LEDGERNAME,
                   SUM(ABS(CAST(a.AMOUNT AS REAL))) as amt
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT IN ({ie_ph}) AND CAST(a.AMOUNT AS REAL) < 0{date_filter}
            GROUP BY month, a.LEDGERNAME
            ORDER BY month, amt DESC
        """, ie_groups + params).fetchall()
    except sqlite3.OperationalError:
        return []


def monthly_gross_profit(conn, date_from=None, date_to=None, voucher_types=None):
    """Monthly P&L summary: sales, purchases, gross profit, expenses, net profit.

    Optional filters:
        voucher_types: list of voucher type names to include
    """
    sales_rows = monthly_sales(conn, date_from=date_from, date_to=date_to,
                               voucher_types=voucher_types)
    sales = {r[0]: r[2] for r in sales_rows if r[0] and r[2]}
    purchases_rows = monthly_purchases(conn, date_from=date_from, date_to=date_to,
                                       voucher_types=voucher_types)
    purchases = {r[0]: r[2] for r in purchases_rows if r[0] and r[2]}

    # Monthly indirect expenses total
    date_filter = ""
    _params = []
    if date_from:
        date_filter += " AND v.DATE >= ?"
        _params.append(date_from)
    if date_to:
        date_filter += " AND v.DATE <= ?"
        _params.append(date_to)
    if voucher_types:
        ph = ",".join(["?"] * len(voucher_types))
        date_filter += f" AND v.VOUCHERTYPENAME IN ({ph})"
        _params.extend(voucher_types)
    ie_ph2, ie_groups2 = _nature_placeholders(conn, 'indirect_expense')
    try:
        exp_rows = conn.execute(f"""
            SELECT SUBSTR(v.DATE,1,6) as month,
                   SUM(ABS(CAST(a.AMOUNT AS REAL))) as amt
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT IN ({ie_ph2}) AND CAST(a.AMOUNT AS REAL) < 0{date_filter}
            GROUP BY month ORDER BY month
        """, ie_groups2 + _params).fetchall()
    except sqlite3.OperationalError:
        exp_rows = []
    expenses = {r[0]: r[1] for r in exp_rows if r[0] and r[1]}

    months = sorted(set(list(sales.keys()) + list(purchases.keys())))
    result = []
    for m in months:
        s = sales.get(m, 0) or 0
        p = purchases.get(m, 0) or 0
        e = expenses.get(m, 0) or 0
        gp = s - p
        np_ = gp - e
        result.append({
            "month": m,
            "sales": s,
            "purchases": p,
            "gross_profit": gp,
            "indirect_expenses": e,
            "net_profit": np_,
            "gp_margin": (gp / s * 100) if s > 0 else 0,
            "np_margin": (np_ / s * 100) if s > 0 else 0,
        })
    return result


# ── PARTY ANALYSIS ──────────────────────────────────────────────────────────

def top_customers_by_sales(conn, limit=15, date_from=None, date_to=None,
                           voucher_types=None):
    """Top customers by total sales value.

    Optional filters:
        voucher_types: list of voucher type names to include
    """
    vcols = _get_cols(conn, "trn_voucher")
    if "PARTYLEDGERNAME" not in vcols:
        return []

    date_filter = ""
    params = []
    if date_from:
        date_filter += " AND v.DATE >= ?"
        params.append(date_from)
    if date_to:
        date_filter += " AND v.DATE <= ?"
        params.append(date_to)
    if voucher_types:
        ph = ",".join(["?"] * len(voucher_types))
        date_filter += f" AND v.VOUCHERTYPENAME IN ({ph})"
        params.extend(voucher_types)
    params.append(limit)
    s_ph, s_groups = _nature_placeholders(conn, 'sales')
    try:
        return conn.execute(f"""
            SELECT v.PARTYLEDGERNAME as party,
                   COUNT(DISTINCT v.GUID) as invoice_count,
                   SUM(ABS(CAST(a.AMOUNT AS REAL))) as total_sales
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT IN ({s_ph})
              AND v.PARTYLEDGERNAME IS NOT NULL AND v.PARTYLEDGERNAME != ''{date_filter}
            GROUP BY party ORDER BY total_sales DESC LIMIT ?
        """, s_groups + params).fetchall()
    except sqlite3.OperationalError:
        return []


def top_suppliers_by_purchase(conn, limit=15, date_from=None, date_to=None,
                              voucher_types=None):
    """Top suppliers by purchase value.

    Optional filters:
        voucher_types: list of voucher type names to include
    """
    vcols = _get_cols(conn, "trn_voucher")
    if "PARTYLEDGERNAME" not in vcols:
        return []

    date_filter = ""
    params = []
    if date_from:
        date_filter += " AND v.DATE >= ?"
        params.append(date_from)
    if date_to:
        date_filter += " AND v.DATE <= ?"
        params.append(date_to)
    if voucher_types:
        ph = ",".join(["?"] * len(voucher_types))
        date_filter += f" AND v.VOUCHERTYPENAME IN ({ph})"
        params.extend(voucher_types)
    params.append(limit)
    p_ph, p_groups = _nature_placeholders(conn, 'purchase')
    try:
        return conn.execute(f"""
            SELECT v.PARTYLEDGERNAME as party,
                   COUNT(DISTINCT v.GUID) as invoice_count,
                   SUM(ABS(CAST(a.AMOUNT AS REAL))) as total_purchases
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT IN ({p_ph})
              AND v.PARTYLEDGERNAME IS NOT NULL AND v.PARTYLEDGERNAME != ''{date_filter}
            GROUP BY party ORDER BY total_purchases DESC LIMIT ?
        """, p_groups + params).fetchall()
    except sqlite3.OperationalError:
        return []


def customer_monthly_sales(conn, top_n=5, date_from=None, date_to=None):
    """Monthly sales for top N customers."""
    top = top_customers_by_sales(conn, top_n, date_from=date_from, date_to=date_to)
    if not top:
        return {}
    top_names = [r[0] for r in top if r[0]]

    date_filter = ""
    params_extra = []
    if date_from:
        date_filter += " AND v.DATE >= ?"
        params_extra.append(date_from)
    if date_to:
        date_filter += " AND v.DATE <= ?"
        params_extra.append(date_to)

    s_ph, s_groups = _nature_placeholders(conn, 'sales')
    result = {}
    for name in top_names:
        try:
            rows = conn.execute(f"""
                SELECT SUBSTR(v.DATE,1,6) as month,
                       SUM(ABS(CAST(a.AMOUNT AS REAL))) as amt
                FROM trn_voucher v
                JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
                JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
                WHERE l.PARENT IN ({s_ph}) AND v.PARTYLEDGERNAME = ?{date_filter}
                GROUP BY month ORDER BY month
            """, s_groups + [name] + params_extra).fetchall()
            result[name] = {r[0]: r[1] for r in rows}
        except sqlite3.OperationalError:
            result[name] = {}
    return result


# ── BANK ANALYSIS ───────────────────────────────────────────────────────────

def bank_balances(conn, date_from=None, date_to=None):
    """All bank account balances."""
    lcols = _get_cols(conn, "mst_ledger")
    has_ob = "OPENINGBALANCE" in lcols
    has_cb = "CLOSINGBALANCE" in lcols

    _bank_all = get_groups_by_nature(conn, 'bank') + get_groups_by_nature(conn, 'bank_od') + get_groups_by_nature(conn, 'cash')
    bank_groups = list(dict.fromkeys(_bank_all))
    bank_ph = ",".join(["?"] * len(bank_groups)) if bank_groups else "'__NONE__'"

    if date_from or date_to:
        date_cond = ""
        params = []
        if date_from:
            date_cond += " AND v.DATE >= ?"
            params.append(date_from)
        if date_to:
            date_cond += " AND v.DATE <= ?"
            params.append(date_to)
        ob_expr = "CAST(l.OPENINGBALANCE AS REAL)" if has_ob else "0"
        try:
            return conn.execute(f"""
                SELECT l.NAME, l.PARENT,
                       {ob_expr} as opening,
                       COALESCE({ob_expr}, 0) +
                       COALESCE((
                           SELECT SUM(CAST(a.AMOUNT AS REAL))
                           FROM trn_accounting a
                           JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                           WHERE a.LEDGERNAME = l.NAME{date_cond}
                       ), 0) as closing
                FROM mst_ledger l
                WHERE l.PARENT IN ({bank_ph})
                ORDER BY ABS(closing) DESC
            """, params + bank_groups).fetchall()
        except sqlite3.OperationalError:
            return []

    if has_cb:
        try:
            return conn.execute(f"""
                SELECT NAME, PARENT,
                       CAST(OPENINGBALANCE AS REAL) as opening,
                       CAST(CLOSINGBALANCE AS REAL) as closing
                FROM mst_ledger
                WHERE PARENT IN ({bank_ph})
                ORDER BY ABS(CAST(CLOSINGBALANCE AS REAL)) DESC
            """, bank_groups).fetchall()
        except sqlite3.OperationalError:
            return []
    else:
        # No closing balance column — compute from transactions
        try:
            ob_expr = "CAST(l.OPENINGBALANCE AS REAL)" if has_ob else "0"
            return conn.execute(f"""
                SELECT l.NAME, l.PARENT,
                       {ob_expr} as opening,
                       COALESCE({ob_expr}, 0) +
                       COALESCE((
                           SELECT SUM(CAST(a.AMOUNT AS REAL))
                           FROM trn_accounting a
                           WHERE a.LEDGERNAME = l.NAME
                       ), 0) as closing
                FROM mst_ledger l
                WHERE l.PARENT IN ({bank_ph})
                ORDER BY ABS(closing) DESC
            """, bank_groups).fetchall()
        except sqlite3.OperationalError:
            return []


def monthly_bank_movement(conn, date_from=None, date_to=None):
    """Monthly net movement through bank accounts."""
    date_filter = ""
    params = []
    if date_from:
        date_filter += " AND v.DATE >= ?"
        params.append(date_from)
    if date_to:
        date_filter += " AND v.DATE <= ?"
        params.append(date_to)
    _bm_all = get_groups_by_nature(conn, 'bank') + get_groups_by_nature(conn, 'bank_od')
    bm_groups = list(dict.fromkeys(_bm_all))
    bm_ph = ",".join(["?"] * len(bm_groups)) if bm_groups else "'__NONE__'"
    try:
        return conn.execute(f"""
            SELECT SUBSTR(v.DATE,1,6) as month,
                   a.LEDGERNAME,
                   SUM(CASE WHEN CAST(a.AMOUNT AS REAL) < 0 THEN ABS(CAST(a.AMOUNT AS REAL)) ELSE 0 END) as debits,
                   SUM(CASE WHEN CAST(a.AMOUNT AS REAL) > 0 THEN ABS(CAST(a.AMOUNT AS REAL)) ELSE 0 END) as credits
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT IN ({bm_ph}){date_filter}
            GROUP BY month, a.LEDGERNAME
            ORDER BY month
        """, bm_groups + params).fetchall()
    except sqlite3.OperationalError:
        return []


# ── CASH FLOW STATEMENT ────────────────────────────────────────────────────

def _safe_sum_by_group(conn, parent_group, month):
    """Safely get sum of amounts for a ledger group in a month (recursive sub-groups)."""
    ph, groups = _group_placeholders(conn, [parent_group])
    try:
        row = conn.execute(f"""
            SELECT SUM(CAST(a.AMOUNT AS REAL))
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT IN ({ph}) AND SUBSTR(v.DATE,1,6) = ?
        """, groups + [month]).fetchone()
        return (row[0] or 0) if row else 0
    except sqlite3.OperationalError:
        return 0


def _safe_sum_by_groups(conn, parent_groups, month):
    """Safely get sum of amounts for multiple ledger groups in a month (recursive sub-groups)."""
    if not parent_groups:
        return 0
    ph, groups = _group_placeholders(conn, list(parent_groups))
    try:
        row = conn.execute(f"""
            SELECT SUM(CAST(a.AMOUNT AS REAL))
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT IN ({ph}) AND SUBSTR(v.DATE,1,6) = ?
        """, groups + [month]).fetchone()
        return (row[0] or 0) if row else 0
    except sqlite3.OperationalError:
        return 0


def cash_flow_statement(conn, date_from=None, date_to=None):
    """
    Indirect method cash flow statement.
    Operating: Net Profit + Non-cash adjustments + Working capital changes
    Investing: Fixed asset movements
    Financing: Loan movements, capital changes
    """
    months_data = monthly_gross_profit(conn, date_from=date_from, date_to=date_to)
    if not months_data:
        return []

    receipt_types, payment_types = _detect_receipt_payment_types(conn)
    acct_cols = _get_cols(conn, "trn_accounting")
    has_deemed = "ISDEEMEDPOSITIVE" in acct_cols

    # Operating activities - monthly
    monthly_cf = []

    for md in months_data:
        month = md["month"]
        net_profit = md["net_profit"] or 0

        debtor_movement = _safe_sum_by_group(conn, 'Sundry Debtors', month)
        creditor_movement = _safe_sum_by_group(conn, 'Sundry Creditors', month)
        tax_movement = _safe_sum_by_group(conn, 'Duties & Taxes', month)
        bank_cash_movement = _safe_sum_by_groups(
            conn, ['Bank Accounts', 'Bank OD A/c', 'Cash-in-Hand'], month
        )
        loan_movement = _safe_sum_by_groups(
            conn, ['Secured Loans', 'Unsecured Loans', 'Loans (Liability)'], month
        )

        # Receipts for the month
        receipts = 0
        if receipt_types:
            rph = ",".join(["?"] * len(receipt_types))
            deemed_filter = " AND a.ISDEEMEDPOSITIVE = 'Yes'" if has_deemed else ""
            try:
                row = conn.execute(f"""
                    SELECT COALESCE(SUM(ABS(CAST(a.AMOUNT AS REAL))), 0)
                    FROM trn_voucher v
                    JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
                    WHERE v.VOUCHERTYPENAME IN ({rph}){deemed_filter}
                      AND SUBSTR(v.DATE,1,6) = ?
                """, receipt_types + [month]).fetchone()
                receipts = (row[0] or 0) if row else 0
            except sqlite3.OperationalError:
                receipts = 0

        # Payments for the month
        payments = 0
        if payment_types:
            pph = ",".join(["?"] * len(payment_types))
            deemed_filter = " AND a.ISDEEMEDPOSITIVE = 'Yes'" if has_deemed else ""
            try:
                row = conn.execute(f"""
                    SELECT COALESCE(SUM(ABS(CAST(a.AMOUNT AS REAL))), 0)
                    FROM trn_voucher v
                    JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
                    WHERE v.VOUCHERTYPENAME IN ({pph}){deemed_filter}
                      AND SUBSTR(v.DATE,1,6) = ?
                """, payment_types + [month]).fetchone()
                payments = (row[0] or 0) if row else 0
            except sqlite3.OperationalError:
                payments = 0

        operating_cf = net_profit - debtor_movement + creditor_movement + tax_movement

        monthly_cf.append({
            "month": month,
            "net_profit": net_profit,
            "debtor_change": debtor_movement,
            "creditor_change": creditor_movement,
            "tax_change": tax_movement,
            "loan_change": loan_movement,
            "receipts": receipts,
            "payments": payments,
            "net_cash_flow": receipts - payments,
            "bank_cash_change": bank_cash_movement,
            "operating_cf": operating_cf,
        })

    return monthly_cf


def project_cash_flow(conn, months_ahead=3, date_from=None, date_to=None):
    """Project future cash flows based on historical trends."""
    cf = cash_flow_statement(conn, date_from=date_from, date_to=date_to)
    if len(cf) < 3:
        return []

    # Use last 3 months weighted average for projection
    recent = cf[-3:]

    # Weighted: most recent month gets 3x, 2nd gets 2x, 3rd gets 1x
    weights = [1, 2, 3]
    total_weight = sum(weights)

    def weighted_avg(key):
        vals = [(r.get(key, 0) or 0) for r in recent]
        return sum(v * w for v, w in zip(vals, weights)) / total_weight

    avg_sales = weighted_avg("net_profit") + weighted_avg("debtor_change") - weighted_avg("creditor_change")
    avg_receipts = weighted_avg("receipts")
    avg_payments = weighted_avg("payments")
    avg_net_cf = weighted_avg("net_cash_flow")
    avg_net_profit = weighted_avg("net_profit")

    # Calculate trend (is it improving or declining?)
    if len(cf) >= 6:
        first_half = cf[:len(cf)//2]
        second_half = cf[len(cf)//2:]
        first_avg = sum((r.get("net_profit", 0) or 0) for r in first_half) / max(len(first_half), 1)
        second_avg = sum((r.get("net_profit", 0) or 0) for r in second_half) / max(len(second_half), 1)
        sales_trend = second_avg - first_avg
    else:
        sales_trend = 0

    # Get last month's data
    last_month = cf[-1]["month"]
    try:
        last_year = int(last_month[:4])
        last_m = int(last_month[4:6])
    except (ValueError, IndexError):
        return []

    # Current bank + cash balance
    balances = bank_balances(conn)
    current_cash = 0
    for r in balances:
        bal = r[3] if r[3] else 0
        if bal < 0:
            current_cash += abs(bal)  # debit balances
        else:
            current_cash += bal

    projections = []
    running_cash = current_cash

    for i in range(months_ahead):
        last_m += 1
        if last_m > 12:
            last_m = 1
            last_year += 1
        proj_month = f"{last_year}{last_m:02d}"

        # Apply slight trend adjustment — guard against zero division
        denom = max(abs(avg_net_profit), 1)
        trend_factor = 1 + (sales_trend / denom) * 0.1 * (i + 1)
        trend_factor = max(0.7, min(1.3, trend_factor))  # cap between -30% to +30%

        proj_receipts = avg_receipts * trend_factor
        proj_payments = avg_payments * trend_factor
        proj_net_cf = proj_receipts - proj_payments
        proj_net_profit = avg_net_profit * trend_factor

        running_cash += proj_net_cf

        projections.append({
            "month": proj_month,
            "projected_receipts": proj_receipts,
            "projected_payments": proj_payments,
            "projected_net_cf": proj_net_cf,
            "projected_net_profit": proj_net_profit,
            "projected_cash_balance": running_cash,
            "is_projection": True,
            "confidence": max(0.5, 0.9 - i * 0.15),  # decreasing confidence
        })

    return projections


# ── WORKING CAPITAL ─────────────────────────────────────────────────────────

def working_capital_analysis(conn, date_from=None, date_to=None):
    """Current assets vs current liabilities."""
    lcols = _get_cols(conn, "mst_ledger")
    has_cb = "CLOSINGBALANCE" in lcols

    ca_groups = ['Sundry Debtors', 'Cash-in-Hand', 'Bank Accounts', 'Bank OD A/c',
                 'Stock-in-Hand', 'Deposits (Asset)', 'Loans & Advances (Asset)']
    cl_groups = ['Sundry Creditors', 'Duties & Taxes', 'Provisions']

    def _get_group_balance(group_name):
        """Get balance for a group including all recursive sub-groups."""
        all_groups = _get_all_groups_under(conn, [group_name])
        if not all_groups:
            return 0
        ph = ",".join(["?"] * len(all_groups))
        if has_cb:
            try:
                row = conn.execute(f"""
                    SELECT COALESCE(SUM(ABS(CAST(CLOSINGBALANCE AS REAL))), 0)
                    FROM mst_ledger WHERE PARENT IN ({ph})
                """, list(all_groups)).fetchone()
                return (row[0] or 0) if row else 0
            except sqlite3.OperationalError:
                return 0
        else:
            # Compute from opening + transactions
            has_ob = "OPENINGBALANCE" in lcols
            ob_expr = "CAST(l.OPENINGBALANCE AS REAL)" if has_ob else "0"
            try:
                row = conn.execute(f"""
                    SELECT COALESCE(SUM(ABS(
                        COALESCE({ob_expr}, 0) +
                        COALESCE((SELECT SUM(CAST(a.AMOUNT AS REAL))
                                  FROM trn_accounting a WHERE a.LEDGERNAME = l.NAME), 0)
                    )), 0)
                    FROM mst_ledger l WHERE l.PARENT IN ({ph})
                """, list(all_groups)).fetchone()
                return (row[0] or 0) if row else 0
            except sqlite3.OperationalError:
                return 0

    ca = {}
    for g in ca_groups:
        val = _get_group_balance(g)
        if val > 0:
            ca[g] = val

    cl = {}
    for g in cl_groups:
        val = _get_group_balance(g)
        if val > 0:
            cl[g] = val

    total_ca = sum(ca.values())
    total_cl = sum(cl.values())

    return {
        "current_assets": ca,
        "current_liabilities": cl,
        "total_ca": total_ca,
        "total_cl": total_cl,
        "working_capital": total_ca - total_cl,
        "current_ratio": (total_ca / total_cl) if total_cl > 0 else 0,
    }


# ── KEY RATIOS ──────────────────────────────────────────────────────────────

def key_ratios(conn, date_from=None, date_to=None):
    """Calculate key financial ratios."""
    try:
        from tally_reports import profit_and_loss, balance_sheet
    except ImportError:
        return {}

    try:
        pl = profit_and_loss(conn, from_date=date_from, to_date=date_to)
        bs = balance_sheet(conn, date_from=date_from, date_to=date_to)
    except Exception:
        return {}

    wc = working_capital_analysis(conn, date_from=date_from, date_to=date_to)

    total_sales = pl.get("total_income", 0) or 0
    total_expenses = pl.get("total_expense", 0) or 0
    net_profit = pl.get("net_profit", 0) or 0
    gross_profit = pl.get("gross_profit", 0) or 0
    total_assets = bs.get("total_assets", 0) or 0
    total_liabilities = bs.get("total_liabilities", 0) or 0

    # Debtor/creditor balances (recursive sub-groups)
    lcols = _get_cols(conn, "mst_ledger")
    if "CLOSINGBALANCE" in lcols:
        try:
            d_ph, d_groups = _nature_placeholders(conn, 'debtors')
            total_debtors = _safe_fetchone_val(conn.execute(f"""
                SELECT COALESCE(SUM(ABS(CAST(CLOSINGBALANCE AS REAL))), 0)
                FROM mst_ledger WHERE PARENT IN ({d_ph})
            """, d_groups))
            c_ph, c_groups = _nature_placeholders(conn, 'creditors')
            total_creditors = _safe_fetchone_val(conn.execute(f"""
                SELECT COALESCE(SUM(ABS(CAST(CLOSINGBALANCE AS REAL))), 0)
                FROM mst_ledger WHERE PARENT IN ({c_ph})
            """, c_groups))
        except sqlite3.OperationalError:
            total_debtors = 0
            total_creditors = 0
    else:
        total_debtors = 0
        total_creditors = 0

    # Months of data
    sales_rows = monthly_sales(conn, date_from=date_from, date_to=date_to)
    months_count = len(set(r[0] for r in sales_rows if r[0]))
    annualized_sales = (total_sales / months_count * 12) if months_count > 0 else total_sales
    annualized_purchases = (total_expenses / months_count * 12) if months_count > 0 else total_expenses

    ratios = {
        "gross_profit_margin": (gross_profit / total_sales * 100) if total_sales > 0 else 0,
        "net_profit_margin": (net_profit / total_sales * 100) if total_sales > 0 else 0,
        "current_ratio": wc.get("current_ratio", 0),
        "working_capital": wc.get("working_capital", 0),
        "debtor_days": (total_debtors / annualized_sales * 365) if annualized_sales > 0 else 0,
        "creditor_days": (total_creditors / annualized_purchases * 365) if annualized_purchases > 0 else 0,
        "total_debtors": total_debtors,
        "total_creditors": total_creditors,
        "roa": (net_profit / total_assets * 100) if total_assets > 0 else 0,
    }

    return ratios


# ── COLLECTION EFFICIENCY ───────────────────────────────────────────────────

def collection_efficiency(conn, date_from=None, date_to=None):
    """Monthly collection efficiency: receipts as % of opening debtors + sales.
    Dynamically detects Receipt voucher types.
    """
    sales_data = {r[0]: r[2] for r in monthly_sales(conn, date_from=date_from, date_to=date_to) if r[0]}
    receipt_types, _ = _detect_receipt_payment_types(conn)
    acct_cols = _get_cols(conn, "trn_accounting")
    has_deemed = "ISDEEMEDPOSITIVE" in acct_cols

    receipt_data = {}
    date_filter = ""
    _params = []
    if date_from:
        date_filter += " AND v.DATE >= ?"
        _params.append(date_from)
    if date_to:
        date_filter += " AND v.DATE <= ?"
        _params.append(date_to)

    if receipt_types:
        rph = ",".join(["?"] * len(receipt_types))
        deemed_filter = " AND a.ISDEEMEDPOSITIVE = 'Yes'" if has_deemed else ""
        try:
            rows = conn.execute(f"""
                SELECT SUBSTR(v.DATE,1,6) as month,
                       SUM(ABS(CAST(a.AMOUNT AS REAL))) as amt
                FROM trn_voucher v
                JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
                WHERE v.VOUCHERTYPENAME IN ({rph}){deemed_filter}{date_filter}
                GROUP BY month ORDER BY month
            """, receipt_types + _params).fetchall()
            for r in rows:
                if r[0]:
                    receipt_data[r[0]] = r[1]
        except sqlite3.OperationalError:
            pass

    months = sorted(set(list(sales_data.keys()) + list(receipt_data.keys())))
    result = []
    for m in months:
        s = sales_data.get(m, 0) or 0
        r = receipt_data.get(m, 0) or 0
        eff = (r / s * 100) if s > 0 else 0
        result.append({"month": m, "sales": s, "collections": r, "efficiency": eff})
    return result


# ── DRILL-DOWN QUERIES ─────────────────────────────────────────────────────

def drill_monthly_invoices(conn, month_code, ledger_parent):
    """Get all invoices for a given month and ledger parent (Sales Accounts / Purchase Accounts).
    Includes all recursive sub-groups."""
    all_groups = _get_all_groups_under(conn, [ledger_parent])
    ph = ",".join(["?"] * len(all_groups))
    try:
        return conn.execute(f"""
            SELECT v.GUID, v.DATE, v.VOUCHERNUMBER, v.PARTYLEDGERNAME,
                   SUM(ABS(CAST(a.AMOUNT AS REAL))) as amount
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT IN ({ph}) AND SUBSTR(v.DATE,1,6) = ?
            GROUP BY v.GUID
            ORDER BY v.DATE
        """, list(all_groups) + [month_code]).fetchall()
    except sqlite3.OperationalError:
        return []


def drill_party_invoices(conn, party_name, ledger_parent):
    """Get all invoices for a specific party under a ledger parent.
    Includes all recursive sub-groups."""
    all_groups = _get_all_groups_under(conn, [ledger_parent])
    ph = ",".join(["?"] * len(all_groups))
    try:
        return conn.execute(f"""
            SELECT v.GUID, v.DATE, v.VOUCHERNUMBER, v.VOUCHERTYPENAME,
                   SUM(ABS(CAST(a.AMOUNT AS REAL))) as amount
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT IN ({ph}) AND v.PARTYLEDGERNAME = ?
            GROUP BY v.GUID
            ORDER BY v.DATE
        """, list(all_groups) + [party_name]).fetchall()
    except sqlite3.OperationalError:
        return []


def drill_voucher_entries(conn, voucher_guid):
    """Get all accounting entries for a specific voucher."""
    acct_cols = _get_cols(conn, "trn_accounting")
    deemed_col = "a.ISDEEMEDPOSITIVE" if "ISDEEMEDPOSITIVE" in acct_cols else "'' AS ISDEEMEDPOSITIVE"
    try:
        return conn.execute(f"""
            SELECT a.LEDGERNAME, CAST(a.AMOUNT AS REAL) as amount, {deemed_col}
            FROM trn_accounting a WHERE a.VOUCHER_GUID = ?
        """, (voucher_guid,)).fetchall()
    except sqlite3.OperationalError:
        return []


def drill_voucher_header(conn, voucher_guid):
    """Get voucher header info."""
    vcols = _get_cols(conn, "trn_voucher")
    narration_col = "v.NARRATION" if "NARRATION" in vcols else "'' AS NARRATION"
    try:
        return conn.execute(f"""
            SELECT v.GUID, v.DATE, v.VOUCHERNUMBER, v.VOUCHERTYPENAME,
                   v.PARTYLEDGERNAME, {narration_col}
            FROM trn_voucher v WHERE v.GUID = ?
        """, (voucher_guid,)).fetchone()
    except sqlite3.OperationalError:
        return None


def drill_expense_transactions(conn, ledger_name, month_code):
    """Get all transactions for an expense ledger in a month."""
    try:
        return conn.execute("""
            SELECT v.GUID, v.DATE, v.VOUCHERNUMBER, v.VOUCHERTYPENAME,
                   v.PARTYLEDGERNAME,
                   ABS(CAST(a.AMOUNT AS REAL)) as amount
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            WHERE a.LEDGERNAME = ? AND SUBSTR(v.DATE,1,6) = ?
                  AND CAST(a.AMOUNT AS REAL) < 0
            ORDER BY v.DATE
        """, (ledger_name, month_code)).fetchall()
    except sqlite3.OperationalError:
        return []


def drill_receipt_payment_vouchers(conn, month_code, voucher_type):
    """Get all receipt or payment vouchers for a month."""
    acct_cols = _get_cols(conn, "trn_accounting")
    deemed_filter = " AND a.ISDEEMEDPOSITIVE = 'Yes'" if "ISDEEMEDPOSITIVE" in acct_cols else ""
    try:
        return conn.execute(f"""
            SELECT v.GUID, v.DATE, v.VOUCHERNUMBER, v.PARTYLEDGERNAME,
                   SUM(ABS(CAST(a.AMOUNT AS REAL))) as amount
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            WHERE v.VOUCHERTYPENAME = ?{deemed_filter}
                  AND SUBSTR(v.DATE,1,6) = ?
            GROUP BY v.GUID
            ORDER BY v.DATE
        """, (voucher_type, month_code)).fetchall()
    except sqlite3.OperationalError:
        return []


def drill_bank_transactions(conn, bank_name):
    """Get all transactions for a bank account (like a bank statement)."""
    try:
        return conn.execute("""
            SELECT v.DATE, v.VOUCHERNUMBER, v.VOUCHERTYPENAME,
                   v.PARTYLEDGERNAME, v.GUID,
                   CASE WHEN CAST(a.AMOUNT AS REAL) < 0
                        THEN ABS(CAST(a.AMOUNT AS REAL)) ELSE 0 END as debit,
                   CASE WHEN CAST(a.AMOUNT AS REAL) > 0
                        THEN ABS(CAST(a.AMOUNT AS REAL)) ELSE 0 END as credit
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            WHERE a.LEDGERNAME = ?
            ORDER BY v.DATE, v.VOUCHERNUMBER
        """, (bank_name,)).fetchall()
    except sqlite3.OperationalError:
        return []
