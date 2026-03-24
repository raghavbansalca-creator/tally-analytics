"""
Monthly MIS Dashboard
Investor/Board-grade Management Information System
Interactive drill-down into P&L lines, ratios, and customer concentration.
"""

import streamlit as st
import sqlite3
import os
import sys
import datetime
import pandas as pd
import numpy as np
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from sidebar_filters import render_sidebar_filters
from tally_reports import get_groups_by_nature

# ── PAGE CONFIG ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="Monthly MIS — SLV", page_icon="M", layout="wide")

import sys as _sys2, os as _os2
_sys2.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from styles import inject_base_styles, page_header, section_header, metric_card, fmt, fmt_full, badge, footer
inject_base_styles()

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tally_data.db")


def _safe_parse_date(date_str, fallback=None):
    """Safely parse YYYYMMDD date string."""
    try:
        if date_str and len(date_str) >= 8:
            return datetime.date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
    except (ValueError, TypeError):
        pass
    return fallback


def _safe_div(a, b, default=0):
    """Safe division -- returns default if b is 0 or None."""
    if not b:
        return default
    return a / b


def _safe_cols(conn, table):
    """Return set of column names for a table."""
    try:
        return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        return set()


def _get_all_groups_under(conn, root_groups):
    """Recursively get all group names under any of the root groups (inclusive)."""
    if isinstance(root_groups, str):
        root_groups = [root_groups]
    try:
        conn.execute("SELECT 1 FROM mst_group LIMIT 1")
    except Exception:
        return list(root_groups)
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


def _group_ph(conn, root_groups):
    """Return (placeholders_sql, group_list) for use in IN clauses."""
    groups = _get_all_groups_under(conn, root_groups)
    return ",".join(["?"] * len(groups)), groups


def _nature_ph(conn, nature):
    """Return (placeholders_sql, group_list) using Tally's own flag-based classification."""
    groups = get_groups_by_nature(conn, nature)
    if not groups:
        return "'__NONE__'", []
    return ",".join(["?"] * len(groups)), groups

# ── GLOBAL DATE FILTER ──
_conn_dates = sqlite3.connect(DB_PATH)
_min_date_row = _conn_dates.execute("SELECT MIN(DATE) FROM trn_voucher").fetchone()
_max_date_row = _conn_dates.execute("SELECT MAX(DATE) FROM trn_voucher").fetchone()
_conn_dates.close()
_min_dt = _safe_parse_date(_min_date_row[0] if _min_date_row else None, fallback=datetime.date(2025, 4, 1))
_max_dt = _safe_parse_date(_max_date_row[0] if _max_date_row else None, fallback=datetime.date.today())
if "global_start_date" not in st.session_state:
    st.session_state.global_start_date = _min_dt
if "global_end_date" not in st.session_state:
    st.session_state.global_end_date = _max_dt
st.sidebar.markdown("### Date Range")
_from = st.sidebar.date_input("From", value=st.session_state.global_start_date, min_value=_min_dt, max_value=_max_dt, key="mis_filter_from")
_to = st.sidebar.date_input("To", value=st.session_state.global_end_date, min_value=_min_dt, max_value=_max_dt, key="mis_filter_to")
st.session_state.global_start_date = _from
st.session_state.global_end_date = _to
DATE_FROM = _from.strftime("%Y%m%d")
DATE_TO = _to.strftime("%Y%m%d")
st.sidebar.caption(f"Showing: {_from.strftime('%d %b %Y')} to {_to.strftime('%d %b %Y')}")
if st.sidebar.button("Reset to Full Period", key="mis_reset_dates"):
    st.session_state.global_start_date = _min_dt
    st.session_state.global_end_date = _max_dt
    st.rerun()

# ── DYNAMIC SIDEBAR FILTERS ─────────────────────────────────────────────────
_mis_filter_conn = sqlite3.connect(DB_PATH)
_filters = render_sidebar_filters(_mis_filter_conn, page_key="mis")
_vch_types_filter = _filters.get("voucher_types")
_mis_filter_conn.close()

# ── SESSION STATE FOR DRILL-DOWN ─────────────────────────────────────────────
if "mis_view" not in st.session_state:
    st.session_state.mis_view = "main"  # main, drill_detail
if "mis_drill_month" not in st.session_state:
    st.session_state.mis_drill_month = None  # "202504" etc
if "mis_drill_line" not in st.session_state:
    st.session_state.mis_drill_line = None  # "revenue", "purchases", "expense:Salary", "gross_profit", "ratio:..."
if "mis_drill_party" not in st.session_state:
    st.session_state.mis_drill_party = None


def go_drill(line, month=None, party=None):
    """Navigate to a drill-down view."""
    st.session_state.mis_view = "drill_detail"
    st.session_state.mis_drill_line = line
    st.session_state.mis_drill_month = month
    st.session_state.mis_drill_party = party


def go_back():
    """Return to main MIS view."""
    st.session_state.mis_view = "main"
    st.session_state.mis_drill_month = None
    st.session_state.mis_drill_line = None
    st.session_state.mis_drill_party = None


# ── INDIAN NUMBER FORMATTING ────────────────────────────────────────────────

def fmt_indian(n, decimals=0, prefix=""):
    """Format number in Indian numbering system (Lakhs/Crores)."""
    if n is None or (isinstance(n, float) and np.isnan(n)):
        return "—"
    negative = n < 0
    n = abs(n)
    if decimals == 0:
        s = str(int(round(n)))
    else:
        s = f"{n:.{decimals}f}"
        parts = s.split(".")
        s = parts[0]
        decimal_part = parts[1]

    # Apply Indian grouping: last 3 digits, then groups of 2
    if len(s) <= 3:
        result = s
    else:
        result = s[-3:]
        s = s[:-3]
        while s:
            result = s[-2:] + "," + result
            s = s[:-2]

    if decimals > 0:
        result = result + "." + decimal_part

    if negative:
        result = "(" + prefix + result + ")"
    else:
        result = prefix + result
    return result


def fmt_lakhs(n, decimals=1):
    """Format as Lakhs with L suffix."""
    if n is None or (isinstance(n, float) and np.isnan(n)):
        return "—"
    negative = n < 0
    n_abs = abs(n)
    val = n_abs / 100000
    if val >= 100:
        formatted = f"{val / 100:.{decimals}f} Cr"
    else:
        formatted = f"{val:.{decimals}f} L"
    return f"({formatted})" if negative else formatted


def fmt_pct(n, decimals=1):
    """Format as percentage."""
    if n is None or (isinstance(n, float) and np.isnan(n)):
        return "—"
    return f"{n:.{decimals}f}%"


def trend_arrow(current, previous):
    """Return colored arrow for trend."""
    if previous is None or current is None:
        return ""
    if previous == 0:
        return ""
    change = ((current - previous) / abs(previous)) * 100
    if change > 2:
        return f"<span style='color:#10b981;font-weight:bold'>▲ {change:+.1f}%</span>"
    elif change < -2:
        return f"<span style='color:#ef4444;font-weight:bold'>▼ {change:+.1f}%</span>"
    else:
        return f"<span style='color:#6b7280'>● {change:+.1f}%</span>"


def sparkline_bar(values, width=80, height=20):
    """Generate a tiny inline SVG sparkline bar chart."""
    if not values or all(v == 0 for v in values):
        return ""
    max_val = max(abs(v) for v in values if v != 0)
    bar_width = _safe_div(width, len(values), default=width)
    bars = []
    for i, v in enumerate(values):
        bar_h = abs(v) / max_val * height if max_val > 0 else 0
        color = "#10b981" if v >= 0 else "#ef4444"
        x = i * bar_width
        y = height - bar_h
        bars.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width * 0.8:.1f}" height="{bar_h:.1f}" fill="{color}" rx="1"/>')
    svg = f'<svg width="{width}" height="{height}" style="vertical-align:middle">' + "".join(bars) + "</svg>"
    return svg


# ── DATA LOADING ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_all_data(date_from=None, date_to=None, voucher_types_tuple=None):
    conn = sqlite3.connect(DB_PATH)
    data = {}

    # Build date filter fragment
    _df = ""
    _dp = []
    if date_from:
        _df += " AND v.DATE >= ?"
        _dp.append(date_from)
    if date_to:
        _df += " AND v.DATE <= ?"
        _dp.append(date_to)

    # Voucher type filter fragment
    _vf = ""
    _vp = []
    if voucher_types_tuple:
        _vp = list(voucher_types_tuple)
        _vf = " AND v.VOUCHERTYPENAME IN (" + ",".join(["?"] * len(_vp)) + ")"

    # Resolve groups using Tally's own flag-based classification
    _s_ph, _s_g = _nature_ph(conn, 'sales')
    _p_ph, _p_g = _nature_ph(conn, 'purchase')
    _de_ph, _de_g = _nature_ph(conn, 'direct_expense')
    _ie_ph, _ie_g = _nature_ph(conn, 'indirect_expense')

    # Monthly Sales
    rows = conn.execute(f"""
        SELECT SUBSTR(v.DATE,1,6) as month,
               COUNT(DISTINCT v.GUID) as vch_count,
               ABS(SUM(CAST(a.AMOUNT AS REAL))) as sales_amt
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT IN ({_s_ph}){_df}{_vf}
        GROUP BY month ORDER BY month
    """, _s_g + _dp + _vp).fetchall()
    data["sales"] = {r[0]: {"count": r[1], "amount": r[2]} for r in rows}

    # Monthly Purchases
    rows = conn.execute(f"""
        SELECT SUBSTR(v.DATE,1,6) as month,
               COUNT(DISTINCT v.GUID) as vch_count,
               ABS(SUM(CAST(a.AMOUNT AS REAL))) as amt
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT IN ({_p_ph}){_df}{_vf}
        GROUP BY month ORDER BY month
    """, _p_g + _dp + _vp).fetchall()
    data["purchases"] = {r[0]: {"count": r[1], "amount": r[2]} for r in rows}

    # Monthly Direct Expenses (Freight)
    rows = conn.execute(f"""
        SELECT SUBSTR(v.DATE,1,6) as month,
               a.LEDGERNAME,
               ABS(SUM(CAST(a.AMOUNT AS REAL))) as amt
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT IN ({_de_ph}) AND CAST(a.AMOUNT AS REAL) < 0{_df}{_vf}
        GROUP BY month, a.LEDGERNAME ORDER BY month
    """, _de_g + _dp + _vp).fetchall()
    direct_exp = defaultdict(lambda: defaultdict(float))
    for r in rows:
        direct_exp[r[0]][r[1]] = r[2]
    data["direct_expenses"] = dict(direct_exp)

    # Monthly Indirect Expenses (detailed by ledger)
    rows = conn.execute(f"""
        SELECT SUBSTR(v.DATE,1,6) as month,
               a.LEDGERNAME,
               ABS(SUM(CAST(a.AMOUNT AS REAL))) as amt
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT IN ({_ie_ph}) AND CAST(a.AMOUNT AS REAL) < 0{_df}{_vf}
        GROUP BY month, a.LEDGERNAME
        ORDER BY month, amt DESC
    """, _ie_g + _dp + _vp).fetchall()
    indirect_exp = defaultdict(lambda: defaultdict(float))
    all_expense_ledgers = set()
    for r in rows:
        indirect_exp[r[0]][r[1]] = r[2]
        all_expense_ledgers.add(r[1])
    data["indirect_expenses"] = dict(indirect_exp)
    data["expense_ledgers"] = sorted(all_expense_ledgers)

    # Monthly Receipts
    rows = conn.execute(f"""
        SELECT SUBSTR(v.DATE,1,6) as month,
               ABS(SUM(CAST(a.AMOUNT AS REAL))) as amt
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        WHERE v.VOUCHERTYPENAME = 'Receipt' AND a.ISDEEMEDPOSITIVE = 'Yes'{_df}
        GROUP BY month ORDER BY month
    """, _dp).fetchall()
    data["receipts"] = {r[0]: r[1] for r in rows}

    # Monthly Payments
    rows = conn.execute(f"""
        SELECT SUBSTR(v.DATE,1,6) as month,
               ABS(SUM(CAST(a.AMOUNT AS REAL))) as amt
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        WHERE v.VOUCHERTYPENAME = 'Payment' AND a.ISDEEMEDPOSITIVE = 'Yes'{_df}
        GROUP BY month ORDER BY month
    """, _dp).fetchall()
    data["payments"] = {r[0]: r[1] for r in rows}

    # Invoice counts (Sales vouchers)
    _df2 = ""
    _dp2 = []
    if date_from:
        _df2 += " AND DATE >= ?"
        _dp2.append(date_from)
    if date_to:
        _df2 += " AND DATE <= ?"
        _dp2.append(date_to)
    rows = conn.execute(f"""
        SELECT SUBSTR(DATE,1,6) as m, COUNT(DISTINCT GUID)
        FROM trn_voucher WHERE VOUCHERTYPENAME = 'Sales'{_df2}
        GROUP BY m ORDER BY m
    """, _dp2).fetchall()
    data["invoice_counts"] = {r[0]: r[1] for r in rows}

    # Top 5 customers
    top5 = conn.execute(f"""
        SELECT v.PARTYLEDGERNAME as party,
               ABS(SUM(CAST(a.AMOUNT AS REAL))) as total_sales
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT IN ({_s_ph})
          AND v.PARTYLEDGERNAME IS NOT NULL AND v.PARTYLEDGERNAME != ''{_df}
        GROUP BY party ORDER BY total_sales DESC LIMIT 5
    """, _s_g + _dp).fetchall()
    data["top5_customers"] = top5

    # Stock-in-Hand (Opening & Closing for P&L stock adjustment)
    _stock_groups = get_groups_by_nature(conn, 'stock') if hasattr(get_groups_by_nature, '__call__') else []
    if not _stock_groups:
        try:
            _stock_groups = [r[0] for r in conn.execute("SELECT NAME FROM mst_group WHERE UPPER(NAME) LIKE '%STOCK%HAND%' OR UPPER(RESERVEDNAME) LIKE '%STOCK%HAND%'").fetchall()]
        except Exception:
            _stock_groups = []
    _mis_cols_s = {r[1] for r in conn.execute("PRAGMA table_info(mst_ledger)").fetchall()}
    _mis_bc_s = "COMPUTED_CB" if "COMPUTED_CB" in _mis_cols_s else "CLOSINGBALANCE"
    data["opening_stock"] = 0
    data["closing_stock"] = 0
    if _stock_groups:
        _sk_ph = ",".join(["?"] * len(_stock_groups))
        try:
            row = conn.execute(f"SELECT COALESCE(SUM(ABS(CAST(OPENINGBALANCE AS REAL))), 0), COALESCE(SUM(ABS(CAST({_mis_bc_s} AS REAL))), 0) FROM mst_ledger WHERE PARENT IN ({_sk_ph})", _stock_groups).fetchone()
            data["opening_stock"] = row[0] or 0
            data["closing_stock"] = row[1] or 0
        except Exception:
            pass

    # Bank/Cash balances (flag-based sub-groups)
    _bk_groups = get_groups_by_nature(conn, 'bank') + get_groups_by_nature(conn, 'bank_od') + get_groups_by_nature(conn, 'cash')
    _bk_groups = list(dict.fromkeys(_bk_groups))  # deduplicate
    _bk_ph = ",".join(["?"] * len(_bk_groups)) if _bk_groups else "'__NONE__'"
    _bk_g = _bk_groups
    _mis_cols = {r[1] for r in conn.execute("PRAGMA table_info(mst_ledger)").fetchall()}
    _mis_bc = "COMPUTED_CB" if "COMPUTED_CB" in _mis_cols else "CLOSINGBALANCE"
    rows = conn.execute(f"""
        SELECT NAME, PARENT,
               CAST(OPENINGBALANCE AS REAL) as opening,
               CAST({_mis_bc} AS REAL) as closing
        FROM mst_ledger
        WHERE PARENT IN ({_bk_ph})
        ORDER BY ABS(CAST({_mis_bc} AS REAL)) DESC
    """, _bk_g).fetchall()
    data["bank_balances"] = rows

    # Debtors & Creditors closing (flag-based sub-groups)
    _d_ph, _d_g = _nature_ph(conn, 'debtors')
    _c_ph, _c_g = _nature_ph(conn, 'creditors')
    data["total_debtors"] = conn.execute(
        f"SELECT COALESCE(ABS(SUM(CAST({_mis_bc} AS REAL))), 0) FROM mst_ledger WHERE PARENT IN ({_d_ph})",
        _d_g
    ).fetchone()[0]
    data["total_creditors"] = conn.execute(
        f"SELECT COALESCE(ABS(SUM(CAST({_mis_bc} AS REAL))), 0) FROM mst_ledger WHERE PARENT IN ({_c_ph})",
        _c_g
    ).fetchone()[0]

    conn.close()
    return data


def get_months(data):
    all_months = set()
    for key in ["sales", "purchases"]:
        all_months.update(data[key].keys())
    return sorted(all_months)


def month_label(m):
    """Convert YYYYMM to Apr'25 style label."""
    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    yr = m[:4]
    mn = int(m[4:6])
    return f"{month_names[mn - 1]}'{yr[2:]}"


# ── DRILL-DOWN DATA QUERIES ─────────────────────────────────────────────────

def drill_revenue(month_code):
    """Get all sales invoices for a specific month.
    Uses signed amounts so credit notes appear as negative (returns),
    ensuring the drill-down total matches the monthly aggregate (ABS of net).
    """
    conn = sqlite3.connect(DB_PATH)
    s_ph, s_g = _nature_ph(conn, 'sales')
    rows = conn.execute(f"""
        SELECT v.DATE, v.VOUCHERNUMBER, v.PARTYLEDGERNAME, v.NARRATION,
               SUM(CAST(a.AMOUNT AS REAL)) as amount, v.VOUCHERTYPENAME
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT IN ({s_ph}) AND SUBSTR(v.DATE,1,6) = ?
        GROUP BY v.GUID ORDER BY v.DATE
    """, s_g + [month_code]).fetchall()
    conn.close()
    # Positive = sales, negative = credit notes/returns
    df = pd.DataFrame(rows, columns=["Date", "Voucher No", "Party", "Narration", "Amount", "Type"])
    return df


def drill_purchases(month_code):
    """Get all purchase bills for a specific month.
    Uses signed amounts so debit notes appear as negative (returns),
    ensuring the drill-down total matches the monthly aggregate.
    """
    conn = sqlite3.connect(DB_PATH)
    p_ph, p_g = _nature_ph(conn, 'purchase')
    rows = conn.execute(f"""
        SELECT v.DATE, v.VOUCHERNUMBER, v.PARTYLEDGERNAME, v.NARRATION,
               SUM(CAST(a.AMOUNT AS REAL)) as amount, v.VOUCHERTYPENAME
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT IN ({p_ph}) AND SUBSTR(v.DATE,1,6) = ?
        GROUP BY v.GUID ORDER BY v.DATE
    """, p_g + [month_code]).fetchall()
    conn.close()
    df = pd.DataFrame(rows, columns=["Date", "Voucher No", "Party", "Narration", "Amount", "Type"])
    return df


def drill_expense(ledger_name, month_code):
    """Get all transactions for a specific expense ledger in a month."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT v.DATE, v.VOUCHERNUMBER, v.VOUCHERTYPENAME, v.PARTYLEDGERNAME, v.NARRATION,
               ABS(CAST(a.AMOUNT AS REAL)) as amount
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        WHERE a.LEDGERNAME = ? AND SUBSTR(v.DATE,1,6) = ? AND CAST(a.AMOUNT AS REAL) < 0
        ORDER BY v.DATE
    """, (ledger_name, month_code)).fetchall()
    conn.close()
    df = pd.DataFrame(rows, columns=["Date", "Voucher No", "Type", "Party", "Narration", "Amount"])
    return df


def drill_direct_expense(month_code):
    """Get all direct expense transactions for a month."""
    conn = sqlite3.connect(DB_PATH)
    de_ph, de_g = _nature_ph(conn, 'direct_expense')
    rows = conn.execute(f"""
        SELECT v.DATE, v.VOUCHERNUMBER, v.VOUCHERTYPENAME, a.LEDGERNAME, v.PARTYLEDGERNAME, v.NARRATION,
               ABS(CAST(a.AMOUNT AS REAL)) as amount
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT IN ({de_ph}) AND SUBSTR(v.DATE,1,6) = ? AND CAST(a.AMOUNT AS REAL) < 0
        ORDER BY v.DATE
    """, de_g + [month_code]).fetchall()
    conn.close()
    df = pd.DataFrame(rows, columns=["Date", "Voucher No", "Type", "Ledger", "Party", "Narration", "Amount"])
    return df


def drill_gross_profit(month_code, data):
    """Show sales vs purchases breakdown for a month."""
    sales_amt = data["sales"].get(month_code, {}).get("amount", 0)
    purch_amt = data["purchases"].get(month_code, {}).get("amount", 0)
    de = data.get("direct_expenses", {}).get(month_code, {})
    direct_total = sum(de.values())
    gp = sales_amt - purch_amt - direct_total

    rows_data = [
        ("Revenue (Net Sales)", sales_amt),
        ("Less: COGS (Purchases)", -purch_amt),
    ]
    if direct_total > 0:
        rows_data.append(("Less: Direct Expenses", -direct_total))
    rows_data.append(("GROSS PROFIT", gp))
    if sales_amt > 0:
        rows_data.append(("Gross Margin %", _safe_div(gp, sales_amt) * 100))

    df = pd.DataFrame(rows_data, columns=["Line Item", "Amount"])
    return df


def drill_customer_invoices(party_name):
    """Get all invoices for a specific customer.
    Uses signed amounts so credit notes appear as negative (returns).
    """
    conn = sqlite3.connect(DB_PATH)
    s_ph, s_g = _nature_ph(conn, 'sales')
    rows = conn.execute(f"""
        SELECT v.DATE, v.VOUCHERNUMBER, SUBSTR(v.DATE,1,6) as month,
               v.NARRATION,
               SUM(CAST(a.AMOUNT AS REAL)) as amount, v.VOUCHERTYPENAME
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT IN ({s_ph})
          AND v.PARTYLEDGERNAME = ?
        GROUP BY v.GUID ORDER BY v.DATE
    """, s_g + [party_name]).fetchall()
    conn.close()
    df = pd.DataFrame(rows, columns=["Date", "Voucher No", "Month", "Narration", "Amount", "Type"])
    return df


def drill_total_opex(month_code):
    """Get all indirect expense transactions for a month."""
    conn = sqlite3.connect(DB_PATH)
    ie_ph, ie_g = _nature_ph(conn, 'indirect_expense')
    rows = conn.execute(f"""
        SELECT v.DATE, v.VOUCHERNUMBER, v.VOUCHERTYPENAME, a.LEDGERNAME, v.PARTYLEDGERNAME, v.NARRATION,
               ABS(CAST(a.AMOUNT AS REAL)) as amount
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT IN ({ie_ph}) AND SUBSTR(v.DATE,1,6) = ? AND CAST(a.AMOUNT AS REAL) < 0
        ORDER BY a.LEDGERNAME, v.DATE
    """, ie_g + [month_code]).fetchall()
    conn.close()
    df = pd.DataFrame(rows, columns=["Date", "Voucher No", "Type", "Expense Head", "Party", "Narration", "Amount"])
    return df


# ── BUILD P&L DATA ──────────────────────────────────────────────────────────

def build_pnl(data, months):
    """Build full P&L dictionary with all line items."""
    pnl = {}

    # Revenue
    pnl["Revenue (Net Sales)"] = [data["sales"].get(m, {}).get("amount", 0) for m in months]

    # COGS
    pnl["Less: COGS (Purchases)"] = [data["purchases"].get(m, {}).get("amount", 0) for m in months]

    # Direct Expenses
    direct_totals = []
    for m in months:
        de = data.get("direct_expenses", {}).get(m, {})
        direct_totals.append(sum(de.values()))
    if any(v > 0 for v in direct_totals):
        pnl["Less: Direct Expenses"] = direct_totals

    # Gross Profit
    gp = []
    for i, m in enumerate(months):
        rev = pnl["Revenue (Net Sales)"][i]
        cogs = pnl["Less: COGS (Purchases)"][i]
        de = direct_totals[i] if any(v > 0 for v in direct_totals) else 0
        gp.append(rev - cogs - de)
    pnl["GROSS PROFIT"] = gp

    # Gross Margin %
    pnl["Gross Margin %"] = [
        _safe_div(gp[i], pnl["Revenue (Net Sales)"][i]) * 100
        for i in range(len(months))
    ]

    # Individual indirect expenses (sorted by total descending)
    ledger_totals = {}
    for ledger in data["expense_ledgers"]:
        total = sum(data["indirect_expenses"].get(m, {}).get(ledger, 0) for m in months)
        if total > 0:
            ledger_totals[ledger] = total

    sorted_ledgers = sorted(ledger_totals.keys(), key=lambda x: ledger_totals[x], reverse=True)

    for ledger in sorted_ledgers:
        pnl[f"  {ledger}"] = [
            data["indirect_expenses"].get(m, {}).get(ledger, 0) for m in months
        ]

    # Total Operating Expenses
    opex = []
    for i, m in enumerate(months):
        total = sum(data["indirect_expenses"].get(m, {}).get(l, 0) for l in sorted_ledgers)
        opex.append(total)
    pnl["TOTAL OPERATING EXPENSES"] = opex

    # EBITDA / Operating Profit
    ebitda = [gp[i] - opex[i] for i in range(len(months))]
    pnl["EBITDA (Operating Profit)"] = ebitda

    # Operating Margin %
    pnl["Operating Margin %"] = [
        _safe_div(ebitda[i], pnl["Revenue (Net Sales)"][i]) * 100
        for i in range(len(months))
    ]

    # Stock Adjustment (Change in Inventory)
    opening_stock = data.get("opening_stock", 0) or 0
    closing_stock = data.get("closing_stock", 0) or 0
    stock_change = opening_stock - closing_stock  # Positive = stock consumed (expense)
    if abs(stock_change) > 0:
        # Spread stock change evenly across months for display
        # (actual stock is an annual adjustment, not monthly)
        per_month = stock_change / len(months) if months else 0
        pnl["Less: Stock Adjustment (Op-Cl)"] = [round(per_month, 2) for _ in months]

    # Net Profit = EBITDA - Stock Adjustment
    net_profit = []
    for i in range(len(months)):
        np_val = ebitda[i] - (stock_change / len(months) if months and abs(stock_change) > 0 else 0)
        net_profit.append(np_val)
    pnl["NET PROFIT / (LOSS)"] = net_profit

    # Net Margin %
    pnl["Net Margin %"] = [
        _safe_div(net_profit[i], pnl["Revenue (Net Sales)"][i]) * 100
        for i in range(len(months))
    ]

    return pnl, sorted_ledgers


# ── DRILL-DOWN DETAIL VIEW ───────────────────────────────────────────────────

def render_drill_view(data, months, pnl, sorted_ledgers):
    """Render the drill-down detail view."""
    line = st.session_state.mis_drill_line
    month_code = st.session_state.mis_drill_month
    party = st.session_state.mis_drill_party

    # Back button
    if st.button("← Back to MIS", type="primary", key="back_btn"):
        go_back()
        st.rerun()

    st.markdown("---")

    month_lbl = month_label(month_code) if month_code else "All Months"

    # --- Revenue drill ---
    if line == "revenue":
        st.subheader(f"Revenue Drill-Down — {month_lbl}")
        st.caption("All sales invoices and credit notes for this month")
        df = drill_revenue(month_code)
        if df.empty:
            st.info("No sales transactions found for this month.")
        else:
            net_total = abs(df["Amount"].sum())
            n_returns = (df["Amount"] < 0).sum()
            label_parts = [f"{len(df)} vouchers"]
            if n_returns > 0:
                label_parts.append(f"{n_returns} credit notes")
            st.markdown(f"**Net Revenue: {fmt_indian(net_total, prefix='₹')}** ({fmt_lakhs(net_total)}) — {', '.join(label_parts)}")
            df["Amount (₹)"] = df["Amount"].apply(lambda x: fmt_indian(x, prefix="₹"))
            st.dataframe(
                df[["Date", "Voucher No", "Type", "Party", "Narration", "Amount (₹)"]],
                hide_index=True, use_container_width=True, height=min(len(df) * 38 + 40, 600)
            )

    # --- Purchases drill ---
    elif line == "purchases":
        st.subheader(f"Purchases Drill-Down — {month_lbl}")
        st.caption("All purchase bills and debit notes for this month")
        df = drill_purchases(month_code)
        if df.empty:
            st.info("No purchase transactions found for this month.")
        else:
            net_total = abs(df["Amount"].sum())
            n_returns = (df["Amount"] < 0).sum()
            label_parts = [f"{len(df)} vouchers"]
            if n_returns > 0:
                label_parts.append(f"{n_returns} debit notes")
            st.markdown(f"**Net Purchases: {fmt_indian(net_total, prefix='₹')}** ({fmt_lakhs(net_total)}) — {', '.join(label_parts)}")
            df["Amount (₹)"] = df["Amount"].apply(lambda x: fmt_indian(x, prefix="₹"))
            st.dataframe(
                df[["Date", "Voucher No", "Type", "Party", "Narration", "Amount (₹)"]],
                hide_index=True, use_container_width=True, height=min(len(df) * 38 + 40, 600)
            )

    # --- Gross Profit drill ---
    elif line == "gross_profit":
        st.subheader(f"Gross Profit Breakdown — {month_lbl}")
        st.caption("Sales vs Purchases vs Direct Expenses")
        df = drill_gross_profit(month_code, data)
        # Display as a styled breakdown
        for _, row in df.iterrows():
            label = row["Line Item"]
            val = row["Amount"]
            if "%" in label:
                st.markdown(f"**{label}:** {fmt_pct(val)}")
            else:
                color = "green" if val >= 0 else "red"
                st.markdown(f"**{label}:** :{color}[{fmt_indian(abs(val), prefix='₹')}] ({fmt_lakhs(val)})")
        st.markdown("---")
        st.markdown("**Click below to drill further:**")
        col1, col2 = st.columns(2)
        with col1:
            if st.button(f"View Sales Invoices ({month_lbl})", key="gp_to_rev"):
                go_drill("revenue", month_code)
                st.rerun()
        with col2:
            if st.button(f"View Purchase Bills ({month_lbl})", key="gp_to_purch"):
                go_drill("purchases", month_code)
                st.rerun()

    # --- Direct Expenses drill ---
    elif line == "direct_expenses":
        st.subheader(f"Direct Expenses Drill-Down — {month_lbl}")
        df = drill_direct_expense(month_code)
        if df.empty:
            st.info("No direct expense transactions found for this month.")
        else:
            total = df["Amount"].sum()
            st.markdown(f"**Total: {fmt_indian(total, prefix='₹')}** ({fmt_lakhs(total)}) — {len(df)} entries")
            df["Amount (₹)"] = df["Amount"].apply(lambda x: fmt_indian(x, prefix="₹"))
            st.dataframe(
                df[["Date", "Voucher No", "Type", "Ledger", "Party", "Narration", "Amount (₹)"]],
                hide_index=True, use_container_width=True, height=min(len(df) * 38 + 40, 600)
            )

    # --- Individual expense head drill ---
    elif line and line.startswith("expense:"):
        ledger_name = line.replace("expense:", "")
        st.subheader(f"{ledger_name} — {month_lbl}")
        st.caption(f"All transactions booked under '{ledger_name}'")
        df = drill_expense(ledger_name, month_code)
        if df.empty:
            st.info("No transactions found.")
        else:
            total = df["Amount"].sum()
            st.markdown(f"**Total: {fmt_indian(total, prefix='₹')}** ({fmt_lakhs(total)}) — {len(df)} entries")
            df["Amount (₹)"] = df["Amount"].apply(lambda x: fmt_indian(x, prefix="₹"))
            st.dataframe(
                df[["Date", "Voucher No", "Type", "Party", "Narration", "Amount (₹)"]],
                hide_index=True, use_container_width=True, height=min(len(df) * 38 + 40, 600)
            )

    # --- Total OpEx drill ---
    elif line == "total_opex":
        st.subheader(f"Total Operating Expenses — {month_lbl}")
        st.caption("All indirect expense transactions grouped by expense head")
        df = drill_total_opex(month_code)
        if df.empty:
            st.info("No operating expense transactions found.")
        else:
            # Summary by head
            summary = df.groupby("Expense Head")["Amount"].sum().sort_values(ascending=False).reset_index()
            summary["Amount (₹)"] = summary["Amount"].apply(lambda x: fmt_indian(x, prefix="₹"))
            grand = df["Amount"].sum()
            st.markdown(f"**Grand Total: {fmt_indian(grand, prefix='₹')}** ({fmt_lakhs(grand)})")
            st.markdown("**By Expense Head:**")
            st.dataframe(summary[["Expense Head", "Amount (₹)"]], hide_index=True, use_container_width=True)
            st.markdown("**All Transactions:**")
            df["Amount (₹)"] = df["Amount"].apply(lambda x: fmt_indian(x, prefix="₹"))
            st.dataframe(
                df[["Date", "Voucher No", "Type", "Expense Head", "Party", "Narration", "Amount (₹)"]],
                hide_index=True, use_container_width=True, height=min(len(df) * 38 + 40, 600)
            )

    # --- EBITDA / Net Profit drill ---
    elif line in ("ebitda", "net_profit"):
        label = "EBITDA (Operating Profit)" if line == "ebitda" else "Net Profit / (Loss)"
        st.subheader(f"{label} Breakdown — {month_lbl}")
        idx = months.index(month_code) if month_code in months else 0
        rev = pnl["Revenue (Net Sales)"][idx]
        cogs = pnl["Less: COGS (Purchases)"][idx]
        de = pnl.get("Less: Direct Expenses", [0] * len(months))[idx]
        gp_val = pnl["GROSS PROFIT"][idx]
        opex_val = pnl["TOTAL OPERATING EXPENSES"][idx]
        ebitda_val = pnl["EBITDA (Operating Profit)"][idx]

        breakdown = [
            ("Revenue (Net Sales)", rev),
            ("Less: COGS (Purchases)", -cogs),
        ]
        if de > 0:
            breakdown.append(("Less: Direct Expenses", -de))
        breakdown.append(("= GROSS PROFIT", gp_val))
        breakdown.append(("Less: Operating Expenses", -opex_val))
        breakdown.append(("= EBITDA / Net Profit", ebitda_val))

        for label_text, val in breakdown:
            if label_text.startswith("="):
                st.markdown(f"### {label_text}: {fmt_indian(val, prefix='₹')} ({fmt_lakhs(val)})")
            else:
                color = "green" if val >= 0 else "red"
                st.markdown(f"**{label_text}:** :{color}[{fmt_indian(abs(val), prefix='₹')}] ({fmt_lakhs(val)})")

    # --- Ratio drill ---
    elif line and line.startswith("ratio:"):
        ratio_name = line.replace("ratio:", "")
        st.subheader(f"Ratio Calculation — {ratio_name}")
        if month_code:
            st.caption(f"Month: {month_lbl}")
            idx = months.index(month_code) if month_code in months else 0
            rev = pnl["Revenue (Net Sales)"][idx]
            gp_val = pnl["GROSS PROFIT"][idx]
            ebitda_val = pnl["EBITDA (Operating Profit)"][idx]
            np_val = pnl["NET PROFIT / (LOSS)"][idx]
            inv_count = data["invoice_counts"].get(month_code, 1)
            rcpt = data["receipts"].get(month_code, 0)

            if ratio_name == "Gross Margin":
                st.markdown(f"**Formula:** Gross Profit / Revenue x 100")
                st.markdown(f"**Gross Profit:** {fmt_indian(gp_val, prefix='₹')}")
                st.markdown(f"**Revenue:** {fmt_indian(rev, prefix='₹')}")
                result = (gp_val / rev * 100) if rev > 0 else 0
                st.markdown(f"### Result: {fmt_pct(result)}")
            elif ratio_name == "Op. Margin":
                st.markdown(f"**Formula:** Operating Profit / Revenue x 100")
                st.markdown(f"**Operating Profit (EBITDA):** {fmt_indian(ebitda_val, prefix='₹')}")
                st.markdown(f"**Revenue:** {fmt_indian(rev, prefix='₹')}")
                result = (ebitda_val / rev * 100) if rev > 0 else 0
                st.markdown(f"### Result: {fmt_pct(result)}")
            elif ratio_name == "Net Margin":
                st.markdown(f"**Formula:** Net Profit / Revenue x 100")
                st.markdown(f"**Net Profit:** {fmt_indian(np_val, prefix='₹')}")
                st.markdown(f"**Revenue:** {fmt_indian(rev, prefix='₹')}")
                result = (np_val / rev * 100) if rev > 0 else 0
                st.markdown(f"### Result: {fmt_pct(result)}")
            elif ratio_name == "MoM Growth":
                if idx > 0:
                    prev_rev = pnl["Revenue (Net Sales)"][idx - 1]
                    st.markdown(f"**Formula:** (Current Revenue - Previous Revenue) / Previous Revenue x 100")
                    st.markdown(f"**Current Month Revenue ({month_lbl}):** {fmt_indian(rev, prefix='₹')}")
                    prev_lbl = month_label(months[idx - 1])
                    st.markdown(f"**Previous Month Revenue ({prev_lbl}):** {fmt_indian(prev_rev, prefix='₹')}")
                    result = (_safe_div(rev, prev_rev) - 1) * 100 if prev_rev > 0 else 0
                    st.markdown(f"### Result: {fmt_pct(result)}")
                else:
                    st.info("No previous month available for comparison.")
            elif ratio_name == "Rev/Invoice":
                st.markdown(f"**Formula:** Revenue / Number of Sales Invoices")
                st.markdown(f"**Revenue:** {fmt_indian(rev, prefix='₹')}")
                st.markdown(f"**Invoice Count:** {inv_count}")
                result = rev / inv_count if inv_count > 0 else 0
                st.markdown(f"### Result: {fmt_indian(result, prefix='₹')} ({fmt_lakhs(result, 2)})")
            elif ratio_name == "Collection %":
                st.markdown(f"**Formula:** Receipts / Revenue x 100")
                st.markdown(f"**Receipts (Collections):** {fmt_indian(rcpt, prefix='₹')}")
                st.markdown(f"**Revenue:** {fmt_indian(rev, prefix='₹')}")
                result = (rcpt / rev * 100) if rev > 0 else 0
                st.markdown(f"### Result: {fmt_pct(result)}")
            else:
                st.info(f"Details for ratio '{ratio_name}' not available.")

    # --- Customer drill ---
    elif line == "customer" and party:
        st.subheader(f"Customer Invoice History — {party}")
        df = drill_customer_invoices(party)
        if df.empty:
            st.info("No invoices found for this customer.")
        else:
            total = df["Amount"].sum()
            st.markdown(f"**Total Revenue: {fmt_indian(total, prefix='₹')}** ({fmt_lakhs(total)}) — {len(df)} invoices")
            # Monthly summary
            monthly_summary = df.groupby("Month")["Amount"].sum().reset_index()
            monthly_summary["Month Label"] = monthly_summary["Month"].apply(month_label)
            monthly_summary["Amount (₹)"] = monthly_summary["Amount"].apply(lambda x: fmt_indian(x, prefix="₹"))
            st.markdown("**Monthly Breakdown:**")
            st.dataframe(
                monthly_summary[["Month Label", "Amount (₹)"]].rename(columns={"Month Label": "Month"}),
                hide_index=True, use_container_width=True
            )
            st.markdown("**All Invoices:**")
            df["Amount (₹)"] = df["Amount"].apply(lambda x: fmt_indian(x, prefix="₹"))
            st.dataframe(
                df[["Date", "Voucher No", "Narration", "Amount (₹)"]],
                hide_index=True, use_container_width=True, height=min(len(df) * 38 + 40, 600)
            )

    else:
        st.warning(f"Unknown drill-down: line={line}, month={month_code}")
        if st.button("Return to MIS"):
            go_back()
            st.rerun()


# ── MAIN DASHBOARD ──────────────────────────────────────────────────────────

def main():
    _vch_tuple = tuple(_vch_types_filter) if _vch_types_filter else None
    data = load_all_data(date_from=DATE_FROM, date_to=DATE_TO,
                         voucher_types_tuple=_vch_tuple)
    months = get_months(data)
    pnl, sorted_ledgers = build_pnl(data, months)
    month_labels = [month_label(m) for m in months]

    # ── ROUTE: DRILL VIEW ─────────────────────────────────────────────────────
    if st.session_state.mis_view == "drill_detail":
        render_drill_view(data, months, pnl, sorted_ledgers)
        return

    try:
        _mis_conn = sqlite3.connect(DB_PATH)
        _mis_co = _mis_conn.execute("SELECT value FROM _metadata WHERE key='company_name'").fetchone()
        _mis_company = _mis_co[0] if _mis_co else "Company"
        _mis_conn.close()
    except Exception:
        _mis_company = "Company"
    page_header(f"{_mis_company} -- Monthly MIS", "Management Information System | Click any figure to drill down")

    # ── EXECUTIVE SUMMARY ────────────────────────────────────────────────────
    ytd_revenue = sum(pnl["Revenue (Net Sales)"])
    ytd_profit = sum(pnl["NET PROFIT / (LOSS)"])
    ytd_gp = sum(pnl["GROSS PROFIT"])
    avg_monthly_rev = _safe_div(ytd_revenue, len(months))
    avg_gp_margin = _safe_div(ytd_gp, ytd_revenue) * 100

    rev_list = pnl["Revenue (Net Sales)"]
    best_month_idx = rev_list.index(max(rev_list))
    worst_month_idx = rev_list.index(min(rev_list))
    np_list = pnl["NET PROFIT / (LOSS)"]
    best_profit_idx = np_list.index(max(np_list))
    worst_profit_idx = np_list.index(min(np_list))

    # Cash position
    cash_balance = 0
    for b in data["bank_balances"]:
        closing = b[3] if b[3] else 0
        if b[1] in ("Bank Accounts", "Cash-in-Hand"):
            cash_balance += abs(closing)
        elif b[1] == "Bank OD A/c":
            cash_balance -= abs(closing)

    avg_monthly_burn = _safe_div(abs(sum(pnl["TOTAL OPERATING EXPENSES"])), len(months))
    avg_monthly_net_outflow = _safe_div(abs(ytd_profit), len(months)) if ytd_profit < 0 else 0
    cash_runway = _safe_div(cash_balance, avg_monthly_net_outflow, default=float('inf'))

    section_header("EXECUTIVE SUMMARY")

    profit_color = "green" if ytd_profit >= 0 else "red"
    cols = st.columns(5)
    with cols[0]:
        metric_card("YTD Revenue", fmt_lakhs(ytd_revenue), sub=f"{len(months)} months", color_class="blue")
    with cols[1]:
        metric_card("YTD Net Profit/Loss", fmt_lakhs(ytd_profit),
                     sub=f"Net Margin: {fmt_pct(_safe_div(ytd_profit, ytd_revenue) * 100)}",
                     color_class=profit_color)
    with cols[2]:
        metric_card("Avg Monthly Revenue", fmt_lakhs(avg_monthly_rev),
                     sub=f"GP Margin: {fmt_pct(avg_gp_margin)}")
    with cols[3]:
        metric_card("Best / Worst Month (Rev)",
                     f"{month_labels[best_month_idx]} / {month_labels[worst_month_idx]}",
                     sub=f"{fmt_lakhs(rev_list[best_month_idx])} / {fmt_lakhs(rev_list[worst_month_idx])}")
    with cols[4]:
        runway_text = f"{cash_runway:.1f} mo" if cash_runway < 100 else "Profitable"
        metric_card("Cash Runway", runway_text, sub=f"Cash Bal: {fmt_lakhs(cash_balance)}")

    # ── MONTHLY P&L STATEMENT ────────────────────────────────────────────────
    section_header("MONTHLY PROFIT & LOSS STATEMENT")
    st.caption("Click any amount to drill into underlying transactions")

    # Build the P&L table as HTML for full control
    pnl_rows_order = ["Revenue (Net Sales)", "Less: COGS (Purchases)"]
    if "Less: Direct Expenses" in pnl:
        pnl_rows_order.append("Less: Direct Expenses")
    pnl_rows_order.append("GROSS PROFIT")
    pnl_rows_order.append("Gross Margin %")
    pnl_rows_order.append("---opex_header---")
    for ledger in sorted_ledgers:
        pnl_rows_order.append(f"  {ledger}")
    pnl_rows_order.append("TOTAL OPERATING EXPENSES")
    pnl_rows_order.append("---divider---")
    pnl_rows_order.append("EBITDA (Operating Profit)")
    pnl_rows_order.append("Operating Margin %")
    pnl_rows_order.append("---divider2---")
    pnl_rows_order.append("NET PROFIT / (LOSS)")
    pnl_rows_order.append("Net Margin %")

    # Define row styling
    bold_rows = {"Revenue (Net Sales)", "GROSS PROFIT", "TOTAL OPERATING EXPENSES",
                 "EBITDA (Operating Profit)", "NET PROFIT / (LOSS)"}
    pct_rows = {"Gross Margin %", "Operating Margin %", "Net Margin %"}
    header_rows = {"GROSS PROFIT", "TOTAL OPERATING EXPENSES", "EBITDA (Operating Profit)", "NET PROFIT / (LOSS)"}

    # Map P&L row keys to drill line identifiers
    def get_drill_line(row_key):
        if row_key == "Revenue (Net Sales)":
            return "revenue"
        elif row_key == "Less: COGS (Purchases)":
            return "purchases"
        elif row_key == "Less: Direct Expenses":
            return "direct_expenses"
        elif row_key == "GROSS PROFIT":
            return "gross_profit"
        elif row_key == "TOTAL OPERATING EXPENSES":
            return "total_opex"
        elif row_key == "EBITDA (Operating Profit)":
            return "ebitda"
        elif row_key == "NET PROFIT / (LOSS)":
            return "net_profit"
        elif row_key.startswith("  "):
            return f"expense:{row_key.strip()}"
        return None

    # Build HTML table
    html = '<div style="overflow-x:auto;"><table class="slv-table" style="font-family:\'JetBrains Mono\',monospace;">'

    # Header row
    html += '<tr>'
    html += '<th style="text-align:left;min-width:200px;">Particulars</th>'
    for ml in month_labels:
        html += f'<th style="min-width:90px;">{ml}</th>'
    html += f'<th style="min-width:100px;">YTD Total</th>'
    html += f'<th style="text-align:center;min-width:80px;">Trend</th>'
    html += '</tr>'

    for row_key in pnl_rows_order:
        if row_key.startswith("---"):
            if "opex" in row_key:
                html += f'<tr><td colspan="{len(months)+3}" style="background:#f1f5f9;padding:6px 12px;font-weight:700;font-size:0.78rem;color:#475569;border:1px solid #e2e8f0;">OPERATING EXPENSES</td></tr>'
            continue

        if row_key not in pnl:
            continue

        values = pnl[row_key]
        is_bold = row_key in bold_rows
        is_pct = row_key in pct_rows
        is_header = row_key in header_rows
        is_expense_line = row_key.startswith("  ")

        # Row styling
        if is_header:
            bg = "#f0f9ff" if row_key == "GROSS PROFIT" else "#fef3c7" if "EBITDA" in row_key else "#f0fdf4" if "NET PROFIT" in row_key else "#fff7ed"
            border_top = "2px solid #94a3b8"
        elif is_pct:
            bg = "#f8fafc"
            border_top = "none"
        else:
            bg = "#ffffff"
            border_top = "none"

        fw = "700" if is_bold else "400"
        fs = "0.82rem" if is_bold else "0.78rem" if is_expense_line else "0.8rem"
        color = "#1e293b" if is_bold else "#64748b" if is_expense_line else "#334155"

        html += f'<tr style="background:{bg};">'

        # Label cell
        label = row_key
        html += f'<td style="padding:6px 12px;font-weight:{fw};font-size:{fs};color:{color};border:1px solid #e2e8f0;border-top:{border_top};white-space:nowrap;">{label}</td>'

        # Value cells
        for i, v in enumerate(values):
            if is_pct:
                cell = fmt_pct(v)
                cell_color = "#10b981" if v > 0 else "#ef4444" if v < 0 else "#6b7280"
            else:
                cell = fmt_indian(v, prefix="")
                cell_color = "#ef4444" if v < 0 else color

            html += f'<td style="text-align:right;padding:6px 8px;font-weight:{fw};font-size:{fs};color:{cell_color};border:1px solid #e2e8f0;border-top:{border_top};">{cell}</td>'

        # YTD column
        ytd_val = sum(values)
        if is_pct:
            ytd_val = _safe_div(sum(values), len(values))
            ytd_cell = fmt_pct(ytd_val)
            ytd_color = "#10b981" if ytd_val > 0 else "#ef4444"
        else:
            ytd_cell = fmt_indian(ytd_val, prefix="")
            ytd_color = "#ef4444" if ytd_val < 0 else "#0f172a"

        html += f'<td style="text-align:right;padding:6px 8px;font-weight:700;font-size:{fs};color:{ytd_color};border:1px solid #e2e8f0;border-top:{border_top};background:#f8fafc;">{ytd_cell}</td>'

        # Trend sparkline
        sparkline = sparkline_bar(values)
        html += f'<td style="text-align:center;padding:6px 4px;border:1px solid #e2e8f0;border-top:{border_top};background:#f8fafc;">{sparkline}</td>'

        html += '</tr>'

    html += '</table></div>'
    st.markdown(html, unsafe_allow_html=True)

    # ── DRILL BUTTONS FOR P&L (below the table) ──────────────────────────────
    st.markdown("")
    with st.expander("**Drill into P&L Line Items** -- Select a line and month to see transactions", expanded=False):
        drill_col1, drill_col2 = st.columns([1, 1])

        with drill_col1:
            # Build list of drillable lines
            drillable_lines = [
                ("Revenue (Net Sales)", "revenue"),
                ("Purchases (COGS)", "purchases"),
            ]
            if "Less: Direct Expenses" in pnl:
                drillable_lines.append(("Direct Expenses", "direct_expenses"))
            drillable_lines.append(("Gross Profit (breakdown)", "gross_profit"))
            for ledger in sorted_ledgers:
                drillable_lines.append((f"  {ledger}", f"expense:{ledger}"))
            drillable_lines.append(("Total Operating Expenses", "total_opex"))
            drillable_lines.append(("EBITDA / Operating Profit", "ebitda"))
            drillable_lines.append(("Net Profit / (Loss)", "net_profit"))

            line_labels = [dl[0] for dl in drillable_lines]
            line_keys = [dl[1] for dl in drillable_lines]

            selected_line_idx = st.selectbox("Select P&L Line:", range(len(line_labels)),
                                             format_func=lambda i: line_labels[i], key="pnl_drill_line")

        with drill_col2:
            month_options = list(range(len(months)))
            selected_month_idx = st.selectbox("Select Month:", month_options,
                                              format_func=lambda i: month_labels[i], key="pnl_drill_month")

        if st.button("View Transactions", key="pnl_drill_go", type="primary"):
            go_drill(line_keys[selected_line_idx], months[selected_month_idx])
            st.rerun()

        # Quick-access: one row of buttons per month for Revenue and Purchases
        st.markdown("**Quick drill — Revenue by month:**")
        rev_cols = st.columns(len(months))
        for i, m in enumerate(months):
            with rev_cols[i]:
                rev_val = pnl["Revenue (Net Sales)"][i]
                if st.button(f"{month_labels[i]}\n{fmt_lakhs(rev_val)}", key=f"qrev_{m}"):
                    go_drill("revenue", m)
                    st.rerun()

        st.markdown("**Quick drill — Purchases by month:**")
        pur_cols = st.columns(len(months))
        for i, m in enumerate(months):
            with pur_cols[i]:
                pur_val = pnl["Less: COGS (Purchases)"][i]
                if st.button(f"{month_labels[i]}\n{fmt_lakhs(pur_val)}", key=f"qpur_{m}"):
                    go_drill("purchases", m)
                    st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)

    # ── KEY RATIOS & METRICS ─────────────────────────────────────────────────
    section_header("KEY STARTUP / DISTRIBUTION RATIOS")

    col1, col2 = st.columns(2)

    with col1:
        # Build ratios table
        ratio_data = []
        for i, m in enumerate(months):
            rev = pnl["Revenue (Net Sales)"][i]
            gp_val = pnl["GROSS PROFIT"][i]
            ebitda_val = pnl["EBITDA (Operating Profit)"][i]
            np_val = pnl["NET PROFIT / (LOSS)"][i]
            inv_count = data["invoice_counts"].get(m, 1)
            receipts_val = data["receipts"].get(m, 0)

            gp_m = _safe_div(gp_val, rev) * 100
            op_m = _safe_div(ebitda_val, rev) * 100
            np_m = _safe_div(np_val, rev) * 100
            mom_growth = (_safe_div(rev, pnl["Revenue (Net Sales)"][i - 1]) - 1) * 100 if i > 0 and pnl["Revenue (Net Sales)"][i - 1] > 0 else 0
            rev_per_inv = _safe_div(rev, inv_count)
            collection_eff = _safe_div(receipts_val, rev) * 100

            ratio_data.append({
                "Month": month_label(m),
                "Gross Margin": fmt_pct(gp_m),
                "Op. Margin": fmt_pct(op_m),
                "Net Margin": fmt_pct(np_m),
                "MoM Growth": fmt_pct(mom_growth) if i > 0 else "—",
                "Rev/Invoice": fmt_lakhs(rev_per_inv, 2),
                "Collection %": fmt_pct(collection_eff),
            })

        df_ratios = pd.DataFrame(ratio_data)
        st.markdown("**Monthly Ratios** (click a ratio below to see calculation)")
        st.dataframe(df_ratios, hide_index=True, use_container_width=True)

        # Drill buttons for ratios
        with st.expander("**Drill into Ratio Calculation**", expanded=False):
            ratio_names = ["Gross Margin", "Op. Margin", "Net Margin", "MoM Growth", "Rev/Invoice", "Collection %"]
            r_col1, r_col2 = st.columns(2)
            with r_col1:
                sel_ratio = st.selectbox("Select Ratio:", ratio_names, key="ratio_drill_sel")
            with r_col2:
                sel_ratio_month = st.selectbox("Select Month:", range(len(months)),
                                               format_func=lambda i: month_labels[i], key="ratio_drill_month")
            if st.button("Show Calculation", key="ratio_drill_go", type="primary"):
                go_drill(f"ratio:{sel_ratio}", months[sel_ratio_month])
                st.rerun()

    with col2:
        # Concentration & Working Capital
        st.markdown("**Customer Concentration (Top 5)** -- click a customer to see invoices")
        total_sales_val = sum(pnl["Revenue (Net Sales)"])
        _top5 = data.get("top5_customers") or []

        if _top5:
            top5_total = sum(c[1] for c in _top5)
            conc_pct = _safe_div(top5_total, total_sales_val) * 100

            conc_html = '<table class="slv-table">'
            conc_html += '<tr><th style="text-align:left;">Customer</th><th>Revenue</th><th>% Share</th></tr>'
            for cust, amt in _top5:
                share = _safe_div(amt, total_sales_val) * 100
                short_name = cust[:30] + "..." if len(cust) > 30 else cust
                conc_html += f'<tr><td style="padding:5px 10px;border-bottom:1px solid #e2e8f0;font-size:0.78rem;">{short_name}</td>'
                conc_html += f'<td style="padding:5px 10px;text-align:right;border-bottom:1px solid #e2e8f0;">{fmt_lakhs(amt)}</td>'
                conc_html += f'<td style="padding:5px 10px;text-align:right;border-bottom:1px solid #e2e8f0;">{fmt_pct(share)}</td></tr>'
            conc_html += f'<tr style="background:#f8fafc;font-weight:700;"><td style="padding:6px 10px;">Top 5 Total</td><td style="padding:6px 10px;text-align:right;">{fmt_lakhs(top5_total)}</td><td style="padding:6px 10px;text-align:right;">{fmt_pct(conc_pct)}</td></tr>'
            conc_html += '</table>'
            st.markdown(conc_html, unsafe_allow_html=True)

            # Customer drill buttons
            _num_cust_cols = max(min(len(_top5), 5), 1)
            cust_cols = st.columns(_num_cust_cols)
            for idx_c, (cust, amt) in enumerate(_top5[:_num_cust_cols]):
                short = cust[:18] + ".." if len(cust) > 18 else cust
                with cust_cols[idx_c]:
                    if st.button(f"{short}", key=f"cust_drill_{idx_c}"):
                        go_drill("customer", party=cust)
                        st.rerun()
        else:
            st.info("No customer data available for this period.")

        st.markdown("<br>", unsafe_allow_html=True)

        # Working Capital Cycle
        st.markdown("**Working Capital Cycle**")
        annualized_sales = _safe_div(total_sales_val, len(months)) * 12
        annualized_purchases = _safe_div(sum(pnl["Less: COGS (Purchases)"]), len(months)) * 12

        debtor_days = _safe_div(data.get("total_debtors", 0), annualized_sales) * 365
        creditor_days = _safe_div(data.get("total_creditors", 0), annualized_purchases) * 365
        wc_cycle = debtor_days - creditor_days

        wc_html = f"""
        <table style="width:100%;font-size:0.85rem;border-collapse:collapse;">
        <tr><td style="padding:5px 10px;">Debtor Days</td><td style="padding:5px 10px;text-align:right;font-weight:600;">{debtor_days:.0f} days</td></tr>
        <tr><td style="padding:5px 10px;">Creditor Days</td><td style="padding:5px 10px;text-align:right;font-weight:600;">{creditor_days:.0f} days</td></tr>
        <tr style="background:#f0f9ff;font-weight:700;border-top:2px solid #3b82f6;">
            <td style="padding:6px 10px;">Net Working Capital Cycle</td>
            <td style="padding:6px 10px;text-align:right;color:{'#10b981' if wc_cycle < 0 else '#ef4444'}">{wc_cycle:.0f} days</td>
        </tr>
        <tr><td style="padding:5px 10px;font-size:0.75rem;color:#94a3b8;" colspan="2">Negative = Creditor-funded operations (good for distribution)</td></tr>
        </table>
        """
        st.markdown(wc_html, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── CONTRIBUTION MARGIN ANALYSIS ─────────────────────────────────────────
    section_header("CONTRIBUTION MARGIN ANALYSIS")

    cm_html = '<table class="slv-table" style="font-family:\'JetBrains Mono\',monospace;">'
    cm_html += '<tr>'
    cm_html += '<th style="text-align:left;">Metric</th>'
    for ml in month_labels:
        cm_html += f'<th style="min-width:85px;">{ml}</th>'
    cm_html += '<th>YTD</th></tr>'

    # Revenue
    cm_html += '<tr style="background:#ffffff;"><td style="padding:5px 12px;font-weight:600;">Revenue</td>'
    for v in pnl["Revenue (Net Sales)"]:
        cm_html += f'<td style="padding:5px 6px;text-align:right;">{fmt_lakhs(v)}</td>'
    cm_html += f'<td style="padding:5px 8px;text-align:right;font-weight:700;background:#f8fafc;">{fmt_lakhs(sum(pnl["Revenue (Net Sales)"]))}</td></tr>'

    # Variable Costs (COGS)
    cm_html += '<tr style="background:#ffffff;"><td style="padding:5px 12px;">Less: Variable Costs (COGS)</td>'
    for v in pnl["Less: COGS (Purchases)"]:
        cm_html += f'<td style="padding:5px 6px;text-align:right;color:#ef4444;">({fmt_lakhs(v)})</td>'
    cm_html += f'<td style="padding:5px 8px;text-align:right;font-weight:700;color:#ef4444;background:#f8fafc;">({fmt_lakhs(sum(pnl["Less: COGS (Purchases)"]))})</td></tr>'

    # Contribution Margin
    cm_values = [pnl["Revenue (Net Sales)"][i] - pnl["Less: COGS (Purchases)"][i] for i in range(len(months))]
    cm_html += '<tr style="background:#f0f9ff;border-top:2px solid #3b82f6;"><td style="padding:6px 12px;font-weight:700;">Contribution Margin</td>'
    for v in cm_values:
        color = "#10b981" if v >= 0 else "#ef4444"
        cm_html += f'<td style="padding:6px 6px;text-align:right;font-weight:700;color:{color};">{fmt_lakhs(v)}</td>'
    ytd_cm = sum(cm_values)
    cm_html += f'<td style="padding:6px 8px;text-align:right;font-weight:700;background:#f8fafc;color:{"#10b981" if ytd_cm >= 0 else "#ef4444"}">{fmt_lakhs(ytd_cm)}</td></tr>'

    # CM %
    cm_pcts = [_safe_div(cm_values[i], pnl["Revenue (Net Sales)"][i]) * 100 for i in range(len(months))]
    cm_html += '<tr style="background:#f8fafc;"><td style="padding:5px 12px;color:#64748b;">Contribution Margin %</td>'
    for v in cm_pcts:
        color = "#10b981" if v >= 0 else "#ef4444"
        cm_html += f'<td style="padding:5px 6px;text-align:right;color:{color};">{fmt_pct(v)}</td>'
    avg_cm_pct = sum(cm_pcts) / len(cm_pcts) if cm_pcts else 0
    cm_html += f'<td style="padding:5px 8px;text-align:right;background:#f8fafc;color:{"#10b981" if avg_cm_pct >= 0 else "#ef4444"}">{fmt_pct(avg_cm_pct)}</td></tr>'

    # Fixed Costs
    fc_values = pnl["TOTAL OPERATING EXPENSES"]
    cm_html += '<tr style="background:#ffffff;"><td style="padding:5px 12px;">Less: Fixed Costs (OpEx)</td>'
    for v in fc_values:
        cm_html += f'<td style="padding:5px 6px;text-align:right;color:#ef4444;">({fmt_lakhs(v)})</td>'
    cm_html += f'<td style="padding:5px 8px;text-align:right;font-weight:700;color:#ef4444;background:#f8fafc;">({fmt_lakhs(sum(fc_values))})</td></tr>'

    # Fixed Cost Coverage Ratio
    coverage = [_safe_div(cm_values[i], fc_values[i]) for i in range(len(months))]
    cm_html += '<tr style="background:#fef3c7;border-top:2px solid #f59e0b;"><td style="padding:6px 12px;font-weight:700;">Fixed Cost Coverage Ratio</td>'
    for v in coverage:
        color = "#10b981" if v >= 1 else "#ef4444"
        display = f"{v:.1f}x" if v != 0 else "—"
        cm_html += f'<td style="padding:6px 6px;text-align:right;font-weight:600;color:{color};">{display}</td>'
    avg_cov = _safe_div(sum(cm_values), sum(fc_values))
    cm_html += f'<td style="padding:6px 8px;text-align:right;font-weight:700;background:#f8fafc;color:{"#10b981" if avg_cov >= 1 else "#ef4444"}">{avg_cov:.1f}x</td></tr>'

    cm_html += '</table>'
    st.markdown(cm_html, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── TREND ANALYSIS ───────────────────────────────────────────────────────
    section_header("MoM TREND ANALYSIS")

    trend_metrics = {
        "Revenue": pnl["Revenue (Net Sales)"],
        "Gross Profit": pnl["GROSS PROFIT"],
        "Operating Profit": pnl["EBITDA (Operating Profit)"],
        "Receipts (Collections)": [data["receipts"].get(m, 0) for m in months],
    }

    trend_html = '<table class="slv-table" style="font-family:\'JetBrains Mono\',monospace;">'
    trend_html += '<tr>'
    trend_html += '<th style="text-align:left;">Metric</th>'
    for ml in month_labels:
        trend_html += f'<th>{ml}</th>'
    trend_html += '</tr>'

    for metric_name, values in trend_metrics.items():
        # Value row
        trend_html += f'<tr style="background:#ffffff;"><td style="padding:5px 12px;font-weight:600;">{metric_name}</td>'
        for v in values:
            color = "#1e293b" if v >= 0 else "#ef4444"
            trend_html += f'<td style="padding:5px 6px;text-align:right;color:{color};">{fmt_lakhs(v)}</td>'
        trend_html += '</tr>'

        # MoM change row
        trend_html += f'<tr style="background:#f8fafc;"><td style="padding:3px 12px;color:#94a3b8;font-size:0.72rem;">MoM Change</td>'
        for i, v in enumerate(values):
            if i == 0:
                trend_html += '<td style="padding:3px 6px;text-align:right;font-size:0.72rem;color:#94a3b8;">—</td>'
            else:
                arrow = trend_arrow(v, values[i - 1])
                trend_html += f'<td style="padding:3px 6px;text-align:right;font-size:0.72rem;">{arrow}</td>'
        trend_html += '</tr>'

        # Cumulative YTD row
        cumulative = []
        running = 0
        for v in values:
            running += v
            cumulative.append(running)
        trend_html += f'<tr style="background:#f1f5f9;border-bottom:2px solid #e2e8f0;"><td style="padding:3px 12px;color:#64748b;font-size:0.72rem;">Cumulative YTD</td>'
        for v in cumulative:
            color = "#1e293b" if v >= 0 else "#ef4444"
            trend_html += f'<td style="padding:3px 6px;text-align:right;font-size:0.72rem;color:{color};">{fmt_lakhs(v)}</td>'
        trend_html += '</tr>'

    trend_html += '</table>'
    st.markdown(trend_html, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── BURN RATE & RUNWAY ───────────────────────────────────────────────────
    section_header("BURN RATE & CASH POSITION")

    col_a, col_b, col_c = st.columns(3)

    with col_a:
        burn_months = [(month_labels[i], pnl["NET PROFIT / (LOSS)"][i])
                       for i in range(len(months)) if pnl["NET PROFIT / (LOSS)"][i] < 0]
        avg_burn = abs(_safe_div(sum(v for _, v in burn_months), len(burn_months))) if burn_months else 0

        st.markdown("**Monthly Burn Rate**")
        burn_html = f"""
        <div style="background:#fef2f2;border:1px solid #fecaca;border-radius:8px;padding:1rem;text-align:center;">
            <div style="font-size:0.75rem;color:#991b1b;text-transform:uppercase;">Avg Monthly Burn</div>
            <div style="font-size:1.5rem;font-weight:700;color:#dc2626;">{fmt_lakhs(avg_burn)}</div>
            <div style="font-size:0.75rem;color:#b91c1c;">{len(burn_months)} of {len(months)} months in loss</div>
        </div>
        """
        st.markdown(burn_html, unsafe_allow_html=True)

    with col_b:
        st.markdown("**Cash Balance**")
        cash_html = '<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:1rem;">'
        for b in data["bank_balances"]:
            name = b[0][:35]
            closing = b[3] if b[3] else 0
            color = "#16a34a" if closing < 0 else "#dc2626"
            cash_html += f'<div style="display:flex;justify-content:space-between;padding:3px 0;font-size:0.78rem;">'
            cash_html += f'<span style="color:#374151;">{name}</span>'
            cash_html += f'<span style="color:{color};font-weight:600;">{fmt_lakhs(abs(closing))}</span></div>'
        cash_html += f'<div style="border-top:2px solid #16a34a;margin-top:6px;padding-top:6px;display:flex;justify-content:space-between;">'
        cash_html += f'<span style="font-weight:700;">Net Cash Position</span>'
        cash_html += f'<span style="font-weight:700;color:#0f172a;">{fmt_lakhs(cash_balance)}</span></div></div>'
        st.markdown(cash_html, unsafe_allow_html=True)

    with col_c:
        st.markdown("**Collection Efficiency**")
        coll_eff_data = []
        for i, m in enumerate(months):
            rev = pnl["Revenue (Net Sales)"][i]
            rcpt = data["receipts"].get(m, 0)
            eff = _safe_div(rcpt, rev) * 100
            coll_eff_data.append(eff)

        avg_coll = _safe_div(sum(coll_eff_data), len(coll_eff_data)) if coll_eff_data else 0

        coll_html = f"""
        <div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:1rem;text-align:center;">
            <div style="font-size:0.75rem;color:#1e40af;text-transform:uppercase;">Avg Collection Efficiency</div>
            <div style="font-size:1.5rem;font-weight:700;color:#2563eb;">{fmt_pct(avg_coll)}</div>
            <div style="font-size:0.75rem;color:#3b82f6;">Receipts as % of Sales</div>
        </div>
        """
        st.markdown(coll_html, unsafe_allow_html=True)

        coll_bar_html = '<div style="margin-top:8px;">'
        for i, m in enumerate(months):
            eff = coll_eff_data[i]
            bar_w = min(eff, 100)
            color = "#10b981" if eff >= 80 else "#f59e0b" if eff >= 50 else "#ef4444"
            coll_bar_html += f'<div style="display:flex;align-items:center;margin:2px 0;font-size:0.7rem;">'
            coll_bar_html += f'<span style="width:45px;color:#64748b;">{month_labels[i]}</span>'
            coll_bar_html += f'<div style="flex:1;background:#e2e8f0;height:10px;border-radius:5px;margin:0 6px;">'
            coll_bar_html += f'<div style="width:{bar_w}%;background:{color};height:10px;border-radius:5px;"></div></div>'
            coll_bar_html += f'<span style="width:35px;color:{color};font-weight:600;text-align:right;">{eff:.0f}%</span></div>'
        coll_bar_html += '</div>'
        st.markdown(coll_bar_html, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── FOOTER ───────────────────────────────────────────────────────────────
    footer(_mis_company)


# ── PERSISTENT CHAT BAR ─────────────────────────────────────────────────────
st.markdown("---")
from chat_engine import ask, format_result_as_text

chat_input = st.chat_input("Ask anything — P&L, Balance Sheet, ledger of [party], debtors, creditors...")
if chat_input:
    result = ask(chat_input)
    st.markdown(f"**You:** {chat_input}")
    if result.get("type") == "chat":
        st.markdown(result.get("message", ""))
    else:
        st.markdown(format_result_as_text(result))


if __name__ == "__main__":
    main()
