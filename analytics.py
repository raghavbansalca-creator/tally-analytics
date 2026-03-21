"""
Seven Labs Vision — Analytics Engine
Comprehensive business analytics, cash flow, and projections.
"""

import sqlite3
import os
from datetime import datetime, timedelta
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), "tally_data.db")

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
    return conn.execute(f"""
        SELECT SUBSTR(v.DATE,1,6) as month,
               COUNT(DISTINCT v.GUID) as vch_count,
               SUM(ABS(CAST(a.AMOUNT AS REAL))) as sales_amt
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT = 'Sales Accounts'{date_filter}
        GROUP BY month ORDER BY month
    """, params).fetchall()


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
    return conn.execute(f"""
        SELECT SUBSTR(v.DATE,1,6) as month,
               COUNT(DISTINCT v.GUID) as vch_count,
               SUM(ABS(CAST(a.AMOUNT AS REAL))) as amt
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT = 'Purchase Accounts'{date_filter}
        GROUP BY month ORDER BY month
    """, params).fetchall()


def monthly_receipts_payments(conn, date_from=None, date_to=None):
    """Monthly cash inflows (receipts) and outflows (payments)."""
    date_filter = ""
    params = []
    if date_from:
        date_filter += " AND v.DATE >= ?"
        params.append(date_from)
    if date_to:
        date_filter += " AND v.DATE <= ?"
        params.append(date_to)
    receipts = conn.execute(f"""
        SELECT SUBSTR(v.DATE,1,6) as month,
               COUNT(DISTINCT v.GUID) as cnt,
               SUM(ABS(CAST(a.AMOUNT AS REAL))) as amt
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        WHERE v.VOUCHERTYPENAME = 'Receipt' AND a.ISDEEMEDPOSITIVE = 'Yes'{date_filter}
        GROUP BY month ORDER BY month
    """, params).fetchall()

    payments = conn.execute(f"""
        SELECT SUBSTR(v.DATE,1,6) as month,
               COUNT(DISTINCT v.GUID) as cnt,
               SUM(ABS(CAST(a.AMOUNT AS REAL))) as amt
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        WHERE v.VOUCHERTYPENAME = 'Payment' AND a.ISDEEMEDPOSITIVE = 'Yes'{date_filter}
        GROUP BY month ORDER BY month
    """, params).fetchall()

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
    return conn.execute(f"""
        SELECT SUBSTR(v.DATE,1,6) as month,
               a.LEDGERNAME,
               SUM(ABS(CAST(a.AMOUNT AS REAL))) as amt
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT = 'Indirect Expenses' AND CAST(a.AMOUNT AS REAL) < 0{date_filter}
        GROUP BY month, a.LEDGERNAME
        ORDER BY month, amt DESC
    """, params).fetchall()


def monthly_gross_profit(conn, date_from=None, date_to=None, voucher_types=None):
    """Monthly P&L summary: sales, purchases, gross profit, expenses, net profit.

    Optional filters:
        voucher_types: list of voucher type names to include
    """
    sales = {r[0]: r[2] for r in monthly_sales(conn, date_from=date_from, date_to=date_to,
                                                voucher_types=voucher_types)}
    purchases = {r[0]: r[2] for r in monthly_purchases(conn, date_from=date_from, date_to=date_to,
                                                        voucher_types=voucher_types)}

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
    exp_rows = conn.execute(f"""
        SELECT SUBSTR(v.DATE,1,6) as month,
               SUM(ABS(CAST(a.AMOUNT AS REAL))) as amt
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT = 'Indirect Expenses' AND CAST(a.AMOUNT AS REAL) < 0{date_filter}
        GROUP BY month ORDER BY month
    """, _params).fetchall()
    expenses = {r[0]: r[1] for r in exp_rows}

    months = sorted(set(list(sales.keys()) + list(purchases.keys())))
    result = []
    for m in months:
        s = sales.get(m, 0)
        p = purchases.get(m, 0)
        e = expenses.get(m, 0)
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
    return conn.execute(f"""
        SELECT v.PARTYLEDGERNAME as party,
               COUNT(DISTINCT v.GUID) as invoice_count,
               SUM(ABS(CAST(a.AMOUNT AS REAL))) as total_sales
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT = 'Sales Accounts'
          AND v.PARTYLEDGERNAME IS NOT NULL AND v.PARTYLEDGERNAME != ''{date_filter}
        GROUP BY party ORDER BY total_sales DESC LIMIT ?
    """, params).fetchall()


def top_suppliers_by_purchase(conn, limit=15, date_from=None, date_to=None,
                              voucher_types=None):
    """Top suppliers by purchase value.

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
    params.append(limit)
    return conn.execute(f"""
        SELECT v.PARTYLEDGERNAME as party,
               COUNT(DISTINCT v.GUID) as invoice_count,
               SUM(ABS(CAST(a.AMOUNT AS REAL))) as total_purchases
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT = 'Purchase Accounts'
          AND v.PARTYLEDGERNAME IS NOT NULL AND v.PARTYLEDGERNAME != ''{date_filter}
        GROUP BY party ORDER BY total_purchases DESC LIMIT ?
    """, params).fetchall()


def customer_monthly_sales(conn, top_n=5, date_from=None, date_to=None):
    """Monthly sales for top N customers."""
    top = top_customers_by_sales(conn, top_n, date_from=date_from, date_to=date_to)
    top_names = [r[0] for r in top]

    date_filter = ""
    params_extra = []
    if date_from:
        date_filter += " AND v.DATE >= ?"
        params_extra.append(date_from)
    if date_to:
        date_filter += " AND v.DATE <= ?"
        params_extra.append(date_to)

    result = {}
    for name in top_names:
        rows = conn.execute(f"""
            SELECT SUBSTR(v.DATE,1,6) as month,
                   SUM(ABS(CAST(a.AMOUNT AS REAL))) as amt
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT = 'Sales Accounts' AND v.PARTYLEDGERNAME = ?{date_filter}
            GROUP BY month ORDER BY month
        """, [name] + params_extra).fetchall()
        result[name] = {r[0]: r[1] for r in rows}
    return result


# ── BANK ANALYSIS ───────────────────────────────────────────────────────────

def bank_balances(conn, date_from=None, date_to=None):
    """All bank account balances."""
    if date_from or date_to:
        date_cond = ""
        params = []
        if date_from:
            date_cond += " AND v.DATE >= ?"
            params.append(date_from)
        if date_to:
            date_cond += " AND v.DATE <= ?"
            params.append(date_to)
        return conn.execute(f"""
            SELECT l.NAME, l.PARENT,
                   CAST(l.OPENINGBALANCE AS REAL) as opening,
                   COALESCE(CAST(l.OPENINGBALANCE AS REAL), 0) +
                   COALESCE((
                       SELECT SUM(CAST(a.AMOUNT AS REAL))
                       FROM trn_accounting a
                       JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                       WHERE a.LEDGERNAME = l.NAME{date_cond}
                   ), 0) as closing
            FROM mst_ledger l
            WHERE l.PARENT IN ('Bank Accounts', 'Bank OD A/c', 'Cash-in-Hand')
            ORDER BY ABS(closing) DESC
        """, params).fetchall()
    return conn.execute("""
        SELECT NAME, PARENT,
               CAST(OPENINGBALANCE AS REAL) as opening,
               CAST(CLOSINGBALANCE AS REAL) as closing
        FROM mst_ledger
        WHERE PARENT IN ('Bank Accounts', 'Bank OD A/c', 'Cash-in-Hand')
        ORDER BY ABS(CAST(CLOSINGBALANCE AS REAL)) DESC
    """).fetchall()


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
    return conn.execute(f"""
        SELECT SUBSTR(v.DATE,1,6) as month,
               a.LEDGERNAME,
               SUM(CASE WHEN CAST(a.AMOUNT AS REAL) < 0 THEN ABS(CAST(a.AMOUNT AS REAL)) ELSE 0 END) as debits,
               SUM(CASE WHEN CAST(a.AMOUNT AS REAL) > 0 THEN ABS(CAST(a.AMOUNT AS REAL)) ELSE 0 END) as credits
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT IN ('Bank Accounts', 'Bank OD A/c'){date_filter}
        GROUP BY month, a.LEDGERNAME
        ORDER BY month
    """, params).fetchall()


# ── CASH FLOW STATEMENT ────────────────────────────────────────────────────

def cash_flow_statement(conn, date_from=None, date_to=None):
    """
    Indirect method cash flow statement.
    Operating: Net Profit + Non-cash adjustments + Working capital changes
    Investing: Fixed asset movements
    Financing: Loan movements, capital changes
    """
    months_data = monthly_gross_profit(conn, date_from=date_from, date_to=date_to)

    # Operating activities - monthly
    monthly_cf = []

    for md in months_data:
        month = md["month"]

        # Net profit for the month
        net_profit = md["net_profit"]

        # Debtor movement (change in receivables)
        debtor_movement = conn.execute("""
            SELECT SUM(CAST(a.AMOUNT AS REAL))
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT = 'Sundry Debtors' AND SUBSTR(v.DATE,1,6) = ?
        """, (month,)).fetchone()[0] or 0

        # Creditor movement
        creditor_movement = conn.execute("""
            SELECT SUM(CAST(a.AMOUNT AS REAL))
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT = 'Sundry Creditors' AND SUBSTR(v.DATE,1,6) = ?
        """, (month,)).fetchone()[0] or 0

        # Tax/duty movement
        tax_movement = conn.execute("""
            SELECT SUM(CAST(a.AMOUNT AS REAL))
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT = 'Duties & Taxes' AND SUBSTR(v.DATE,1,6) = ?
        """, (month,)).fetchone()[0] or 0

        # Bank/Cash movement (actual cash change)
        bank_cash_movement = conn.execute("""
            SELECT SUM(CAST(a.AMOUNT AS REAL))
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT IN ('Bank Accounts', 'Bank OD A/c', 'Cash-in-Hand')
              AND SUBSTR(v.DATE,1,6) = ?
        """, (month,)).fetchone()[0] or 0

        # Loan movements
        loan_movement = conn.execute("""
            SELECT SUM(CAST(a.AMOUNT AS REAL))
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT IN ('Secured Loans', 'Unsecured Loans', 'Loans (Liability)')
              AND SUBSTR(v.DATE,1,6) = ?
        """, (month,)).fetchone()[0] or 0

        # Receipts and payments for the month
        receipts = conn.execute("""
            SELECT COALESCE(SUM(ABS(CAST(a.AMOUNT AS REAL))), 0)
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            WHERE v.VOUCHERTYPENAME = 'Receipt' AND a.ISDEEMEDPOSITIVE = 'Yes'
              AND SUBSTR(v.DATE,1,6) = ?
        """, (month,)).fetchone()[0]

        payments = conn.execute("""
            SELECT COALESCE(SUM(ABS(CAST(a.AMOUNT AS REAL))), 0)
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            WHERE v.VOUCHERTYPENAME = 'Payment' AND a.ISDEEMEDPOSITIVE = 'Yes'
              AND SUBSTR(v.DATE,1,6) = ?
        """, (month,)).fetchone()[0]

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
        return sum(r[key] * w for r, w in zip(recent, weights)) / total_weight

    avg_sales = weighted_avg("net_profit") + weighted_avg("debtor_change") - weighted_avg("creditor_change")
    avg_receipts = weighted_avg("receipts")
    avg_payments = weighted_avg("payments")
    avg_net_cf = weighted_avg("net_cash_flow")
    avg_net_profit = weighted_avg("net_profit")

    # Calculate trend (is it improving or declining?)
    if len(cf) >= 6:
        first_half = cf[:len(cf)//2]
        second_half = cf[len(cf)//2:]
        sales_trend = (sum(r["net_profit"] for r in second_half) / len(second_half)) - \
                      (sum(r["net_profit"] for r in first_half) / len(first_half))
    else:
        sales_trend = 0

    # Get last month's data
    last_month = cf[-1]["month"]
    last_year = int(last_month[:4])
    last_m = int(last_month[4:6])

    # Current bank + cash balance
    balances = bank_balances(conn)
    current_cash = sum(abs(r[3]) for r in balances if r[3] and r[3] < 0)  # debit balances
    current_cash += sum(r[3] for r in balances if r[3] and r[3] > 0)

    projections = []
    running_cash = current_cash

    for i in range(months_ahead):
        last_m += 1
        if last_m > 12:
            last_m = 1
            last_year += 1
        proj_month = f"{last_year}{last_m:02d}"

        # Apply slight trend adjustment
        trend_factor = 1 + (sales_trend / max(abs(avg_net_profit), 1)) * 0.1 * (i + 1)
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
    ca_groups = ['Sundry Debtors', 'Cash-in-Hand', 'Bank Accounts', 'Bank OD A/c',
                 'Stock-in-Hand', 'Deposits (Asset)', 'Loans & Advances (Asset)']
    cl_groups = ['Sundry Creditors', 'Duties & Taxes', 'Provisions']

    ca = {}
    for g in ca_groups:
        row = conn.execute("""
            SELECT COALESCE(SUM(ABS(CAST(CLOSINGBALANCE AS REAL))), 0)
            FROM mst_ledger WHERE PARENT = ?
        """, (g,)).fetchone()
        val = row[0] if row else 0
        if val > 0:
            ca[g] = val

    cl = {}
    for g in cl_groups:
        row = conn.execute("""
            SELECT COALESCE(SUM(ABS(CAST(CLOSINGBALANCE AS REAL))), 0)
            FROM mst_ledger WHERE PARENT = ?
        """, (g,)).fetchone()
        val = row[0] if row else 0
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
        "current_ratio": total_ca / total_cl if total_cl > 0 else 0,
    }


# ── KEY RATIOS ──────────────────────────────────────────────────────────────

def key_ratios(conn, date_from=None, date_to=None):
    """Calculate key financial ratios."""
    from tally_reports import profit_and_loss, balance_sheet

    pl = profit_and_loss(conn, from_date=date_from, to_date=date_to)
    bs = balance_sheet(conn, date_from=date_from, date_to=date_to)
    wc = working_capital_analysis(conn, date_from=date_from, date_to=date_to)

    total_sales = pl["total_income"]
    total_expenses = pl["total_expense"]
    net_profit = pl["net_profit"]
    gross_profit = pl["gross_profit"]
    total_assets = bs["total_assets"]
    total_liabilities = bs["total_liabilities"]

    # Debtor/creditor balances
    total_debtors = conn.execute("""
        SELECT COALESCE(SUM(ABS(CAST(CLOSINGBALANCE AS REAL))), 0)
        FROM mst_ledger WHERE PARENT = 'Sundry Debtors'
    """).fetchone()[0]

    total_creditors = conn.execute("""
        SELECT COALESCE(SUM(ABS(CAST(CLOSINGBALANCE AS REAL))), 0)
        FROM mst_ledger WHERE PARENT = 'Sundry Creditors'
    """).fetchone()[0]

    # Months of data
    months_count = len(set(r[0] for r in monthly_sales(conn, date_from=date_from, date_to=date_to)))
    annualized_sales = total_sales / months_count * 12 if months_count > 0 else total_sales
    annualized_purchases = total_expenses / months_count * 12 if months_count > 0 else total_expenses

    ratios = {
        "gross_profit_margin": (gross_profit / total_sales * 100) if total_sales > 0 else 0,
        "net_profit_margin": (net_profit / total_sales * 100) if total_sales > 0 else 0,
        "current_ratio": wc["current_ratio"],
        "working_capital": wc["working_capital"],
        "debtor_days": (total_debtors / annualized_sales * 365) if annualized_sales > 0 else 0,
        "creditor_days": (total_creditors / annualized_purchases * 365) if annualized_purchases > 0 else 0,
        "total_debtors": total_debtors,
        "total_creditors": total_creditors,
        "roa": (net_profit / total_assets * 100) if total_assets > 0 else 0,
    }

    return ratios


# ── COLLECTION EFFICIENCY ───────────────────────────────────────────────────

def collection_efficiency(conn, date_from=None, date_to=None):
    """Monthly collection efficiency: receipts as % of opening debtors + sales."""
    sales_data = {r[0]: r[2] for r in monthly_sales(conn, date_from=date_from, date_to=date_to)}
    receipt_data = {}
    date_filter = ""
    _params = []
    if date_from:
        date_filter += " AND v.DATE >= ?"
        _params.append(date_from)
    if date_to:
        date_filter += " AND v.DATE <= ?"
        _params.append(date_to)
    rows = conn.execute(f"""
        SELECT SUBSTR(v.DATE,1,6) as month,
               SUM(ABS(CAST(a.AMOUNT AS REAL))) as amt
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        WHERE v.VOUCHERTYPENAME = 'Receipt' AND a.ISDEEMEDPOSITIVE = 'Yes'{date_filter}
        GROUP BY month ORDER BY month
    """, _params).fetchall()
    for r in rows:
        receipt_data[r[0]] = r[1]

    months = sorted(set(list(sales_data.keys()) + list(receipt_data.keys())))
    result = []
    for m in months:
        s = sales_data.get(m, 0)
        r = receipt_data.get(m, 0)
        eff = (r / s * 100) if s > 0 else 0
        result.append({"month": m, "sales": s, "collections": r, "efficiency": eff})
    return result


# ── DRILL-DOWN QUERIES ─────────────────────────────────────────────────────

def drill_monthly_invoices(conn, month_code, ledger_parent):
    """Get all invoices for a given month and ledger parent (Sales Accounts / Purchase Accounts)."""
    return conn.execute("""
        SELECT v.GUID, v.DATE, v.VOUCHERNUMBER, v.PARTYLEDGERNAME,
               SUM(ABS(CAST(a.AMOUNT AS REAL))) as amount
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT = ? AND SUBSTR(v.DATE,1,6) = ?
        GROUP BY v.GUID
        ORDER BY v.DATE
    """, (ledger_parent, month_code)).fetchall()


def drill_party_invoices(conn, party_name, ledger_parent):
    """Get all invoices for a specific party under a ledger parent."""
    return conn.execute("""
        SELECT v.GUID, v.DATE, v.VOUCHERNUMBER, v.VOUCHERTYPENAME,
               SUM(ABS(CAST(a.AMOUNT AS REAL))) as amount
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT = ? AND v.PARTYLEDGERNAME = ?
        GROUP BY v.GUID
        ORDER BY v.DATE
    """, (ledger_parent, party_name)).fetchall()


def drill_voucher_entries(conn, voucher_guid):
    """Get all accounting entries for a specific voucher."""
    return conn.execute("""
        SELECT a.LEDGERNAME, CAST(a.AMOUNT AS REAL) as amount, a.ISDEEMEDPOSITIVE
        FROM trn_accounting a WHERE a.VOUCHER_GUID = ?
    """, (voucher_guid,)).fetchall()


def drill_voucher_header(conn, voucher_guid):
    """Get voucher header info."""
    return conn.execute("""
        SELECT v.GUID, v.DATE, v.VOUCHERNUMBER, v.VOUCHERTYPENAME,
               v.PARTYLEDGERNAME, v.NARRATION
        FROM trn_voucher v WHERE v.GUID = ?
    """, (voucher_guid,)).fetchone()


def drill_expense_transactions(conn, ledger_name, month_code):
    """Get all transactions for an expense ledger in a month."""
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


def drill_receipt_payment_vouchers(conn, month_code, voucher_type):
    """Get all receipt or payment vouchers for a month."""
    return conn.execute("""
        SELECT v.GUID, v.DATE, v.VOUCHERNUMBER, v.PARTYLEDGERNAME,
               SUM(ABS(CAST(a.AMOUNT AS REAL))) as amount
        FROM trn_voucher v
        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
        WHERE v.VOUCHERTYPENAME = ? AND a.ISDEEMEDPOSITIVE = 'Yes'
              AND SUBSTR(v.DATE,1,6) = ?
        GROUP BY v.GUID
        ORDER BY v.DATE
    """, (voucher_type, month_code)).fetchall()


def drill_bank_transactions(conn, bank_name):
    """Get all transactions for a bank account (like a bank statement)."""
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
