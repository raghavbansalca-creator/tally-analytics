"""
Seven Labs Vision — Tally Report Engine
SQL templates for standard accounting reports against SQLite.
All amounts in Tally: positive = credit/income side, negative = debit/expense side.
For debtors: closing balance negative means they owe us (receivable).
ISDEEMEDPOSITIVE = Yes means the natural balance is debit (assets/expenses).
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "tally_data.db")


def get_conn():
    return sqlite3.connect(DB_PATH)


# ── TALLY GROUP CLASSIFICATION ──────────────────────────────────────────────
# Primary groups and their Balance Sheet / P&L classification.
# In Tally: ISREVENUE = 'Yes' means P&L group; 'No' means Balance Sheet group.

BS_ASSET_ROOTS = ["Current Assets", "Fixed Assets", "Investments", "Misc. Expenses (ASSET)"]
BS_LIABILITY_ROOTS = ["Current Liabilities", "Loans (Liability)", "Capital Account", "Branch / Divisions", "Suspense A/c"]
PL_INCOME_ROOTS = ["Sales Accounts", "Direct Incomes", "Indirect Incomes"]
PL_EXPENSE_ROOTS = ["Purchase Accounts", "Direct Expenses", "Indirect Expenses"]


def get_all_groups_under(conn, root_groups):
    """Get all group names that fall under any of the root groups (recursive)."""
    all_groups = set()
    queue = list(root_groups)
    while queue:
        parent = queue.pop(0)
        all_groups.add(parent)
        children = conn.execute(
            "SELECT NAME FROM mst_group WHERE PARENT = ?", (parent,)
        ).fetchall()
        for (child,) in children:
            if child not in all_groups:
                queue.append(child)
    return all_groups


def get_ledger_totals_by_group(conn, group_names, as_of_date=None):
    """Get ledger closing balances grouped by their parent group.
    Returns dict: {group_name: [(ledger_name, closing_balance), ...]}

    If as_of_date is provided, calculates from opening + transactions up to that date.
    Otherwise uses the stored closing balance from mst_ledger.
    """
    if not group_names:
        return {}

    placeholders = ",".join(["?"] * len(group_names))

    if as_of_date:
        # Calculate from opening balance + sum of transactions
        sql = f"""
        SELECT l.PARENT, l.NAME,
               COALESCE(CAST(l.OPENINGBALANCE AS REAL), 0) +
               COALESCE((
                   SELECT SUM(CAST(a.AMOUNT AS REAL))
                   FROM trn_accounting a
                   JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                   WHERE a.LEDGERNAME = l.NAME AND v.DATE <= ?
               ), 0) as balance
        FROM mst_ledger l
        WHERE l.PARENT IN ({placeholders})
        ORDER BY l.PARENT, l.NAME
        """
        rows = conn.execute(sql, [as_of_date] + list(group_names)).fetchall()
    else:
        sql = f"""
        SELECT PARENT, NAME, CAST(CLOSINGBALANCE AS REAL) as balance
        FROM mst_ledger
        WHERE PARENT IN ({placeholders})
        ORDER BY PARENT, NAME
        """
        rows = conn.execute(sql, list(group_names)).fetchall()

    result = {}
    for parent, name, balance in rows:
        if parent not in result:
            result[parent] = []
        result[parent].append((name, balance or 0.0))
    return result


# ── TRIAL BALANCE ────────────────────────────────────────────────────────────

def trial_balance(conn, as_of_date=None):
    """Generate Trial Balance: all ledgers with their closing balances.
    Returns list of (group, ledger, debit, credit)."""
    if as_of_date:
        sql = """
        SELECT l.PARENT, l.NAME,
               COALESCE(CAST(l.OPENINGBALANCE AS REAL), 0) +
               COALESCE((
                   SELECT SUM(CAST(a.AMOUNT AS REAL))
                   FROM trn_accounting a
                   JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                   WHERE a.LEDGERNAME = l.NAME AND v.DATE <= ?
               ), 0) as balance
        FROM mst_ledger l
        ORDER BY l.PARENT, l.NAME
        """
        rows = conn.execute(sql, [as_of_date]).fetchall()
    else:
        sql = """
        SELECT PARENT, NAME, CAST(CLOSINGBALANCE AS REAL) as balance
        FROM mst_ledger
        ORDER BY PARENT, NAME
        """
        rows = conn.execute(sql).fetchall()

    result = []
    for group, name, balance in rows:
        bal = balance or 0.0
        if bal == 0:
            continue
        # In Tally: negative closing balance = debit balance for liabilities/income
        # positive closing balance = debit balance for assets/expenses
        # We need to determine debit/credit based on the amount sign
        debit = abs(bal) if bal < 0 else 0.0
        credit = abs(bal) if bal > 0 else 0.0
        result.append((group, name, debit, credit))

    return result


# ── PROFIT & LOSS ────────────────────────────────────────────────────────────

def profit_and_loss(conn, from_date=None, to_date=None):
    """Generate Profit & Loss statement.
    Returns dict with income groups, expense groups, and net profit.

    Structure: {
        'income': {group_name: [(ledger, amount), ...]},
        'expense': {group_name: [(ledger, amount), ...]},
        'gross_profit': float,
        'net_profit': float,
        'total_income': float,
        'total_expense': float,
    }
    """
    # Get all income and expense group names
    income_groups = get_all_groups_under(conn, PL_INCOME_ROOTS)
    expense_groups = get_all_groups_under(conn, PL_EXPENSE_ROOTS)

    # Calculate P&L from transactions within the period
    def get_pl_amounts(group_names):
        if not group_names:
            return {}
        placeholders = ",".join(["?"] * len(group_names))

        if from_date and to_date:
            sql = f"""
            SELECT l.PARENT, a.LEDGERNAME, SUM(CAST(a.AMOUNT AS REAL)) as total
            FROM trn_accounting a
            JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT IN ({placeholders})
              AND v.DATE >= ? AND v.DATE <= ?
            GROUP BY l.PARENT, a.LEDGERNAME
            HAVING total != 0
            ORDER BY l.PARENT, total
            """
            params = list(group_names) + [from_date, to_date]
        else:
            sql = f"""
            SELECT l.PARENT, a.LEDGERNAME, SUM(CAST(a.AMOUNT AS REAL)) as total
            FROM trn_accounting a
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT IN ({placeholders})
            GROUP BY l.PARENT, a.LEDGERNAME
            HAVING total != 0
            ORDER BY l.PARENT, total
            """
            params = list(group_names)

        rows = conn.execute(sql, params).fetchall()
        result = {}
        for parent, ledger, total in rows:
            if parent not in result:
                result[parent] = []
            result[parent].append((ledger, total))
        return result

    income = get_pl_amounts(income_groups)
    expense = get_pl_amounts(expense_groups)

    # Calculate totals
    # Income amounts are positive (credit), expenses are positive (debit)
    total_income = sum(abs(amt) for entries in income.values() for _, amt in entries)
    total_expense = sum(abs(amt) for entries in expense.values() for _, amt in entries)

    # Gross profit: Direct Income - Direct Expense
    direct_income_groups = get_all_groups_under(conn, ["Sales Accounts", "Direct Incomes"])
    direct_expense_groups = get_all_groups_under(conn, ["Purchase Accounts", "Direct Expenses"])
    gross_income = sum(abs(amt) for g, entries in income.items() if g in direct_income_groups for _, amt in entries)
    gross_expense = sum(abs(amt) for g, entries in expense.items() if g in direct_expense_groups for _, amt in entries)

    return {
        "income": income,
        "expense": expense,
        "gross_profit": gross_income - gross_expense,
        "net_profit": total_income - total_expense,
        "total_income": total_income,
        "total_expense": total_expense,
    }


# ── BALANCE SHEET ────────────────────────────────────────────────────────────

def balance_sheet(conn, as_of_date=None):
    """Generate Balance Sheet.
    Returns dict with assets and liabilities grouped.

    Structure: {
        'assets': {group_name: [(ledger, balance), ...]},
        'liabilities': {group_name: [(ledger, balance), ...]},
        'total_assets': float,
        'total_liabilities': float,
    }
    """
    asset_groups = get_all_groups_under(conn, BS_ASSET_ROOTS)
    liability_groups = get_all_groups_under(conn, BS_LIABILITY_ROOTS)

    assets = get_ledger_totals_by_group(conn, asset_groups, as_of_date)
    liabilities = get_ledger_totals_by_group(conn, liability_groups, as_of_date)

    # Filter zero balances
    assets = {g: [(n, b) for n, b in entries if b != 0] for g, entries in assets.items()}
    liabilities = {g: [(n, b) for n, b in entries if b != 0] for g, entries in liabilities.items()}
    assets = {g: e for g, e in assets.items() if e}
    liabilities = {g: e for g, e in liabilities.items() if e}

    # Assets: negative closing balance in Tally = debit balance = asset
    total_assets = sum(abs(bal) for entries in assets.values() for _, bal in entries)
    total_liabilities = sum(abs(bal) for entries in liabilities.values() for _, bal in entries)

    return {
        "assets": assets,
        "liabilities": liabilities,
        "total_assets": total_assets,
        "total_liabilities": total_liabilities,
    }


# ── LEDGER DETAIL (STATEMENT OF ACCOUNT) ────────────────────────────────────

def ledger_detail(conn, ledger_name, from_date=None, to_date=None):
    """Get all transactions for a specific ledger with running balance.
    Returns: (opening_balance, transactions_list, closing_balance)
    Each transaction: (date, vch_type, vch_number, narration, debit, credit, balance)
    """
    # Opening balance
    ob = conn.execute(
        "SELECT CAST(OPENINGBALANCE AS REAL) FROM mst_ledger WHERE NAME = ?",
        (ledger_name,)
    ).fetchone()
    opening = ob[0] if ob and ob[0] else 0.0

    # Transactions
    date_filter = ""
    params = [ledger_name]
    if from_date:
        date_filter += " AND v.DATE >= ?"
        params.append(from_date)
    if to_date:
        date_filter += " AND v.DATE <= ?"
        params.append(to_date)

    sql = f"""
    SELECT v.DATE, v.VOUCHERTYPENAME, v.VOUCHERNUMBER, v.NARRATION,
           CAST(a.AMOUNT AS REAL), v.PARTYLEDGERNAME
    FROM trn_accounting a
    JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
    WHERE a.LEDGERNAME = ?{date_filter}
    ORDER BY v.DATE, v.VOUCHERNUMBER
    """
    rows = conn.execute(sql, params).fetchall()

    transactions = []
    running = opening
    for date, vtype, vnum, narration, amount, party in rows:
        amt = amount or 0.0
        debit = abs(amt) if amt < 0 else 0.0
        credit = abs(amt) if amt > 0 else 0.0
        running += amt
        transactions.append({
            "date": date,
            "voucher_type": vtype or "",
            "voucher_number": vnum or "",
            "narration": narration or "",
            "party": party or "",
            "debit": debit,
            "credit": credit,
            "balance": running,
        })

    return opening, transactions, running


# ── GROUP-WISE P&L DRILLDOWN ─────────────────────────────────────────────────

def pl_group_drilldown(conn, group_name, from_date=None, to_date=None):
    """Drilldown into a P&L group: show all transactions under that group.
    Returns list of dicts with date, voucher, ledger, party, amount."""
    all_groups = get_all_groups_under(conn, [group_name])
    placeholders = ",".join(["?"] * len(all_groups))

    date_filter = ""
    params = list(all_groups)
    if from_date:
        date_filter += " AND v.DATE >= ?"
        params.append(from_date)
    if to_date:
        date_filter += " AND v.DATE <= ?"
        params.append(to_date)

    sql = f"""
    SELECT v.DATE, v.VOUCHERTYPENAME, v.VOUCHERNUMBER, a.LEDGERNAME,
           v.PARTYLEDGERNAME, CAST(a.AMOUNT AS REAL), v.NARRATION
    FROM trn_accounting a
    JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
    JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
    WHERE l.PARENT IN ({placeholders}){date_filter}
    ORDER BY v.DATE, v.VOUCHERNUMBER
    """
    rows = conn.execute(sql, params).fetchall()

    return [{
        "date": r[0], "voucher_type": r[1], "voucher_number": r[2],
        "ledger": r[3], "party": r[4], "amount": r[5], "narration": r[6],
    } for r in rows]


# ── DEBTOR/CREDITOR AGING ────────────────────────────────────────────────────

def debtor_aging(conn):
    """Simple debtor aging based on closing balances from mst_ledger.
    Groups: Sundry Debtors."""
    sql = """
    SELECT NAME, CAST(CLOSINGBALANCE AS REAL) as balance
    FROM mst_ledger
    WHERE PARENT = 'Sundry Debtors'
      AND CAST(CLOSINGBALANCE AS REAL) != 0
    ORDER BY CAST(CLOSINGBALANCE AS REAL)
    """
    rows = conn.execute(sql).fetchall()
    return [(name, abs(bal)) for name, bal in rows if bal]


def creditor_aging(conn):
    """Simple creditor listing based on closing balances."""
    sql = """
    SELECT NAME, CAST(CLOSINGBALANCE AS REAL) as balance
    FROM mst_ledger
    WHERE PARENT = 'Sundry Creditors'
      AND CAST(CLOSINGBALANCE AS REAL) != 0
    ORDER BY CAST(CLOSINGBALANCE AS REAL)
    """
    rows = conn.execute(sql).fetchall()
    return [(name, abs(bal)) for name, bal in rows if bal]


# ── VOUCHER TYPE SUMMARY ────────────────────────────────────────────────────

def voucher_summary(conn, from_date=None, to_date=None):
    """Summary of vouchers by type."""
    date_filter = ""
    params = []
    if from_date:
        date_filter += " AND DATE >= ?"
        params.append(from_date)
    if to_date:
        date_filter += " AND DATE <= ?"
        params.append(to_date)

    sql = f"""
    SELECT VOUCHERTYPENAME, COUNT(*) as count,
           SUM(CASE WHEN CAST(a_total AS REAL) > 0 THEN CAST(a_total AS REAL) ELSE 0 END) as total_amount
    FROM (
        SELECT v.VOUCHERTYPENAME,
               (SELECT SUM(ABS(CAST(a.AMOUNT AS REAL)))
                FROM trn_accounting a WHERE a.VOUCHER_GUID = v.GUID
                AND CAST(a.AMOUNT AS REAL) > 0) as a_total
        FROM trn_voucher v
        WHERE 1=1{date_filter}
    )
    GROUP BY VOUCHERTYPENAME
    ORDER BY count DESC
    """
    return conn.execute(sql, params).fetchall()


# ── SEARCH ───────────────────────────────────────────────────────────────────

def search_ledger(conn, query):
    """Search ledgers by name (fuzzy)."""
    sql = """
    SELECT NAME, PARENT, CAST(CLOSINGBALANCE AS REAL) as balance
    FROM mst_ledger
    WHERE NAME LIKE ?
    ORDER BY NAME
    LIMIT 20
    """
    return conn.execute(sql, [f"%{query}%"]).fetchall()


# ── QUICK TEST ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    conn = get_conn()

    print("=== TRIAL BALANCE (top 10) ===")
    tb = trial_balance(conn)
    total_dr = sum(d for _, _, d, _ in tb)
    total_cr = sum(c for _, _, _, c in tb)
    for g, n, d, c in tb[:10]:
        print(f"  {g:25s} {n:35s} Dr: {d:>12,.2f}  Cr: {c:>12,.2f}")
    print(f"  {'TOTAL':>62s} Dr: {total_dr:>12,.2f}  Cr: {total_cr:>12,.2f}")

    print("\n=== PROFIT & LOSS ===")
    pl = profit_and_loss(conn)
    print(f"  Total Income:  {pl['total_income']:>12,.2f}")
    print(f"  Total Expense: {pl['total_expense']:>12,.2f}")
    print(f"  Gross Profit:  {pl['gross_profit']:>12,.2f}")
    print(f"  Net Profit:    {pl['net_profit']:>12,.2f}")
    print(f"  Income groups: {list(pl['income'].keys())}")
    print(f"  Expense groups: {list(pl['expense'].keys())}")

    print("\n=== BALANCE SHEET ===")
    bs = balance_sheet(conn)
    print(f"  Total Assets:      {bs['total_assets']:>12,.2f}")
    print(f"  Total Liabilities: {bs['total_liabilities']:>12,.2f}")
    print(f"  Asset groups: {list(bs['assets'].keys())}")
    print(f"  Liability groups: {list(bs['liabilities'].keys())}")

    conn.close()
