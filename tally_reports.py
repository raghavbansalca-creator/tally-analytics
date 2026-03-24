"""
Seven Labs Vision — Tally Report Engine
SQL templates for standard accounting reports against SQLite.
All amounts in Tally: positive = credit/income side, negative = debit/expense side.
For debtors: closing balance negative means they owe us (receivable).
ISDEEMEDPOSITIVE = Yes means the natural balance is debit (assets/expenses).
Defensive coding: works with ANY company's Tally data.
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "tally_data.db")

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


def clear_col_cache():
    """Clear the column cache (call after re-sync)."""
    _TABLE_COLS.clear()


def _bal_col(conn):
    """Return the best balance column: COMPUTED_CB if available, else CLOSINGBALANCE."""
    cols = _get_cols(conn, "mst_ledger")
    return "COMPUTED_CB" if "COMPUTED_CB" in cols else "CLOSINGBALANCE"


def get_conn():
    return sqlite3.connect(DB_PATH)


# ── TALLY GROUP CLASSIFICATION (FLAG-BASED) ─────────────────────────────────
# Uses Tally's own ISREVENUE + ISDEEMEDPOSITIVE flags from mst_group:
#   ISREVENUE + ISDEEMEDPOSITIVE = Classification:
#     Yes + No  = INCOME  (Sales, Direct/Indirect Incomes)
#     Yes + Yes = EXPENSE (Purchases, Direct/Indirect Expenses)
#     No  + Yes = ASSET   (Bank, Cash, Debtors, Fixed Assets, Stock)
#     No  + No  = LIABILITY (Capital, Creditors, Loans, Duties)

# Legacy hardcoded roots — kept as fallback only if mst_group has no flag columns.
BS_ASSET_ROOTS = ["Current Assets", "Fixed Assets", "Investments", "Misc. Expenses (ASSET)"]
BS_LIABILITY_ROOTS = ["Current Liabilities", "Loans (Liability)", "Capital Account", "Branch / Divisions", "Suspense A/c"]
PL_INCOME_ROOTS = ["Sales Accounts", "Direct Incomes", "Indirect Incomes"]
PL_EXPENSE_ROOTS = ["Purchase Accounts", "Direct Expenses", "Indirect Expenses"]


def _get_recursive(conn, parent):
    """Get parent + all descendants recursively."""
    if not _table_exists(conn, "mst_group"):
        return [parent]
    result = [parent]
    queue = [parent]
    while queue:
        curr = queue.pop(0)
        try:
            children = [r[0] for r in conn.execute(
                "SELECT NAME FROM mst_group WHERE PARENT=?", (curr,)
            ).fetchall()]
        except sqlite3.OperationalError:
            children = []
        for c in children:
            if c not in result:
                result.append(c)
                queue.append(c)
    return result


def _has_group_flags(conn):
    """Check if mst_group has ISREVENUE and ISDEEMEDPOSITIVE columns."""
    cols = _get_cols(conn, "mst_group")
    return "ISREVENUE" in cols and "ISDEEMEDPOSITIVE" in cols


def get_groups_by_nature(conn, nature):
    """Get all group names by their financial nature using Tally's own flags.

    nature: 'income', 'expense', 'asset', 'liability',
            'sales' (subset of income), 'purchase' (subset of expense),
            'direct_income', 'indirect_income',
            'direct_expense', 'indirect_expense',
            'debtors', 'creditors', 'bank', 'cash',
            'bank_od', 'loans', 'duties_taxes',
            'fixed_assets', 'stock', 'capital'

    Returns list of group names (includes all sub-groups via Tally flags).
    """
    if not _table_exists(conn, "mst_group") or not _has_group_flags(conn):
        # Fallback to hardcoded roots
        fallback_map = {
            'income': PL_INCOME_ROOTS,
            'expense': PL_EXPENSE_ROOTS,
            'asset': BS_ASSET_ROOTS,
            'liability': BS_LIABILITY_ROOTS,
            'sales': ["Sales Accounts"],
            'purchase': ["Purchase Accounts"],
            'direct_income': ["Direct Incomes"],
            'indirect_income': ["Indirect Incomes"],
            'direct_expense': ["Direct Expenses"],
            'indirect_expense': ["Indirect Expenses"],
            'debtors': ["Sundry Debtors"],
            'creditors': ["Sundry Creditors"],
            'bank': ["Bank Accounts"],
            'bank_od': ["Bank OD A/c"],
            'cash': ["Cash-in-Hand"],
            'loans': ["Secured Loans", "Unsecured Loans", "Loans (Liability)"],
            'duties_taxes': ["Duties & Taxes"],
            'fixed_assets': ["Fixed Assets"],
            'stock': ["Stock-in-Hand"],
            'capital': ["Capital Account"],
        }
        roots = fallback_map.get(nature, [])
        result = set()
        for r in roots:
            result.update(get_all_groups_under(conn, [r]))
        return list(result)

    try:
        if nature == 'income':
            groups = [r[0] for r in conn.execute(
                "SELECT NAME FROM mst_group WHERE ISREVENUE='Yes' AND ISDEEMEDPOSITIVE='No'"
            ).fetchall()]
        elif nature == 'expense':
            groups = [r[0] for r in conn.execute(
                "SELECT NAME FROM mst_group WHERE ISREVENUE='Yes' AND ISDEEMEDPOSITIVE='Yes'"
            ).fetchall()]
        elif nature == 'asset':
            groups = [r[0] for r in conn.execute(
                "SELECT NAME FROM mst_group WHERE ISREVENUE='No' AND ISDEEMEDPOSITIVE='Yes'"
            ).fetchall()]
        elif nature == 'liability':
            groups = [r[0] for r in conn.execute(
                "SELECT NAME FROM mst_group WHERE ISREVENUE='No' AND ISDEEMEDPOSITIVE='No'"
            ).fetchall()]
        elif nature == 'sales':
            # Sales = income groups that affect gross profit
            groups = [r[0] for r in conn.execute(
                "SELECT NAME FROM mst_group WHERE ISREVENUE='Yes' AND ISDEEMEDPOSITIVE='No' AND AFFECTSGROSSPROFIT='Yes'"
            ).fetchall()]
        elif nature == 'purchase':
            # Purchase = expense groups that affect gross profit
            groups = [r[0] for r in conn.execute(
                "SELECT NAME FROM mst_group WHERE ISREVENUE='Yes' AND ISDEEMEDPOSITIVE='Yes' AND AFFECTSGROSSPROFIT='Yes'"
            ).fetchall()]
        elif nature == 'direct_income':
            groups = [r[0] for r in conn.execute(
                "SELECT NAME FROM mst_group WHERE ISREVENUE='Yes' AND ISDEEMEDPOSITIVE='No' AND AFFECTSGROSSPROFIT='Yes'"
            ).fetchall()]
        elif nature == 'indirect_income':
            groups = [r[0] for r in conn.execute(
                "SELECT NAME FROM mst_group WHERE ISREVENUE='Yes' AND ISDEEMEDPOSITIVE='No' AND AFFECTSGROSSPROFIT='No'"
            ).fetchall()]
        elif nature == 'direct_expense':
            groups = [r[0] for r in conn.execute(
                "SELECT NAME FROM mst_group WHERE ISREVENUE='Yes' AND ISDEEMEDPOSITIVE='Yes' AND AFFECTSGROSSPROFIT='Yes'"
            ).fetchall()]
        elif nature == 'indirect_expense':
            groups = [r[0] for r in conn.execute(
                "SELECT NAME FROM mst_group WHERE ISREVENUE='Yes' AND ISDEEMEDPOSITIVE='Yes' AND AFFECTSGROSSPROFIT='No'"
            ).fetchall()]
        elif nature == 'debtors':
            groups = _get_recursive(conn, 'Sundry Debtors')
        elif nature == 'creditors':
            groups = _get_recursive(conn, 'Sundry Creditors')
        elif nature == 'bank':
            groups = _get_recursive(conn, 'Bank Accounts')
        elif nature == 'bank_od':
            groups = _get_recursive(conn, 'Bank OD A/c')
        elif nature == 'cash':
            groups = _get_recursive(conn, 'Cash-in-Hand')
        elif nature == 'loans':
            groups = _get_recursive(conn, 'Secured Loans') + _get_recursive(conn, 'Unsecured Loans') + _get_recursive(conn, 'Loans (Liability)')
            groups = list(dict.fromkeys(groups))  # deduplicate preserving order
        elif nature == 'duties_taxes':
            groups = _get_recursive(conn, 'Duties & Taxes')
        elif nature == 'fixed_assets':
            groups = _get_recursive(conn, 'Fixed Assets')
        elif nature == 'stock':
            groups = _get_recursive(conn, 'Stock-in-Hand')
        elif nature == 'capital':
            groups = _get_recursive(conn, 'Capital Account')
        else:
            groups = []
    except sqlite3.OperationalError:
        groups = []
    return groups


def get_all_groups_under(conn, root_groups):
    """Get all group names that fall under any of the root groups (recursive).
    NOTE: Prefer get_groups_by_nature() which uses Tally's own flags."""
    if not _table_exists(conn, "mst_group"):
        return set(root_groups)

    all_groups = set()
    queue = list(root_groups)
    while queue:
        parent = queue.pop(0)
        all_groups.add(parent)
        try:
            children = conn.execute(
                "SELECT NAME FROM mst_group WHERE PARENT = ?", (parent,)
            ).fetchall()
        except sqlite3.OperationalError:
            children = []
        for (child,) in children:
            if child and child not in all_groups:
                queue.append(child)
    return all_groups


def get_ledger_totals_by_group(conn, group_names, as_of_date=None, date_from=None, date_to=None):
    """Get ledger closing balances grouped by their parent group.
    Returns dict: {group_name: [(ledger_name, closing_balance), ...]}

    If as_of_date is provided, calculates from opening + transactions up to that date.
    Otherwise uses the stored closing balance from mst_ledger.
    """
    if not group_names:
        return {}

    lcols = _get_cols(conn, "mst_ledger")
    has_ob = "OPENINGBALANCE" in lcols
    has_cb = "COMPUTED_CB" in lcols or "CLOSINGBALANCE" in lcols

    placeholders = ",".join(["?"] * len(group_names))

    if date_from or date_to:
        date_cond = ""
        date_params = []
        if date_from:
            date_cond += " AND v.DATE >= ?"
            date_params.append(date_from)
        if date_to:
            date_cond += " AND v.DATE <= ?"
            date_params.append(date_to)
        ob_expr = "CAST(l.OPENINGBALANCE AS REAL)" if has_ob else "0"
        sql = f"""
        SELECT l.PARENT, l.NAME,
               COALESCE({ob_expr}, 0) +
               COALESCE((
                   SELECT SUM(CAST(a.AMOUNT AS REAL))
                   FROM trn_accounting a
                   JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                   WHERE a.LEDGERNAME = l.NAME{date_cond}
               ), 0) as balance
        FROM mst_ledger l
        WHERE l.PARENT IN ({placeholders})
        ORDER BY l.PARENT, l.NAME
        """
        try:
            rows = conn.execute(sql, date_params + list(group_names)).fetchall()
        except sqlite3.OperationalError:
            rows = []
    elif as_of_date:
        ob_expr = "CAST(l.OPENINGBALANCE AS REAL)" if has_ob else "0"
        sql = f"""
        SELECT l.PARENT, l.NAME,
               COALESCE({ob_expr}, 0) +
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
        try:
            rows = conn.execute(sql, [as_of_date] + list(group_names)).fetchall()
        except sqlite3.OperationalError:
            rows = []
    elif has_cb:
        bc = _bal_col(conn)
        sql = f"""
        SELECT PARENT, NAME, CAST({bc} AS REAL) as balance
        FROM mst_ledger
        WHERE PARENT IN ({placeholders})
        ORDER BY PARENT, NAME
        """
        try:
            rows = conn.execute(sql, list(group_names)).fetchall()
        except sqlite3.OperationalError:
            rows = []
    else:
        # No closing balance column — compute from opening + all transactions
        ob_expr = "CAST(l.OPENINGBALANCE AS REAL)" if has_ob else "0"
        sql = f"""
        SELECT l.PARENT, l.NAME,
               COALESCE({ob_expr}, 0) +
               COALESCE((
                   SELECT SUM(CAST(a.AMOUNT AS REAL))
                   FROM trn_accounting a
                   WHERE a.LEDGERNAME = l.NAME
               ), 0) as balance
        FROM mst_ledger l
        WHERE l.PARENT IN ({placeholders})
        ORDER BY l.PARENT, l.NAME
        """
        try:
            rows = conn.execute(sql, list(group_names)).fetchall()
        except sqlite3.OperationalError:
            rows = []

    result = {}
    for parent, name, balance in rows:
        if parent not in result:
            result[parent] = []
        result[parent].append((name, balance or 0.0))
    return result


# ── TRIAL BALANCE ────────────────────────────────────────────────────────────

def trial_balance(conn, as_of_date=None, date_from=None, date_to=None):
    """Generate Trial Balance: all ledgers with their closing balances.
    Returns list of (group, ledger, debit, credit)."""
    lcols = _get_cols(conn, "mst_ledger")
    has_ob = "OPENINGBALANCE" in lcols
    has_cb = "COMPUTED_CB" in lcols or "CLOSINGBALANCE" in lcols

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
        sql = f"""
        SELECT l.PARENT, l.NAME,
               COALESCE({ob_expr}, 0) +
               COALESCE((
                   SELECT SUM(CAST(a.AMOUNT AS REAL))
                   FROM trn_accounting a
                   JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                   WHERE a.LEDGERNAME = l.NAME{date_cond}
               ), 0) as balance
        FROM mst_ledger l
        ORDER BY l.PARENT, l.NAME
        """
        try:
            rows = conn.execute(sql, params).fetchall()
        except sqlite3.OperationalError:
            rows = []
    elif as_of_date:
        ob_expr = "CAST(l.OPENINGBALANCE AS REAL)" if has_ob else "0"
        sql = f"""
        SELECT l.PARENT, l.NAME,
               COALESCE({ob_expr}, 0) +
               COALESCE((
                   SELECT SUM(CAST(a.AMOUNT AS REAL))
                   FROM trn_accounting a
                   JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                   WHERE a.LEDGERNAME = l.NAME AND v.DATE <= ?
               ), 0) as balance
        FROM mst_ledger l
        ORDER BY l.PARENT, l.NAME
        """
        try:
            rows = conn.execute(sql, [as_of_date]).fetchall()
        except sqlite3.OperationalError:
            rows = []
    elif has_cb:
        bc = _bal_col(conn)
        sql = f"""
        SELECT PARENT, NAME, CAST({bc} AS REAL) as balance
        FROM mst_ledger
        ORDER BY PARENT, NAME
        """
        try:
            rows = conn.execute(sql).fetchall()
        except sqlite3.OperationalError:
            rows = []
    else:
        # No closing balance — compute from opening + all transactions
        ob_expr = "CAST(l.OPENINGBALANCE AS REAL)" if has_ob else "0"
        sql = f"""
        SELECT l.PARENT, l.NAME,
               COALESCE({ob_expr}, 0) +
               COALESCE((
                   SELECT SUM(CAST(a.AMOUNT AS REAL))
                   FROM trn_accounting a
                   WHERE a.LEDGERNAME = l.NAME
               ), 0) as balance
        FROM mst_ledger l
        ORDER BY l.PARENT, l.NAME
        """
        try:
            rows = conn.execute(sql).fetchall()
        except sqlite3.OperationalError:
            rows = []

    result = []
    for group, name, balance in rows:
        bal = balance or 0.0
        if bal == 0:
            continue
        debit = abs(bal) if bal < 0 else 0.0
        credit = abs(bal) if bal > 0 else 0.0
        result.append((group, name, debit, credit))

    return result


# ── PROFIT & LOSS ────────────────────────────────────────────────────────────

def profit_and_loss(conn, from_date=None, to_date=None,
                    voucher_types=None, ledger_groups=None):
    """Generate Profit & Loss statement.
    Returns dict with income groups, expense groups, and net profit.

    Optional filters (all default to None = no filter):
        voucher_types: list of voucher type names to include
        ledger_groups: list of ledger parent groups to include

    Structure: {
        'income': {group_name: [(ledger, amount), ...]},
        'expense': {group_name: [(ledger, amount), ...]},
        'gross_profit': float,
        'net_profit': float,
        'total_income': float,
        'total_expense': float,
    }
    """
    # Get all income and expense group names using Tally's own flags
    income_groups = set(get_groups_by_nature(conn, 'income'))
    expense_groups = set(get_groups_by_nature(conn, 'expense'))

    # If ledger_groups filter is set, narrow down to matching groups only
    if ledger_groups:
        ledger_groups_set = set(ledger_groups)
        income_groups = income_groups & ledger_groups_set
        expense_groups = expense_groups & ledger_groups_set

    # Calculate P&L from transactions within the period
    def get_pl_amounts(group_names):
        if not group_names:
            return {}
        placeholders = ",".join(["?"] * len(group_names))

        # Build extra filter clauses
        extra_where = ""
        extra_params = []
        if voucher_types:
            vt_ph = ",".join(["?"] * len(voucher_types))
            extra_where += f" AND v.VOUCHERTYPENAME IN ({vt_ph})"
            extra_params.extend(voucher_types)

        if from_date and to_date:
            sql = f"""
            SELECT l.PARENT, a.LEDGERNAME, SUM(CAST(a.AMOUNT AS REAL)) as total
            FROM trn_accounting a
            JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT IN ({placeholders})
              AND v.DATE >= ? AND v.DATE <= ?{extra_where}
            GROUP BY l.PARENT, a.LEDGERNAME
            HAVING total != 0
            ORDER BY l.PARENT, total
            """
            params = list(group_names) + [from_date, to_date] + extra_params
        elif voucher_types:
            sql = f"""
            SELECT l.PARENT, a.LEDGERNAME, SUM(CAST(a.AMOUNT AS REAL)) as total
            FROM trn_accounting a
            JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
            JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
            WHERE l.PARENT IN ({placeholders}){extra_where}
            GROUP BY l.PARENT, a.LEDGERNAME
            HAVING total != 0
            ORDER BY l.PARENT, total
            """
            params = list(group_names) + extra_params
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

        try:
            rows = conn.execute(sql, params).fetchall()
        except sqlite3.OperationalError:
            rows = []

        result = {}
        for parent, ledger, total in rows:
            if parent not in result:
                result[parent] = []
            result[parent].append((ledger, total))
        return result

    income = get_pl_amounts(income_groups)
    expense = get_pl_amounts(expense_groups)

    # Calculate totals
    total_income = sum(abs(amt) for entries in income.values() for _, amt in entries)
    total_expense = sum(abs(amt) for entries in expense.values() for _, amt in entries)

    # Gross profit: Direct Income - Direct Expense (groups that affect gross profit)
    direct_income_groups = set(get_groups_by_nature(conn, 'direct_income'))
    direct_expense_groups = set(get_groups_by_nature(conn, 'direct_expense'))
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

def balance_sheet(conn, as_of_date=None, date_from=None, date_to=None):
    """Generate Balance Sheet.
    Returns dict with assets and liabilities grouped.

    Structure: {
        'assets': {group_name: [(ledger, balance), ...]},
        'liabilities': {group_name: [(ledger, balance), ...]},
        'total_assets': float,
        'total_liabilities': float,
    }
    """
    asset_groups = set(get_groups_by_nature(conn, 'asset'))
    liability_groups = set(get_groups_by_nature(conn, 'liability'))

    assets = get_ledger_totals_by_group(conn, asset_groups, as_of_date, date_from=date_from, date_to=date_to)
    liabilities = get_ledger_totals_by_group(conn, liability_groups, as_of_date, date_from=date_from, date_to=date_to)

    # Filter zero balances
    assets = {g: [(n, b) for n, b in entries if b != 0] for g, entries in assets.items()}
    liabilities = {g: [(n, b) for n, b in entries if b != 0] for g, entries in liabilities.items()}
    assets = {g: e for g, e in assets.items() if e}
    liabilities = {g: e for g, e in liabilities.items() if e}

    total_assets = sum(abs(bal) for entries in assets.values() for _, bal in entries)
    total_liabilities = sum(abs(bal) for entries in liabilities.values() for _, bal in entries)

    return {
        "assets": assets,
        "liabilities": liabilities,
        "total_assets": total_assets,
        "total_liabilities": total_liabilities,
    }


# ── LEDGER DETAIL (STATEMENT OF ACCOUNT) ────────────────────────────────────

def ledger_detail(conn, ledger_name, from_date=None, to_date=None,
                  voucher_types=None):
    """Get all transactions for a specific ledger with running balance.
    Returns: (opening_balance, transactions_list, closing_balance)
    Each transaction: dict with date, voucher_type, voucher_number, narration, party, debit, credit, balance

    Optional filters:
        voucher_types: list of voucher type names to include
    """
    lcols = _get_cols(conn, "mst_ledger")
    vcols = _get_cols(conn, "trn_voucher")
    has_ob = "OPENINGBALANCE" in lcols
    has_narration = "NARRATION" in vcols

    # Opening balance
    opening = 0.0
    if has_ob:
        try:
            ob = conn.execute(
                "SELECT CAST(OPENINGBALANCE AS REAL) FROM mst_ledger WHERE NAME = ?",
                (ledger_name,)
            ).fetchone()
            opening = (ob[0] or 0.0) if ob else 0.0
        except sqlite3.OperationalError:
            opening = 0.0

    # Transactions
    date_filter = ""
    params = [ledger_name]
    if from_date:
        date_filter += " AND v.DATE >= ?"
        params.append(from_date)
    if to_date:
        date_filter += " AND v.DATE <= ?"
        params.append(to_date)
    if voucher_types:
        vt_ph = ",".join(["?"] * len(voucher_types))
        date_filter += f" AND v.VOUCHERTYPENAME IN ({vt_ph})"
        params.extend(voucher_types)

    narration_col = "v.NARRATION" if has_narration else "'' AS NARRATION"
    sql = f"""
    SELECT v.DATE, v.VOUCHERTYPENAME, v.VOUCHERNUMBER, {narration_col},
           CAST(a.AMOUNT AS REAL), v.PARTYLEDGERNAME
    FROM trn_accounting a
    JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
    WHERE a.LEDGERNAME = ?{date_filter}
    ORDER BY v.DATE, v.VOUCHERNUMBER
    """
    try:
        rows = conn.execute(sql, params).fetchall()
    except sqlite3.OperationalError:
        rows = []

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

def pl_group_drilldown(conn, group_name, from_date=None, to_date=None,
                       voucher_types=None):
    """Drilldown into a P&L group: show all transactions under that group.
    Returns list of dicts with date, voucher, ledger, party, amount.

    Optional filters:
        voucher_types: list of voucher type names to include
    """
    all_groups = get_all_groups_under(conn, [group_name])
    if not all_groups:
        return []
    placeholders = ",".join(["?"] * len(all_groups))

    vcols = _get_cols(conn, "trn_voucher")
    narration_col = "v.NARRATION" if "NARRATION" in vcols else "'' AS NARRATION"

    date_filter = ""
    params = list(all_groups)
    if from_date:
        date_filter += " AND v.DATE >= ?"
        params.append(from_date)
    if to_date:
        date_filter += " AND v.DATE <= ?"
        params.append(to_date)
    if voucher_types:
        vt_ph = ",".join(["?"] * len(voucher_types))
        date_filter += f" AND v.VOUCHERTYPENAME IN ({vt_ph})"
        params.extend(voucher_types)

    sql = f"""
    SELECT v.DATE, v.VOUCHERTYPENAME, v.VOUCHERNUMBER, a.LEDGERNAME,
           v.PARTYLEDGERNAME, CAST(a.AMOUNT AS REAL), {narration_col}
    FROM trn_accounting a
    JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
    JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
    WHERE l.PARENT IN ({placeholders}){date_filter}
    ORDER BY v.DATE, v.VOUCHERNUMBER
    """
    try:
        rows = conn.execute(sql, params).fetchall()
    except sqlite3.OperationalError:
        return []

    return [{
        "date": r[0], "voucher_type": r[1], "voucher_number": r[2],
        "ledger": r[3], "party": r[4], "amount": r[5], "narration": r[6],
    } for r in rows]


# ── DEBTOR/CREDITOR AGING ────────────────────────────────────────────────────

def debtor_aging(conn, date_from=None, date_to=None):
    """Simple debtor aging based on closing balances from mst_ledger.
    Groups: Sundry Debtors (recursive — includes all sub-groups).
    When date_from/date_to provided, computes opening + transactions in range."""
    lcols = _get_cols(conn, "mst_ledger")
    has_ob = "OPENINGBALANCE" in lcols
    has_cb = "COMPUTED_CB" in lcols or "CLOSINGBALANCE" in lcols

    debtor_groups = get_groups_by_nature(conn, 'debtors')
    placeholders = ",".join(["?"] * len(debtor_groups))
    group_params = list(debtor_groups)

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
        sql = f"""
        SELECT l.NAME,
               COALESCE({ob_expr}, 0) +
               COALESCE((
                   SELECT SUM(CAST(a.AMOUNT AS REAL))
                   FROM trn_accounting a
                   JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                   WHERE a.LEDGERNAME = l.NAME{date_cond}
               ), 0) as balance
        FROM mst_ledger l
        WHERE l.PARENT IN ({placeholders})
        ORDER BY balance
        """
        try:
            rows = conn.execute(sql, params + group_params).fetchall()
        except sqlite3.OperationalError:
            rows = []
    elif has_cb:
        bc = _bal_col(conn)
        sql = f"""
        SELECT NAME, CAST({bc} AS REAL) as balance
        FROM mst_ledger
        WHERE PARENT IN ({placeholders})
          AND CAST({bc} AS REAL) != 0
        ORDER BY CAST({bc} AS REAL)
        """
        try:
            rows = conn.execute(sql, group_params).fetchall()
        except sqlite3.OperationalError:
            rows = []
    else:
        # No closing balance — compute from opening + all transactions
        ob_expr = "CAST(l.OPENINGBALANCE AS REAL)" if has_ob else "0"
        sql = f"""
        SELECT l.NAME,
               COALESCE({ob_expr}, 0) +
               COALESCE((
                   SELECT SUM(CAST(a.AMOUNT AS REAL))
                   FROM trn_accounting a
                   WHERE a.LEDGERNAME = l.NAME
               ), 0) as balance
        FROM mst_ledger l
        WHERE l.PARENT IN ({placeholders})
        ORDER BY balance
        """
        try:
            rows = conn.execute(sql, group_params).fetchall()
        except sqlite3.OperationalError:
            rows = []

    return [(name, abs(bal)) for name, bal in rows if bal and bal != 0]


def creditor_aging(conn, date_from=None, date_to=None):
    """Simple creditor listing based on closing balances.
    Recursive — includes all sub-groups under Sundry Creditors.
    When date_from/date_to provided, computes opening + transactions in range."""
    lcols = _get_cols(conn, "mst_ledger")
    has_ob = "OPENINGBALANCE" in lcols
    has_cb = "COMPUTED_CB" in lcols or "CLOSINGBALANCE" in lcols

    creditor_groups = get_groups_by_nature(conn, 'creditors')
    placeholders = ",".join(["?"] * len(creditor_groups))
    group_params = list(creditor_groups)

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
        sql = f"""
        SELECT l.NAME,
               COALESCE({ob_expr}, 0) +
               COALESCE((
                   SELECT SUM(CAST(a.AMOUNT AS REAL))
                   FROM trn_accounting a
                   JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                   WHERE a.LEDGERNAME = l.NAME{date_cond}
               ), 0) as balance
        FROM mst_ledger l
        WHERE l.PARENT IN ({placeholders})
        ORDER BY balance
        """
        try:
            rows = conn.execute(sql, params + group_params).fetchall()
        except sqlite3.OperationalError:
            rows = []
    elif has_cb:
        bc = _bal_col(conn)
        sql = f"""
        SELECT NAME, CAST({bc} AS REAL) as balance
        FROM mst_ledger
        WHERE PARENT IN ({placeholders})
          AND CAST({bc} AS REAL) != 0
        ORDER BY CAST({bc} AS REAL)
        """
        try:
            rows = conn.execute(sql, group_params).fetchall()
        except sqlite3.OperationalError:
            rows = []
    else:
        ob_expr = "CAST(l.OPENINGBALANCE AS REAL)" if has_ob else "0"
        sql = f"""
        SELECT l.NAME,
               COALESCE({ob_expr}, 0) +
               COALESCE((
                   SELECT SUM(CAST(a.AMOUNT AS REAL))
                   FROM trn_accounting a
                   WHERE a.LEDGERNAME = l.NAME
               ), 0) as balance
        FROM mst_ledger l
        WHERE l.PARENT IN ({placeholders})
        ORDER BY balance
        """
        try:
            rows = conn.execute(sql, group_params).fetchall()
        except sqlite3.OperationalError:
            rows = []

    return [(name, abs(bal)) for name, bal in rows if bal and bal != 0]


# ── VOUCHER TYPE SUMMARY ────────────────────────────────────────────────────

def voucher_summary(conn, from_date=None, to_date=None, voucher_types=None):
    """Summary of vouchers by type.

    Optional filters:
        voucher_types: list of voucher type names to include
    """
    date_filter = ""
    params = []
    if from_date:
        date_filter += " AND DATE >= ?"
        params.append(from_date)
    if to_date:
        date_filter += " AND DATE <= ?"
        params.append(to_date)
    if voucher_types:
        vt_ph = ",".join(["?"] * len(voucher_types))
        date_filter += f" AND VOUCHERTYPENAME IN ({vt_ph})"
        params.extend(voucher_types)

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
    try:
        return conn.execute(sql, params).fetchall()
    except sqlite3.OperationalError:
        return []


# ── STOCK / INVENTORY SUMMARY ─────────────────────────────────────────────────

def stock_summary(conn):
    """Return stock item summary if mst_stock_item table has data.
    Returns list of dicts or empty list if no stock data."""
    if not _table_exists(conn, "mst_stock_item"):
        return []

    sicols = _get_cols(conn, "mst_stock_item")
    has_cv = "CLOSINGVALUE" in sicols
    has_cb = "CLOSINGBALANCE" in sicols

    try:
        if has_cv:
            rows = conn.execute("""
                SELECT NAME, PARENT,
                       CAST(CLOSINGBALANCE AS REAL) as closing_qty,
                       CAST(CLOSINGVALUE AS REAL) as closing_val
                FROM mst_stock_item
                WHERE CAST(CLOSINGVALUE AS REAL) != 0
                ORDER BY ABS(CAST(CLOSINGVALUE AS REAL)) DESC
                LIMIT 50
            """).fetchall()
            return [{"name": r[0], "group": r[1],
                     "closing_qty": r[2] or 0, "closing_value": r[3] or 0}
                    for r in rows]
        elif has_cb:
            rows = conn.execute("""
                SELECT NAME, PARENT, CAST(CLOSINGBALANCE AS REAL) as closing_qty
                FROM mst_stock_item
                WHERE CAST(CLOSINGBALANCE AS REAL) != 0
                ORDER BY ABS(CAST(CLOSINGBALANCE AS REAL)) DESC
                LIMIT 50
            """).fetchall()
            return [{"name": r[0], "group": r[1],
                     "closing_qty": r[2] or 0, "closing_value": 0}
                    for r in rows]
        else:
            rows = conn.execute("""
                SELECT NAME, PARENT
                FROM mst_stock_item
                ORDER BY NAME
                LIMIT 50
            """).fetchall()
            return [{"name": r[0], "group": r[1],
                     "closing_qty": 0, "closing_value": 0}
                    for r in rows]
    except sqlite3.OperationalError:
        return []


def godown_summary(conn):
    """Return godown summary if mst_godown table has data.
    Returns list of dicts or empty list."""
    if not _table_exists(conn, "mst_godown"):
        return []
    try:
        rows = conn.execute("""
            SELECT NAME, PARENT FROM mst_godown
            WHERE NAME IS NOT NULL AND NAME != ''
            ORDER BY NAME
        """).fetchall()
        return [{"name": r[0], "parent": r[1]} for r in rows]
    except sqlite3.OperationalError:
        return []


# ── SEARCH ───────────────────────────────────────────────────────────────────

def search_ledger(conn, query):
    """Search ledgers by name (fuzzy)."""
    lcols = _get_cols(conn, "mst_ledger")
    has_cb = "COMPUTED_CB" in lcols or "CLOSINGBALANCE" in lcols or "COMPUTED_CB" in lcols
    bc = _bal_col(conn)
    balance_col = f"CAST({bc} AS REAL)" if has_cb else "0"
    try:
        sql = f"""
        SELECT NAME, PARENT, {balance_col} as balance
        FROM mst_ledger
        WHERE NAME LIKE ?
        ORDER BY NAME
        LIMIT 20
        """
        return conn.execute(sql, [f"%{query}%"]).fetchall()
    except sqlite3.OperationalError:
        return []


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
