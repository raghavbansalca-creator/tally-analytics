"""
Seven Labs Vision -- Companies Act Schedule III Financial Statement Exporter
Generates Balance Sheet (Part I) and P&L Statement (Part II) in Excel format
with professional formatting compliant with Companies Act, 2013.
"""

import sqlite3
import os
from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill, numbers
from openpyxl.utils import get_column_letter

DB_PATH = os.path.join(os.path.dirname(__file__), "tally_data.db")


# ── TALLY GROUP MAPPING TO SCHEDULE III ─────────────────────────────────────

# Balance Sheet — Equity & Liabilities side
SCHEDULE_III_LIABILITIES = {
    "shareholders_funds": {
        "share_capital": {
            "label": "Share Capital",
            "note": "1",
            "groups": ["Capital Account", "Share Capital Account",
                       "Non Residents Shareholders"],
        },
        "reserves_surplus": {
            "label": "Reserves and Surplus",
            "note": "2",
            "groups": ["Reserves & Surplus"],
        },
    },
    "non_current_liabilities": {
        "long_term_borrowings": {
            "label": "Long-term borrowings",
            "note": "3",
            "groups": ["Secured Loans", "Unsecured Loans"],
        },
        "deferred_tax_liabilities": {
            "label": "Deferred tax liabilities (Net)",
            "note": "4",
            "groups": [],  # matched by ledger name containing 'deferred tax'
            "match_names": ["deferred tax"],
        },
        "other_long_term_liabilities": {
            "label": "Other Long term liabilities",
            "note": "",
            "groups": [],
        },
        "long_term_provisions": {
            "label": "Long term provisions",
            "note": "",
            "groups": [],
        },
    },
    "current_liabilities": {
        "short_term_borrowings": {
            "label": "Short-term borrowings",
            "note": "5",
            "groups": ["Bank OD A/c"],
        },
        "trade_payables": {
            "label": "Trade payables",
            "note": "6",
            "groups": ["Sundry Creditors"],
        },
        "other_current_liabilities": {
            "label": "Other current liabilities",
            "note": "7",
            # Only direct ledgers under these parents (non-recursive)
            # to avoid double-counting with trade_payables and provisions
            "_direct_groups": ["Current Liabilities"],
            "groups": ["Duties & Taxes", "Salary Payable"],
        },
        "short_term_provisions": {
            "label": "Short-term provisions",
            "note": "8",
            "groups": ["Provisions"],
        },
    },
}

# Balance Sheet — Assets side
SCHEDULE_III_ASSETS = {
    "non_current_assets": {
        "ppe": {
            "label": "Property, Plant and Equipment",
            "note": "9",
            "groups": ["Fixed Assets"],
        },
        "non_current_investments": {
            "label": "Non-current investments",
            "note": "",
            "groups": ["Investments"],
        },
        "deferred_tax_assets": {
            "label": "Deferred tax assets (net)",
            "note": "4",
            "groups": [],
            "match_names": ["deferred tax"],
        },
        "long_term_loans_advances": {
            "label": "Long term loans and advances",
            "note": "",
            "groups": [],
        },
        "other_non_current_assets": {
            "label": "Other non-current assets",
            "note": "",
            "groups": ["Misc. Expenses (ASSET)"],
        },
    },
    "current_assets": {
        "current_investments": {
            "label": "Current investments",
            "note": "",
            "groups": [],
        },
        "inventories": {
            "label": "Inventories",
            "note": "10",
            "groups": ["Stock-in-Hand"],
        },
        "trade_receivables": {
            "label": "Trade receivables",
            "note": "11",
            "groups": ["Sundry Debtors"],
        },
        "cash_equivalents": {
            "label": "Cash and cash equivalents",
            "note": "12",
            "groups": ["Cash-in-Hand", "Bank Accounts"],
        },
        "short_term_loans_advances": {
            "label": "Short-term loans and advances",
            "note": "",
            "groups": ["Loans & Advances (Asset)", "Loan to Employees", "Deposits (Asset)"],
        },
        "other_current_assets": {
            "label": "Other current assets",
            "note": "13",
            "groups": [],
            "_direct_groups": ["Current Assets"],  # Only DIRECT ledgers, not sub-groups
        },
    },
}

# P&L Statement mapping
SCHEDULE_III_PL = {
    "revenue_from_operations": {
        "label": "Revenue from operations",
        "note": "14",
        "groups": ["Sales Accounts"],
    },
    "other_income": {
        "label": "Other Income",
        "note": "15",
        "groups": ["Direct Incomes", "Indirect Incomes"],
    },
    "expenses": {
        "cost_of_materials": {
            "label": "Cost of materials consumed",
            "note": "16",
            "groups": ["Purchase Accounts"],
        },
        "changes_in_inventories": {
            "label": "Changes in inventories of finished goods, WIP and Stock-in-Trade",
            "note": "17",
            "groups": [],  # computed from opening - closing stock
        },
        "employee_benefit": {
            "label": "Employee benefit expense",
            "note": "18",
            "groups": ["Salary Expenses"],
        },
        "finance_costs": {
            "label": "Finance costs",
            "note": "19",
            "groups": [],
            "match_names": ["interest", "bank charges", "finance"],
        },
        "depreciation": {
            "label": "Depreciation and amortization expense",
            "note": "9",
            "groups": [],
            "match_names": ["depreciation", "amortization"],
        },
        "other_expenses": {
            "label": "Other expenses",
            "note": "20",
            "groups": ["Direct Expenses", "Indirect Expenses", "Online Expenses",
                       "Rent Expenses", "Job Work-Non TDS"],
        },
    },
}


# ── DATABASE HELPERS ────────────────────────────────────────────────────────

def _get_conn(db_path=None):
    return sqlite3.connect(db_path or DB_PATH)


def _get_all_groups_under(conn, root_groups):
    """Recursively get all group names under root groups."""
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


def _get_direct_ledger_balances(conn, group_names, as_of_date=None):
    """Get sum of closing balances for ledgers DIRECTLY under given groups
    (non-recursive, no sub-group expansion)."""
    if not group_names:
        return 0.0

    placeholders = ",".join(["?"] * len(group_names))

    if as_of_date:
        sql = f"""
        SELECT COALESCE(SUM(
            COALESCE(CAST(l.OPENINGBALANCE AS REAL), 0) +
            COALESCE((
                SELECT SUM(CAST(a.AMOUNT AS REAL))
                FROM trn_accounting a
                JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                WHERE a.LEDGERNAME = l.NAME AND v.DATE <= ?
            ), 0)
        ), 0)
        FROM mst_ledger l
        WHERE l.PARENT IN ({placeholders})
        """
        row = conn.execute(sql, [as_of_date] + list(group_names)).fetchone()
    else:
        sql = f"""
        SELECT COALESCE(SUM(CAST(CLOSINGBALANCE AS REAL)), 0)
        FROM mst_ledger
        WHERE PARENT IN ({placeholders})
        """
        row = conn.execute(sql, list(group_names)).fetchone()

    return row[0] if row else 0.0


def _get_ledger_balances(conn, group_names, as_of_date=None):
    """Get sum of closing balances for ledgers under given groups."""
    if not group_names:
        return 0.0

    all_groups = set()
    for g in group_names:
        all_groups |= _get_all_groups_under(conn, [g])

    if not all_groups:
        return 0.0

    placeholders = ",".join(["?"] * len(all_groups))

    if as_of_date:
        sql = f"""
        SELECT COALESCE(SUM(
            COALESCE(CAST(l.OPENINGBALANCE AS REAL), 0) +
            COALESCE((
                SELECT SUM(CAST(a.AMOUNT AS REAL))
                FROM trn_accounting a
                JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                WHERE a.LEDGERNAME = l.NAME AND v.DATE <= ?
            ), 0)
        ), 0)
        FROM mst_ledger l
        WHERE l.PARENT IN ({placeholders})
        """
        row = conn.execute(sql, [as_of_date] + list(all_groups)).fetchone()
    else:
        sql = f"""
        SELECT COALESCE(SUM(CAST(CLOSINGBALANCE AS REAL)), 0)
        FROM mst_ledger
        WHERE PARENT IN ({placeholders})
        """
        row = conn.execute(sql, list(all_groups)).fetchone()

    return row[0] if row else 0.0


def _get_ledger_balances_by_name(conn, match_names, as_of_date=None):
    """Get sum of closing balances for ledgers matching name patterns."""
    if not match_names:
        return 0.0

    conditions = " OR ".join(["LOWER(NAME) LIKE ?" for _ in match_names])
    params = [f"%{n.lower()}%" for n in match_names]

    if as_of_date:
        sql = f"""
        SELECT COALESCE(SUM(
            COALESCE(CAST(l.OPENINGBALANCE AS REAL), 0) +
            COALESCE((
                SELECT SUM(CAST(a.AMOUNT AS REAL))
                FROM trn_accounting a
                JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                WHERE a.LEDGERNAME = l.NAME AND v.DATE <= ?
            ), 0)
        ), 0)
        FROM mst_ledger l
        WHERE ({conditions})
        """
        row = conn.execute(sql, [as_of_date] + params).fetchone()
    else:
        sql = f"""
        SELECT COALESCE(SUM(CAST(CLOSINGBALANCE AS REAL)), 0)
        FROM mst_ledger
        WHERE ({conditions})
        """
        row = conn.execute(sql, params).fetchone()

    return row[0] if row else 0.0


def _get_pl_amount(conn, group_names, from_date=None, to_date=None):
    """Get P&L amount for given groups (sum of transaction amounts)."""
    if not group_names:
        return 0.0

    all_groups = set()
    for g in group_names:
        all_groups |= _get_all_groups_under(conn, [g])

    if not all_groups:
        return 0.0

    placeholders = ",".join(["?"] * len(all_groups))

    if from_date and to_date:
        sql = f"""
        SELECT COALESCE(SUM(CAST(a.AMOUNT AS REAL)), 0)
        FROM trn_accounting a
        JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT IN ({placeholders})
          AND v.DATE >= ? AND v.DATE <= ?
        """
        row = conn.execute(sql, list(all_groups) + [from_date, to_date]).fetchone()
    else:
        sql = f"""
        SELECT COALESCE(SUM(CAST(a.AMOUNT AS REAL)), 0)
        FROM trn_accounting a
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE l.PARENT IN ({placeholders})
        """
        row = conn.execute(sql, list(all_groups)).fetchone()

    return row[0] if row else 0.0


def _get_pl_amount_by_name(conn, match_names, from_date=None, to_date=None):
    """Get P&L amount for ledgers matching name patterns."""
    if not match_names:
        return 0.0

    conditions = " OR ".join(["LOWER(l.NAME) LIKE ?" for _ in match_names])
    params = [f"%{n.lower()}%" for n in match_names]

    if from_date and to_date:
        sql = f"""
        SELECT COALESCE(SUM(CAST(a.AMOUNT AS REAL)), 0)
        FROM trn_accounting a
        JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE ({conditions})
          AND v.DATE >= ? AND v.DATE <= ?
        """
        row = conn.execute(sql, params + [from_date, to_date]).fetchone()
    else:
        sql = f"""
        SELECT COALESCE(SUM(CAST(a.AMOUNT AS REAL)), 0)
        FROM trn_accounting a
        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
        WHERE ({conditions})
        """
        row = conn.execute(sql, params).fetchone()

    return row[0] if row else 0.0


def _get_metadata(conn):
    """Fetch company metadata from _metadata table."""
    try:
        rows = conn.execute("SELECT * FROM _metadata").fetchall()
        return {r[0]: r[1] for r in rows}
    except Exception:
        return {}


def _get_opening_stock(conn):
    """Get opening stock value from stock items."""
    try:
        row = conn.execute("""
            SELECT COALESCE(SUM(CAST(OPENINGBALANCE AS REAL)), 0)
            FROM mst_stock_item
        """).fetchone()
        return abs(row[0]) if row and row[0] else 0.0
    except Exception:
        return 0.0


def _get_closing_stock(conn):
    """Get closing stock value from stock items."""
    try:
        row = conn.execute("""
            SELECT COALESCE(SUM(CAST(CLOSINGVALUE AS REAL)), 0)
            FROM mst_stock_item
        """).fetchone()
        return abs(row[0]) if row and row[0] else 0.0
    except Exception:
        return 0.0


# ── DATA EXTRACTION ─────────────────────────────────────────────────────────

def extract_balance_sheet_data(conn, as_of_date=None):
    """Extract all Balance Sheet line items mapped to Schedule III."""

    def get_amount(item_config):
        amt = 0.0
        if item_config.get("groups"):
            amt += _get_ledger_balances(conn, item_config["groups"], as_of_date)
        if item_config.get("match_names"):
            amt += _get_ledger_balances_by_name(conn, item_config["match_names"], as_of_date)
        return amt

    # Tally sign conventions for closing balance:
    #   Liability/Equity groups: positive = credit = genuine liability
    #   Asset groups: negative = debit = genuine asset
    #
    # For individual ledgers within a group, signs can be mixed.
    # We compute NET per Schedule III line item using sum of individual ledger
    # absolute balances (debit balances counted as assets, credit as liabilities).
    # This matches how Tally displays Balance Sheet totals.

    def get_ledger_details(group_names, as_of_date_param=None):
        """Get individual ledger balances for given groups."""
        if not group_names:
            return []
        all_grps = set()
        for g in group_names:
            all_grps |= _get_all_groups_under(conn, [g])
        if not all_grps:
            return []
        ph = ",".join(["?"] * len(all_grps))
        if as_of_date_param:
            sql = f"""
            SELECT l.NAME, l.PARENT,
                   COALESCE(CAST(l.OPENINGBALANCE AS REAL), 0) +
                   COALESCE((
                       SELECT SUM(CAST(a.AMOUNT AS REAL))
                       FROM trn_accounting a
                       JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                       WHERE a.LEDGERNAME = l.NAME AND v.DATE <= ?
                   ), 0) as balance
            FROM mst_ledger l
            WHERE l.PARENT IN ({ph})
            """
            return conn.execute(sql, [as_of_date_param] + list(all_grps)).fetchall()
        else:
            sql = f"""
            SELECT NAME, PARENT, CAST(CLOSINGBALANCE AS REAL) as balance
            FROM mst_ledger
            WHERE PARENT IN ({ph})
            """
            return conn.execute(sql, list(all_grps)).fetchall()

    def get_net_amount(item_config):
        """Get NET sum of ledger balances (preserving Tally signs).
        Tally convention: positive = credit, negative = debit."""
        total = 0.0
        if item_config.get("groups"):
            total += _get_ledger_balances(conn, item_config["groups"], as_of_date)
        if item_config.get("_direct_groups"):
            # Only ledgers directly under these groups, no recursive sub-group expansion
            total += _get_direct_ledger_balances(
                conn, item_config["_direct_groups"], as_of_date)
        if item_config.get("match_names"):
            total += _get_ledger_balances_by_name(
                conn, item_config["match_names"], as_of_date)
        return total

    # For Schedule III, we use NET amounts per line item.
    # Tally sign convention:
    #   Liability/Equity groups: positive (credit) = genuine liability
    #   Asset groups: negative (debit) = genuine asset
    #
    # For display, we show positive numbers on both sides:
    #   Liability side: amount = raw (positive = liability to show)
    #   Asset side: amount = -raw (flip sign: debit negative -> positive asset)
    #
    # If a line item nets to the "wrong" side (e.g., debtors net credit),
    # it will show as a negative number, which is correct per Schedule III
    # (indicates it should technically be reclassified to the other side).

    # Extract liability items
    liabilities = {}
    for section_key, items in SCHEDULE_III_LIABILITIES.items():
        section_data = {}
        for item_key, config in items.items():
            raw = get_net_amount(config)
            # Positive raw = credit = liability amount for display
            section_data[item_key] = {
                "label": config["label"],
                "note": config.get("note", ""),
                "amount": raw,
            }
        liabilities[section_key] = section_data

    # Extract asset items
    assets = {}
    for section_key, items in SCHEDULE_III_ASSETS.items():
        section_data = {}
        for item_key, config in items.items():
            raw = get_net_amount(config)
            # Flip sign: negative (debit) -> positive asset for display
            section_data[item_key] = {
                "label": config["label"],
                "note": config.get("note", ""),
                "amount": -raw,
            }
        assets[section_key] = section_data

    # Add P&L balance (Profit for the year) to Reserves & Surplus
    pl_balance = conn.execute(
        "SELECT CAST(CLOSINGBALANCE AS REAL) FROM mst_ledger WHERE NAME = 'Profit & Loss A/c'"
    ).fetchone()
    if pl_balance and pl_balance[0]:
        # P&L positive (credit) = profit -> add to reserves
        liabilities["shareholders_funds"]["reserves_surplus"]["amount"] += pl_balance[0]

    # Handle Deferred Tax: show on only ONE side based on net position.
    # Also subtract from other_current_liabilities if it was picked up there via _direct_groups.
    dt_raw = _get_ledger_balances_by_name(conn, ["deferred tax"], as_of_date)
    # Remove deferred tax from other_current_liabilities (it may be under Current Liabilities directly)
    liabilities["current_liabilities"]["other_current_liabilities"]["amount"] -= dt_raw
    if dt_raw > 0:
        # Net credit = liability
        liabilities["non_current_liabilities"]["deferred_tax_liabilities"]["amount"] = dt_raw
        assets["non_current_assets"]["deferred_tax_assets"]["amount"] = 0.0
    elif dt_raw < 0:
        # Net debit = asset
        liabilities["non_current_liabilities"]["deferred_tax_liabilities"]["amount"] = 0.0
        assets["non_current_assets"]["deferred_tax_assets"]["amount"] = abs(dt_raw)
    else:
        liabilities["non_current_liabilities"]["deferred_tax_liabilities"]["amount"] = 0.0
        assets["non_current_assets"]["deferred_tax_assets"]["amount"] = 0.0

    # Note: Suspense A/c is under BS_LIABILITY_ROOTS ("Suspense A/c" group)
    # It's NOT in our Schedule III mapping, so we need to capture it.
    # If debit balance -> add to other current assets, if credit -> other current liabilities
    suspense_raw = _get_ledger_balances(conn, ["Suspense A/c"], as_of_date)
    if suspense_raw != 0:
        if suspense_raw < 0:  # debit = asset
            assets["current_assets"]["other_current_assets"]["amount"] += abs(suspense_raw)
        else:  # credit = liability
            liabilities["current_liabilities"]["other_current_liabilities"]["amount"] += suspense_raw

    total_liabilities = sum(
        item["amount"] for section in liabilities.values() for item in section.values()
    )
    total_assets = sum(
        item["amount"] for section in assets.values() for item in section.values()
    )

    return {
        "liabilities": liabilities,
        "assets": assets,
        "total_liabilities": total_liabilities,
        "total_assets": total_assets,
    }


def extract_pl_data(conn, from_date=None, to_date=None):
    """Extract all P&L Statement line items mapped to Schedule III."""

    def get_pl(config):
        amt = 0.0
        if config.get("groups"):
            amt += _get_pl_amount(conn, config["groups"], from_date, to_date)
        if config.get("match_names"):
            amt += _get_pl_amount_by_name(conn, config["match_names"], from_date, to_date)
        return amt

    # Revenue
    revenue_raw = get_pl(SCHEDULE_III_PL["revenue_from_operations"])
    revenue = abs(revenue_raw)  # Income is positive (credit in Tally)

    other_income_raw = get_pl(SCHEDULE_III_PL["other_income"])
    other_income = abs(other_income_raw)

    total_income = revenue + other_income

    # Expenses
    expenses = {}
    expense_configs = SCHEDULE_III_PL["expenses"]

    for key, config in expense_configs.items():
        if key == "changes_in_inventories":
            # Opening stock - Closing stock
            opening = _get_opening_stock(conn)
            closing = _get_closing_stock(conn)
            amt = opening - closing  # Positive = increase in expenses
            expenses[key] = {
                "label": config["label"],
                "note": config.get("note", ""),
                "amount": amt,
            }
        elif key == "other_expenses":
            # Other expenses: get all, then subtract items already captured
            raw = abs(get_pl(config))
            # Subtract employee, finance, depreciation amounts already captured
            # to avoid double-counting (since these groups may overlap)
            expenses[key] = {
                "label": config["label"],
                "note": config.get("note", ""),
                "amount": raw,
            }
        else:
            raw = get_pl(config)
            expenses[key] = {
                "label": config["label"],
                "note": config.get("note", ""),
                "amount": abs(raw),
            }

    # Remove finance costs and depreciation from other_expenses if they got double-counted
    # by checking if their match_names groups overlap with other_expenses groups
    finance_amt = expenses.get("finance_costs", {}).get("amount", 0)
    depreciation_amt = expenses.get("depreciation", {}).get("amount", 0)

    total_expenses = sum(item["amount"] for item in expenses.values())
    profit_before_tax = total_income - total_expenses

    return {
        "revenue": revenue,
        "other_income": other_income,
        "total_income": total_income,
        "expenses": expenses,
        "total_expenses": total_expenses,
        "profit_before_tax": profit_before_tax,
        "tax_current": 0.0,  # Would need specific tax ledger mapping
        "tax_deferred": 0.0,
        "profit_after_tax": profit_before_tax,  # Before tax adjustments
    }


# ── EXCEL FORMATTING HELPERS ───────────────────────────────────────────────

# Fonts
FONT_COMPANY = Font(name="Times New Roman", size=14, bold=True)
FONT_SUBTITLE = Font(name="Times New Roman", size=11, bold=True)
FONT_NORMAL_BOLD = Font(name="Times New Roman", size=10, bold=True)
FONT_NORMAL = Font(name="Times New Roman", size=10)
FONT_SECTION = Font(name="Times New Roman", size=10, bold=True)
FONT_HEADER = Font(name="Times New Roman", size=10, bold=True)
FONT_NOTE = Font(name="Times New Roman", size=9)
FONT_SIGNATORY = Font(name="Times New Roman", size=9)
FONT_SIGNATORY_BOLD = Font(name="Times New Roman", size=9, bold=True)

# Alignments
ALIGN_CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)
ALIGN_LEFT = Alignment(horizontal="left", vertical="center")
ALIGN_LEFT_INDENT1 = Alignment(horizontal="left", vertical="center", indent=2)
ALIGN_LEFT_INDENT2 = Alignment(horizontal="left", vertical="center", indent=4)
ALIGN_RIGHT = Alignment(horizontal="right", vertical="center")
ALIGN_NOTE_CENTER = Alignment(horizontal="center", vertical="center")

# Borders
THIN_BORDER = Side(style="thin")
HAIR_BORDER = Side(style="hair")
BOTTOM_BORDER = Border(bottom=THIN_BORDER)
TOP_BOTTOM_BORDER = Border(top=THIN_BORDER, bottom=THIN_BORDER)
DOUBLE_BOTTOM_BORDER = Border(bottom=Side(style="double"))
HEADER_BORDER = Border(bottom=Side(style="medium"))

# Fill
HEADER_FILL = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")


def _format_amount(amount):
    """Format amount: comma-separated, negative in brackets."""
    if amount is None or amount == 0:
        return "-"
    if amount < 0:
        return f"({abs(amount):,.2f})"
    return f"{amount:,.2f}"


def _write_company_header(ws, company_info, row_start, title, max_col=5):
    """Write the company header block at top of sheet."""
    row = row_start

    # Company Name
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=max_col)
    cell = ws.cell(row=row, column=1, value=company_info.get("name", ""))
    cell.font = FONT_COMPANY
    cell.alignment = ALIGN_CENTER
    row += 1

    # CIN
    cin = company_info.get("cin", "")
    if cin:
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=max_col)
        cell = ws.cell(row=row, column=1, value=f"CIN : {cin}")
        cell.font = FONT_NORMAL
        cell.alignment = ALIGN_CENTER
        row += 1

    # Address
    address = company_info.get("address", "")
    if address:
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=max_col)
        cell = ws.cell(row=row, column=1, value=f"Regd Office: {address}")
        cell.font = FONT_NORMAL
        cell.alignment = ALIGN_CENTER
        row += 1

    # Title
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=max_col)
    cell = ws.cell(row=row, column=1, value=title)
    cell.font = FONT_SUBTITLE
    cell.alignment = ALIGN_CENTER
    row += 1

    # Amount denomination
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=max_col)
    cell = ws.cell(row=row, column=1, value="Amount (in INR)")
    cell.font = Font(name="Times New Roman", size=9, italic=True)
    cell.alignment = Alignment(horizontal="right")
    row += 1

    return row


def _write_column_headers(ws, row, headers):
    """Write column headers with formatting."""
    for col_idx, (header, width) in enumerate(headers, 1):
        cell = ws.cell(row=row, column=col_idx, value=header)
        cell.font = FONT_HEADER
        cell.alignment = ALIGN_CENTER if col_idx > 1 else ALIGN_LEFT
        cell.border = HEADER_BORDER
        cell.fill = HEADER_FILL
        ws.column_dimensions[get_column_letter(col_idx)].width = width
    return row + 1


def _write_amount_row(ws, row, label, note, amount, prev_amount=None,
                      font=None, alignment=None, is_total=False):
    """Write a single line item row."""
    f = font or FONT_NORMAL
    a = alignment or ALIGN_LEFT_INDENT2

    cell = ws.cell(row=row, column=1, value=label)
    cell.font = f
    cell.alignment = a

    # Note column
    if note:
        cell = ws.cell(row=row, column=2, value=note)
        cell.font = FONT_NOTE
        cell.alignment = ALIGN_NOTE_CENTER

    # Current year amount
    cell = ws.cell(row=row, column=3, value=amount if amount else None)
    cell.font = f
    cell.alignment = ALIGN_RIGHT
    cell.number_format = '#,##0.00;(#,##0.00);"-"'

    # Previous year amount
    if prev_amount is not None:
        cell = ws.cell(row=row, column=4, value=prev_amount if prev_amount else None)
        cell.font = f
        cell.alignment = ALIGN_RIGHT
        cell.number_format = '#,##0.00;(#,##0.00);"-"'

    if is_total:
        for col in range(1, 5):
            ws.cell(row=row, column=col).border = TOP_BOTTOM_BORDER
            ws.cell(row=row, column=col).font = FONT_NORMAL_BOLD

    return row + 1


def _write_signatory_block(ws, row, company_info, max_col=4):
    """Write the signatory block at the bottom."""
    row += 2

    # "As per our report of even date"
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=2)
    cell = ws.cell(row=row, column=1, value="As per our report of even date")
    cell.font = FONT_SIGNATORY_BOLD
    row += 1

    # Auditor info (left) and Director info (right)
    auditor_name = company_info.get("auditor_name", "")
    auditor_frn = company_info.get("auditor_frn", "")
    director1 = company_info.get("director1_name", "")
    director1_din = company_info.get("director1_din", "")
    director2 = company_info.get("director2_name", "")
    director2_din = company_info.get("director2_din", "")

    if auditor_name:
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=2)
        cell = ws.cell(row=row, column=1, value=f"For {auditor_name}")
        cell.font = FONT_SIGNATORY_BOLD
        row += 1

        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=2)
        cell = ws.cell(row=row, column=1, value="Chartered Accountants")
        cell.font = FONT_SIGNATORY
        row += 1

        if auditor_frn:
            ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=2)
            cell = ws.cell(row=row, column=1, value=f"FRN: {auditor_frn}")
            cell.font = FONT_SIGNATORY
            row += 1

    row += 1

    # Director signatures on right side
    if director1:
        cell = ws.cell(row=row, column=3, value=director1)
        cell.font = FONT_SIGNATORY_BOLD
        cell.alignment = ALIGN_RIGHT
        if director1_din:
            cell = ws.cell(row=row, column=4, value=f"DIN: {director1_din}")
            cell.font = FONT_SIGNATORY
            cell.alignment = ALIGN_RIGHT
        row += 1

    if director2:
        cell = ws.cell(row=row, column=3, value=director2)
        cell.font = FONT_SIGNATORY_BOLD
        cell.alignment = ALIGN_RIGHT
        if director2_din:
            cell = ws.cell(row=row, column=4, value=f"DIN: {director2_din}")
            cell.font = FONT_SIGNATORY
            cell.alignment = ALIGN_RIGHT
        row += 1

    # Place and Date
    row += 1
    ws.cell(row=row, column=1, value="Place:").font = FONT_SIGNATORY
    ws.cell(row=row, column=3, value="Date:").font = FONT_SIGNATORY

    return row


# ── EXCEL SHEET BUILDERS ───────────────────────────────────────────────────

def _build_balance_sheet(wb, bs_data, company_info, prev_bs_data=None):
    """Build the Balance Sheet worksheet."""
    ws = wb.active
    ws.title = "Balance Sheet"
    ws.sheet_properties.pageSetUpPr.fitToPage = True

    year_end = company_info.get("year_end", "31.03.2026")

    # Company header
    row = _write_company_header(
        ws, company_info, 1,
        f"Balance Sheet as at {year_end}",
        max_col=4
    )

    # Column headers
    headers = [
        ("Particulars", 55),
        ("Note", 8),
        (f"As at {year_end}", 20),
        (f"As at {company_info.get('prev_year_end', '31.03.2025')}", 20),
    ]
    row = _write_column_headers(ws, row, headers)

    # ── I. EQUITY AND LIABILITIES ──
    row += 1
    cell = ws.cell(row=row, column=1, value="I. EQUITY AND LIABILITIES")
    cell.font = FONT_SECTION
    cell.alignment = ALIGN_LEFT
    row += 1

    liabilities = bs_data["liabilities"]
    prev_liab = prev_bs_data["liabilities"] if prev_bs_data else None

    # (1) Shareholder's Funds
    cell = ws.cell(row=row, column=1, value="(1) Shareholder's Funds")
    cell.font = FONT_NORMAL_BOLD
    cell.alignment = ALIGN_LEFT_INDENT1
    row += 1

    sf = liabilities["shareholders_funds"]
    sf_prev = prev_liab["shareholders_funds"] if prev_liab else None
    sf_total = 0
    sf_prev_total = 0

    for key in ["share_capital", "reserves_surplus"]:
        item = sf[key]
        prev_amt = sf_prev[key]["amount"] if sf_prev else None
        letter = chr(ord('a') + list(sf.keys()).index(key))
        row = _write_amount_row(ws, row,
                                f"({letter}) {item['label']}", item["note"],
                                item["amount"], prev_amt)
        sf_total += item["amount"]
        if prev_amt:
            sf_prev_total += prev_amt

    # (2) Share application money pending
    cell = ws.cell(row=row, column=1, value="(2) Share application money pending allotment")
    cell.font = FONT_NORMAL_BOLD
    cell.alignment = ALIGN_LEFT_INDENT1
    row = _write_amount_row(ws, row, "", "", 0, 0)

    # (3) Non-Current Liabilities
    cell = ws.cell(row=row, column=1, value="(3) Non-Current Liabilities")
    cell.font = FONT_NORMAL_BOLD
    cell.alignment = ALIGN_LEFT_INDENT1
    row += 1

    ncl = liabilities["non_current_liabilities"]
    ncl_prev = prev_liab["non_current_liabilities"] if prev_liab else None
    ncl_total = 0

    for idx, (key, item) in enumerate(ncl.items()):
        prev_amt = ncl_prev[key]["amount"] if ncl_prev else None
        letter = chr(ord('a') + idx)
        row = _write_amount_row(ws, row,
                                f"({letter}) {item['label']}", item["note"],
                                item["amount"], prev_amt)
        ncl_total += item["amount"]

    # (4) Current Liabilities
    cell = ws.cell(row=row, column=1, value="(4) Current Liabilities")
    cell.font = FONT_NORMAL_BOLD
    cell.alignment = ALIGN_LEFT_INDENT1
    row += 1

    cl = liabilities["current_liabilities"]
    cl_prev = prev_liab["current_liabilities"] if prev_liab else None
    cl_total = 0

    for idx, (key, item) in enumerate(cl.items()):
        prev_amt = cl_prev[key]["amount"] if cl_prev else None
        letter = chr(ord('a') + idx)
        row = _write_amount_row(ws, row,
                                f"({letter}) {item['label']}", item["note"],
                                item["amount"], prev_amt)
        cl_total += item["amount"]

    # Total Equity & Liabilities
    total_liab = bs_data["total_liabilities"]
    prev_total_liab = prev_bs_data["total_liabilities"] if prev_bs_data else None
    row = _write_amount_row(ws, row, "Total", "", total_liab, prev_total_liab, is_total=True)

    # ── II. ASSETS ──
    row += 1
    cell = ws.cell(row=row, column=1, value="II. ASSETS")
    cell.font = FONT_SECTION
    cell.alignment = ALIGN_LEFT
    row += 1

    assets = bs_data["assets"]
    prev_assets = prev_bs_data["assets"] if prev_bs_data else None

    # (1) Non-current assets
    cell = ws.cell(row=row, column=1, value="(1) Non-current assets")
    cell.font = FONT_NORMAL_BOLD
    cell.alignment = ALIGN_LEFT_INDENT1
    row += 1

    nca = assets["non_current_assets"]
    nca_prev = prev_assets["non_current_assets"] if prev_assets else None

    for idx, (key, item) in enumerate(nca.items()):
        prev_amt = nca_prev[key]["amount"] if nca_prev else None
        letter = chr(ord('a') + idx)
        row = _write_amount_row(ws, row,
                                f"({letter}) {item['label']}", item["note"],
                                item["amount"], prev_amt)

    # (2) Current assets
    cell = ws.cell(row=row, column=1, value="(2) Current assets")
    cell.font = FONT_NORMAL_BOLD
    cell.alignment = ALIGN_LEFT_INDENT1
    row += 1

    ca = assets["current_assets"]
    ca_prev = prev_assets["current_assets"] if prev_assets else None

    for idx, (key, item) in enumerate(ca.items()):
        prev_amt = ca_prev[key]["amount"] if ca_prev else None
        letter = chr(ord('a') + idx)
        row = _write_amount_row(ws, row,
                                f"({letter}) {item['label']}", item["note"],
                                item["amount"], prev_amt)

    # Total Assets
    total_assets = bs_data["total_assets"]
    prev_total_assets = prev_bs_data["total_assets"] if prev_bs_data else None
    row = _write_amount_row(ws, row, "Total", "", total_assets, prev_total_assets, is_total=True)

    # Signatory block
    _write_signatory_block(ws, row, company_info)

    # Print settings
    ws.print_area = f"A1:D{row + 10}"
    ws.page_setup.orientation = "portrait"
    ws.page_setup.paperSize = ws.PAPERSIZE_A4
    ws.page_margins.left = 0.5
    ws.page_margins.right = 0.5


def _build_pl_statement(wb, pl_data, company_info, prev_pl_data=None):
    """Build the Profit & Loss Statement worksheet."""
    ws = wb.create_sheet("Profit and Loss Statement")

    year_end = company_info.get("year_end", "31.03.2026")
    prev_year_end = company_info.get("prev_year_end", "31.03.2025")

    # Company header
    row = _write_company_header(
        ws, company_info, 1,
        f"Statement of Profit and Loss for the year ended {year_end}",
        max_col=4
    )

    # Column headers
    headers = [
        ("Particulars", 55),
        ("Note", 8),
        (f"Year ended {year_end}", 20),
        (f"Year ended {prev_year_end}", 20),
    ]
    row = _write_column_headers(ws, row, headers)

    # I. Revenue from operations
    row = _write_amount_row(ws, row, "I. Revenue from operations",
                            SCHEDULE_III_PL["revenue_from_operations"]["note"],
                            pl_data["revenue"],
                            prev_pl_data["revenue"] if prev_pl_data else None,
                            font=FONT_NORMAL)

    # II. Other Income
    row = _write_amount_row(ws, row, "II. Other Income",
                            SCHEDULE_III_PL["other_income"]["note"],
                            pl_data["other_income"],
                            prev_pl_data["other_income"] if prev_pl_data else None,
                            font=FONT_NORMAL)

    # III. Total Income
    row = _write_amount_row(ws, row, "III. Total Income (I + II)", "",
                            pl_data["total_income"],
                            prev_pl_data["total_income"] if prev_pl_data else None,
                            font=FONT_NORMAL_BOLD, alignment=ALIGN_LEFT,
                            is_total=True)

    row += 1

    # IV. Expenses
    cell = ws.cell(row=row, column=1, value="IV. Expenses:")
    cell.font = FONT_SECTION
    cell.alignment = ALIGN_LEFT
    row += 1

    expenses = pl_data["expenses"]
    prev_expenses = prev_pl_data["expenses"] if prev_pl_data else None

    expense_order = ["cost_of_materials", "changes_in_inventories", "employee_benefit",
                     "finance_costs", "depreciation", "other_expenses"]

    for key in expense_order:
        if key in expenses:
            item = expenses[key]
            prev_amt = prev_expenses[key]["amount"] if prev_expenses and key in prev_expenses else None
            row = _write_amount_row(ws, row, f"     {item['label']}", item["note"],
                                    item["amount"], prev_amt)

    # Total Expenses
    row = _write_amount_row(ws, row, "Total Expenses", "",
                            pl_data["total_expenses"],
                            prev_pl_data["total_expenses"] if prev_pl_data else None,
                            font=FONT_NORMAL_BOLD, alignment=ALIGN_LEFT,
                            is_total=True)

    row += 1

    # V. Profit before tax
    row = _write_amount_row(ws, row, "V. Profit/(Loss) before tax (III - IV)", "",
                            pl_data["profit_before_tax"],
                            prev_pl_data["profit_before_tax"] if prev_pl_data else None,
                            font=FONT_NORMAL_BOLD, alignment=ALIGN_LEFT)

    # Tax expense
    row += 1
    cell = ws.cell(row=row, column=1, value="VI. Tax expense:")
    cell.font = FONT_NORMAL_BOLD
    cell.alignment = ALIGN_LEFT_INDENT1
    row += 1

    row = _write_amount_row(ws, row, "     (1) Current tax", "",
                            pl_data["tax_current"],
                            prev_pl_data["tax_current"] if prev_pl_data else None)
    row = _write_amount_row(ws, row, "     (2) Deferred tax", "4",
                            pl_data["tax_deferred"],
                            prev_pl_data["tax_deferred"] if prev_pl_data else None)

    # Profit after tax
    row = _write_amount_row(ws, row, "VII. Profit/(Loss) after tax (V - VI)", "",
                            pl_data["profit_after_tax"],
                            prev_pl_data["profit_after_tax"] if prev_pl_data else None,
                            font=FONT_NORMAL_BOLD, alignment=ALIGN_LEFT,
                            is_total=True)

    row += 1

    # EPS section
    cell = ws.cell(row=row, column=1, value="VIII. Earnings per equity share:")
    cell.font = FONT_NORMAL_BOLD
    cell.alignment = ALIGN_LEFT
    row += 1

    row = _write_amount_row(ws, row, "     (1) Basic", "", None, None)
    row = _write_amount_row(ws, row, "     (2) Diluted", "", None, None)

    # Signatory
    _write_signatory_block(ws, row, company_info)

    # Print settings
    ws.print_area = f"A1:D{row + 10}"
    ws.page_setup.orientation = "portrait"
    ws.page_setup.paperSize = ws.PAPERSIZE_A4
    ws.page_margins.left = 0.5
    ws.page_margins.right = 0.5


def _build_notes_sheet(wb):
    """Build a Notes to Accounts placeholder sheet."""
    ws = wb.create_sheet("Notes to Accounts")

    ws.column_dimensions["A"].width = 8
    ws.column_dimensions["B"].width = 50
    ws.column_dimensions["C"].width = 20
    ws.column_dimensions["D"].width = 20

    row = 1
    cell = ws.cell(row=row, column=1, value="Notes to Financial Statements")
    cell.font = FONT_SUBTITLE
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=4)
    cell.alignment = ALIGN_CENTER
    row += 2

    notes = [
        ("1", "Share Capital"),
        ("2", "Reserves and Surplus"),
        ("3", "Long-term borrowings"),
        ("4", "Deferred tax liabilities / assets"),
        ("5", "Short-term borrowings"),
        ("6", "Trade payables"),
        ("7", "Other current liabilities"),
        ("8", "Short-term provisions"),
        ("9", "Property, Plant and Equipment / Depreciation"),
        ("10", "Inventories"),
        ("11", "Trade receivables"),
        ("12", "Cash and cash equivalents"),
        ("13", "Other current assets"),
        ("14", "Revenue from operations"),
        ("15", "Other Income"),
        ("16", "Cost of materials consumed"),
        ("17", "Changes in inventories"),
        ("18", "Employee benefit expense"),
        ("19", "Finance costs"),
        ("20", "Other expenses"),
    ]

    for note_num, note_title in notes:
        cell = ws.cell(row=row, column=1, value=f"Note {note_num}")
        cell.font = FONT_NORMAL_BOLD
        cell = ws.cell(row=row, column=2, value=note_title)
        cell.font = FONT_NORMAL_BOLD
        cell.border = BOTTOM_BORDER
        row += 1

        # Headers
        ws.cell(row=row, column=2, value="Particulars").font = FONT_HEADER
        ws.cell(row=row, column=3, value="Current Year").font = FONT_HEADER
        ws.cell(row=row, column=4, value="Previous Year").font = FONT_HEADER
        for c in range(2, 5):
            ws.cell(row=row, column=c).alignment = ALIGN_CENTER
            ws.cell(row=row, column=c).fill = HEADER_FILL
            ws.cell(row=row, column=c).border = HEADER_BORDER
        row += 1

        # Placeholder rows
        for _ in range(3):
            ws.cell(row=row, column=2, value="").font = FONT_NORMAL
            row += 1

        # Total row
        ws.cell(row=row, column=2, value="Total").font = FONT_NORMAL_BOLD
        for c in range(2, 5):
            ws.cell(row=row, column=c).border = TOP_BOTTOM_BORDER
        row += 2


# ── MAIN GENERATOR ─────────────────────────────────────────────────────────

def generate_schedule_iii(db_path=None, company_info=None, output_path="financial_statements.xlsx"):
    """Generate Companies Act Schedule III compliant financial statements.

    Args:
        db_path: Path to tally_data.db
        company_info: Dict with optional overrides:
            {
                "name": "Company Name",
                "cin": "CIN Number",
                "address": "Registered Office",
                "year_end": "31.03.2026",
                "prev_year_end": "31.03.2025",
                "auditor_name": "CA Firm Name",
                "auditor_frn": "FRN",
                "auditor_member": "Membership No",
                "director1_name": "Director 1",
                "director1_din": "DIN",
                "director2_name": "Director 2",
                "director2_din": "DIN",
            }
        output_path: Where to save the Excel file

    Returns:
        dict with bs_data, pl_data, and output_path
    """
    conn = _get_conn(db_path or DB_PATH)

    # Load metadata defaults
    metadata = _get_metadata(conn)

    if company_info is None:
        company_info = {}

    # Fill in defaults from metadata
    if "name" not in company_info:
        company_info["name"] = metadata.get("company_name", "Company Name")
    if "year_end" not in company_info:
        company_info["year_end"] = "31.03.2026"
    if "prev_year_end" not in company_info:
        company_info["prev_year_end"] = "31.03.2025"

    # Extract data
    bs_data = extract_balance_sheet_data(conn)
    pl_data = extract_pl_data(conn)

    # Build workbook
    wb = Workbook()

    _build_balance_sheet(wb, bs_data, company_info)
    _build_pl_statement(wb, pl_data, company_info)
    _build_notes_sheet(wb)

    # Save
    wb.save(output_path)
    conn.close()

    return {
        "bs_data": bs_data,
        "pl_data": pl_data,
        "output_path": output_path,
    }


def get_bs_preview_data(db_path=None):
    """Get Balance Sheet data for Streamlit preview."""
    conn = _get_conn(db_path or DB_PATH)
    data = extract_balance_sheet_data(conn)
    conn.close()
    return data


def get_pl_preview_data(db_path=None, from_date=None, to_date=None):
    """Get P&L data for Streamlit preview."""
    conn = _get_conn(db_path or DB_PATH)
    data = extract_pl_data(conn, from_date, to_date)
    conn.close()
    return data


# ── CLI TEST ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    result = generate_schedule_iii(
        output_path="/tmp/test_financial_statements.xlsx"
    )
    bs = result["bs_data"]
    pl = result["pl_data"]

    print("=== BALANCE SHEET ===")
    print(f"Total Liabilities: {bs['total_liabilities']:,.2f}")
    print(f"Total Assets:      {bs['total_assets']:,.2f}")
    print(f"Difference:        {bs['total_liabilities'] - bs['total_assets']:,.2f}")

    print("\n--- Liabilities ---")
    for section, items in bs["liabilities"].items():
        print(f"  {section}:")
        for key, item in items.items():
            print(f"    {item['label']}: {item['amount']:,.2f}")

    print("\n--- Assets ---")
    for section, items in bs["assets"].items():
        print(f"  {section}:")
        for key, item in items.items():
            print(f"    {item['label']}: {item['amount']:,.2f}")

    print("\n=== PROFIT & LOSS ===")
    print(f"Revenue:           {pl['revenue']:,.2f}")
    print(f"Other Income:      {pl['other_income']:,.2f}")
    print(f"Total Income:      {pl['total_income']:,.2f}")
    print(f"Total Expenses:    {pl['total_expenses']:,.2f}")
    print(f"Profit before tax: {pl['profit_before_tax']:,.2f}")

    print("\n--- Expenses ---")
    for key, item in pl["expenses"].items():
        print(f"  {item['label']}: {item['amount']:,.2f}")

    print(f"\nExcel saved to: {result['output_path']}")
