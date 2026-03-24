"""
Seven Labs Vision — Conversational Engine
Primary: Gemini LLM for natural language understanding.
Fallback: Local keyword classifier if Gemini unavailable.
Routes to report templates or generates ad-hoc SQL.
"""

import json
import os
import re
import sqlite3
import traceback
import logging
from tally_reports import (
    get_conn, trial_balance, profit_and_loss, balance_sheet,
    ledger_detail, pl_group_drilldown, debtor_aging, creditor_aging,
    voucher_summary, search_ledger, get_all_groups_under,
    get_groups_by_nature,
)

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "tally_data.db")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

_MONTH_ABBREVS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _dynamic_month_label(ym):
    """Convert YYYYMM code like '202504' to \"Apr'25\" dynamically."""
    if not ym or len(ym) < 6:
        return str(ym or "")
    try:
        y = int(ym[:4])
        m = int(ym[4:6])
        return f"{_MONTH_ABBREVS[m]}'{str(y)[-2:]}"
    except (ValueError, IndexError):
        return ym


def _group_ph(conn, root_groups):
    """Return (placeholders_sql, group_list) for recursive group queries."""
    if isinstance(root_groups, str):
        root_groups = [root_groups]
    groups = list(get_all_groups_under(conn, root_groups))
    return ",".join(["?"] * len(groups)), groups


def _nature_ph(conn, nature):
    """Return (placeholders_sql, group_list) using Tally's flag-based classification."""
    groups = get_groups_by_nature(conn, nature)
    if not groups:
        return "'__NONE__'", []
    return ",".join(["?"] * len(groups)), groups


def _nature_ph_bank_cash(conn):
    """Return (placeholders_sql, group_list) for bank + bank_od + cash groups."""
    groups = get_groups_by_nature(conn, 'bank') + get_groups_by_nature(conn, 'bank_od') + get_groups_by_nature(conn, 'cash')
    groups = list(dict.fromkeys(groups))
    if not groups:
        return "'__NONE__'", []
    return ",".join(["?"] * len(groups)), groups


def _has_table(conn_or_path, table_name):
    """Check if a table exists. Accepts a connection or path."""
    try:
        if isinstance(conn_or_path, str):
            c = sqlite3.connect(conn_or_path)
            row = c.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            ).fetchone()
            c.close()
            return (row[0] if row else 0) > 0
        else:
            row = conn_or_path.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            ).fetchone()
            return (row[0] if row else 0) > 0
    except Exception:
        return False


def _has_column(conn, table_name, column_name):
    """Check if a column exists in a table."""
    try:
        cols = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return any(c[1].upper() == column_name.upper() for c in (cols or []))
    except Exception:
        return False


def _bal_col(conn):
    """Return the best balance column: COMPUTED_CB if available, else CLOSINGBALANCE."""
    if _has_column(conn, "mst_ledger", "COMPUTED_CB"):
        return "COMPUTED_CB"
    return "CLOSINGBALANCE"


def _safe_fetchone(cursor_result):
    """Safely get fetchone result, returning None on failure."""
    try:
        row = cursor_result.fetchone() if cursor_result else None
        return row
    except Exception:
        return None


def _safe_fetchall(cursor_result):
    """Safely get fetchall result, returning [] on failure."""
    try:
        rows = cursor_result.fetchall() if cursor_result else []
        return rows or []
    except Exception:
        return []


def _get_company_name():
    """Get company name from metadata."""
    try:
        conn = sqlite3.connect(DB_PATH)
        if not _has_table(conn, '_metadata'):
            conn.close()
            return "the company"
        row = conn.execute("SELECT value FROM _metadata WHERE key = 'company_name'").fetchone()
        conn.close()
        return row[0] if row else "the company"
    except Exception:
        return "the company"

USE_LLM = True  # Set to False to force keyword-only mode (skip Gemini)

# ── GEMINI LLM INTEGRATION ───────────────────────────────────────────────────

_gemini_model = None

def _get_gemini_model():
    """Lazy-init Gemini Flash model."""
    global _gemini_model
    if _gemini_model is None:
        try:
            import google.generativeai as genai
            genai.configure(api_key=GEMINI_API_KEY)
            _gemini_model = genai.GenerativeModel("gemini-2.0-flash")
            logger.info("Gemini Flash model initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Gemini: {e}")
            _gemini_model = False  # Mark as failed so we don't retry
    return _gemini_model if _gemini_model else None


def _get_system_prompt():
    company = _get_company_name()
    return f"""You are an expert Chartered Accountant AI assistant for "{company}".

You have access to the company's complete Tally ERP financial data.

YOUR PERSONALITY:
- You speak like a knowledgeable, friendly CA advisor
- You give insights, not just numbers — explain what the numbers mean
- You flag concerns and give actionable recommendations
- You use Indian accounting conventions (Lakhs, Crores, ₹)
- Keep responses concise but informative (3-8 sentences typically)
- Use markdown for formatting (bold, bullet points, tables when helpful)
- When discussing amounts, always use ₹ symbol

IMPORTANT RULES:
- You can ONLY answer questions about {company}'s financial data
- For non-financial questions (weather, cricket, movies, etc.), politely redirect to financial topics
- NEVER suggest modifying data — you are read-only
- If asked to delete/modify/create anything, explain you're a read-only analyst
- Always be honest if data doesn't support a conclusion

You will receive the user's question along with RELEVANT DATA that has been pre-fetched from the database. Use this data to formulate your answer. Do NOT make up numbers — only use what's provided in the data context.
"""


def _build_data_context(question):
    """Pre-fetch relevant data from the DB based on the question, so Gemini can reason about it."""
    q = question.lower().strip()
    conn = get_conn()
    context_parts = []

    try:
        # Always include a P&L summary for financial context
        try:
            pl = profit_and_loss(conn)
            context_parts.append(f"""P&L SUMMARY:
- Total Income (Revenue): ₹{pl['total_income']:,.0f}
- Total Expenses: ₹{pl['total_expense']:,.0f}
- Gross Profit: ₹{pl['gross_profit']:,.0f}
- Net Profit: ₹{pl['net_profit']:,.0f}
- GP Margin: {(pl['gross_profit']/pl['total_income']*100) if pl['total_income'] else 0:.1f}%
- Net Margin: {(pl['net_profit']/pl['total_income']*100) if pl['total_income'] else 0:.1f}%""")

            # Income breakdown
            income_lines = []
            for group, entries in pl.get("income", {}).items():
                gtotal = sum(abs(a) for _, a in entries)
                income_lines.append(f"  {group}: ₹{gtotal:,.0f}")
                for led, amt in entries[:5]:
                    income_lines.append(f"    {led}: ₹{abs(amt):,.0f}")
            if income_lines:
                context_parts.append("INCOME BREAKDOWN:\n" + "\n".join(income_lines))

            # Expense breakdown
            expense_lines = []
            for group, entries in pl.get("expense", {}).items():
                gtotal = sum(abs(a) for _, a in entries)
                expense_lines.append(f"  {group}: ₹{gtotal:,.0f}")
                for led, amt in entries[:5]:
                    expense_lines.append(f"    {led}: ₹{abs(amt):,.0f}")
            if expense_lines:
                context_parts.append("EXPENSE BREAKDOWN:\n" + "\n".join(expense_lines))
        except Exception:
            pass

        # Monthly sales data
        if any(kw in q for kw in ["sales", "revenue", "trend", "month", "grow", "best", "worst",
                                    "increas", "decreas", "compare", "quarter", "q1", "q2", "q3", "q4",
                                    "performance", "business", "summary", "overview", "how are", "how is",
                                    "how did", "tell me"]):
            try:
                from analytics import monthly_sales
                data = monthly_sales(conn)
                labels = {"202504": "Apr'25", "202505": "May'25", "202506": "Jun'25",
                          "202507": "Jul'25", "202508": "Aug'25", "202509": "Sep'25",
                          "202510": "Oct'25", "202511": "Nov'25", "202512": "Dec'25",
                          "202601": "Jan'26"}
                lines = ["MONTHLY SALES:"]
                for month, count, amt in data:
                    lines.append(f"  {labels.get(month, month)}: ₹{amt:,.0f} ({count} invoices)")
                context_parts.append("\n".join(lines))
            except Exception:
                pass

        # Monthly purchases
        if any(kw in q for kw in ["purchase", "buy", "cost", "cogs", "supplier"]):
            try:
                from analytics import monthly_purchases
                data = monthly_purchases(conn)
                labels = {"202504": "Apr'25", "202505": "May'25", "202506": "Jun'25",
                          "202507": "Jul'25", "202508": "Aug'25", "202509": "Sep'25",
                          "202510": "Oct'25", "202511": "Nov'25", "202512": "Dec'25",
                          "202601": "Jan'26"}
                lines = ["MONTHLY PURCHASES:"]
                for month, count, amt in data:
                    lines.append(f"  {labels.get(month, month)}: ₹{amt:,.0f} ({count} bills)")
                context_parts.append("\n".join(lines))
            except Exception:
                pass

        # Bank balances
        if any(kw in q for kw in ["bank", "cash", "balance", "money", "fund", "liquid"]):
            try:
                has_closing = _has_column(conn, "mst_ledger", "COMPUTED_CB") or _has_column(conn, "mst_ledger", "CLOSINGBALANCE")
                if has_closing:
                    _bk_ph, _bk_g = _nature_ph_bank_cash(conn)
                    rows = conn.execute(f"""
                        SELECT NAME, PARENT, CAST({_bal_col(conn)} AS REAL) as bal
                        FROM mst_ledger
                        WHERE PARENT IN ({_bk_ph})
                        ORDER BY ABS(CAST({_bal_col(conn)} AS REAL)) DESC
                    """, _bk_g).fetchall()
                    rows = rows or []
                    lines = ["BANK & CASH BALANCES:"]
                    for name, parent, bal in rows:
                        lines.append(f"  {name} ({parent}): ₹{abs(bal or 0):,.0f}")
                    lines.append(f"  TOTAL: ₹{sum(abs(r[2] or 0) for r in rows):,.0f}")
                    context_parts.append("\n".join(lines))
            except Exception:
                pass

        # GST data
        if any(kw in q for kw in ["gst", "gstr", "tax", "cgst", "sgst", "igst", "itc",
                                    "input tax", "output tax", "liability"]):
            try:
                from gst_engine import gst_monthly_comparison
                monthly = gst_monthly_comparison(conn)
                if monthly:
                    lines = ["GST MONTHLY SUMMARY:"]
                    for m in monthly:
                        ml = m.get('month_label', m.get('month', ''))
                        out = m.get('output_tax', m.get('total_output', 0))
                        inp = m.get('input_tax', m.get('total_input', 0))
                        net = out - inp
                        lines.append(f"  {ml}: Output ₹{out:,.0f} | Input ₹{inp:,.0f} | Net ₹{net:,.0f} ({'Payable' if net > 0 else 'Refundable'})")
                    context_parts.append("\n".join(lines))
            except Exception:
                pass

        # Debtors / receivables
        if any(kw in q for kw in ["debtor", "receivable", "owe", "outstanding", "collect",
                                    "customer", "party", "who owe"]):
            try:
                data = debtor_aging(conn)
                total = sum(b for _, b in data)
                lines = [f"SUNDRY DEBTORS (Total: ₹{total:,.0f}, {len(data)} parties):"]
                for name, bal in sorted(data, key=lambda x: x[1], reverse=True)[:15]:
                    lines.append(f"  {name}: ₹{bal:,.0f}")
                context_parts.append("\n".join(lines))
            except Exception:
                pass

        # Creditors / payables
        if any(kw in q for kw in ["creditor", "payable", "we owe", "supplier", "vendor"]):
            try:
                data = creditor_aging(conn)
                total = sum(b for _, b in data)
                lines = [f"SUNDRY CREDITORS (Total: ₹{total:,.0f}, {len(data)} parties):"]
                for name, bal in sorted(data, key=lambda x: x[1], reverse=True)[:15]:
                    lines.append(f"  {name}: ₹{bal:,.0f}")
                context_parts.append("\n".join(lines))
            except Exception:
                pass

        # Working capital / ratios
        if any(kw in q for kw in ["ratio", "working capital", "current ratio", "liquidity",
                                    "health", "ebitda", "debt", "equity", "turnover",
                                    "efficiency", "improve", "red flag", "concern", "focus",
                                    "advice", "recommend", "suggest"]):
            try:
                from analytics import key_ratios, working_capital_analysis
                ratios = key_ratios(conn)
                wc = working_capital_analysis(conn)
                lines = [f"""KEY RATIOS:
  Gross Profit Margin: {ratios['gross_profit_margin']:.1f}%
  Net Profit Margin: {ratios['net_profit_margin']:.1f}%
  Total Debtors: ₹{ratios['total_debtors']:,.0f}
  Debtor Days: {ratios['debtor_days']:.0f}
  Total Creditors: ₹{ratios['total_creditors']:,.0f}
  Creditor Days: {ratios['creditor_days']:.0f}

WORKING CAPITAL:
  Current Assets: ₹{wc['total_ca']:,.0f}
  Current Liabilities: ₹{wc['total_cl']:,.0f}
  Working Capital: ₹{wc['working_capital']:,.0f}
  Current Ratio: {wc['current_ratio']:.2f}"""]
                context_parts.append("\n".join(lines))
            except Exception:
                pass

        # Balance Sheet
        if any(kw in q for kw in ["balance sheet", "assets", "liabilit", "net worth", "capital"]):
            try:
                bs = balance_sheet(conn)
                lines = [f"""BALANCE SHEET:
  Total Assets: ₹{bs['total_assets']:,.0f}
  Total Liabilities: ₹{bs['total_liabilities']:,.0f}"""]
                for group, entries in bs.get("assets", {}).items():
                    gtotal = sum(abs(b) for _, b in entries)
                    lines.append(f"  Asset - {group}: ₹{gtotal:,.0f}")
                for group, entries in bs.get("liabilities", {}).items():
                    gtotal = sum(abs(b) for _, b in entries)
                    lines.append(f"  Liability - {group}: ₹{gtotal:,.0f}")
                context_parts.append("\n".join(lines))
            except Exception:
                pass

        # Cash flow
        if any(kw in q for kw in ["cash flow", "cashflow", "inflow", "outflow", "projection"]):
            try:
                from analytics import cash_flow_statement
                cf = cash_flow_statement(conn)
                lines = [f"""CASH FLOW STATEMENT:
  Operating Cash Flow: ₹{cf['operating_cf']:,.0f}
  Investing Cash Flow: ₹{cf['investing_cf']:,.0f}
  Financing Cash Flow: ₹{cf['financing_cf']:,.0f}
  Net Cash Flow: ₹{cf['net_cf']:,.0f}"""]
                context_parts.append("\n".join(lines))
            except Exception:
                pass

        # Stock / inventory
        if any(kw in q for kw in ["stock", "inventory", "product", "item", "hsn"]):
            try:
                count = conn.execute("SELECT COUNT(*) FROM mst_stock_item").fetchone()[0]
                groups = conn.execute("""
                    SELECT PARENT, COUNT(*) as cnt FROM mst_stock_item
                    WHERE PARENT IS NOT NULL GROUP BY PARENT ORDER BY cnt DESC LIMIT 10
                """).fetchall()
                lines = [f"INVENTORY: {count} total stock items"]
                for g, c in groups:
                    lines.append(f"  {g}: {c} items")
                context_parts.append("\n".join(lines))
            except Exception:
                pass

        # Voucher counts
        if any(kw in q for kw in ["voucher", "invoice", "transaction", "how many", "count",
                                    "total number"]):
            try:
                rows = conn.execute("""
                    SELECT VOUCHERTYPENAME, COUNT(*) as cnt,
                           ABS(SUM(CAST(
                               (SELECT a.AMOUNT FROM trn_accounting a
                                WHERE a.VOUCHER_GUID = trn_voucher.GUID
                                AND a.ISDEEMEDPOSITIVE = 'Yes' LIMIT 1) AS REAL))) as total
                    FROM trn_voucher GROUP BY VOUCHERTYPENAME ORDER BY cnt DESC
                """).fetchall()
                lines = ["VOUCHER SUMMARY:"]
                for vtype, cnt, total in rows:
                    lines.append(f"  {vtype}: {cnt} vouchers (₹{total or 0:,.0f})")
                context_parts.append("\n".join(lines))
            except Exception:
                pass

        # Party-specific lookup — try to find the party name mentioned
        # Extract potential party/ledger name from the question
        party_keywords = ["ledger of", "ledger for", "statement of", "statement for",
                         "account of", "account for", "balance of", "balance for",
                         "business with", "transactions with", "how much does", "how much do"]
        for pk in party_keywords:
            if pk in q:
                name_part = q.split(pk, 1)[1].strip().rstrip("?.,!")
                if name_part and len(name_part) > 2:
                    try:
                        bc = _bal_col(conn)
                        matches = conn.execute(f"""
                            SELECT NAME, PARENT, CAST({bc} AS REAL)
                            FROM mst_ledger
                            WHERE LOWER(NAME) LIKE ?
                            ORDER BY ABS(CAST({bc} AS REAL)) DESC LIMIT 5
                        """, (f"%{name_part}%",)).fetchall()
                        if matches:
                            lines = [f"LEDGER SEARCH for '{name_part}':"]
                            for name, parent, bal in matches:
                                lines.append(f"  {name} ({parent}): ₹{abs(bal or 0):,.0f}")
                            context_parts.append("\n".join(lines))
                    except Exception:
                        pass
                break

        # Ad-hoc SQL for specific queries Gemini might need
        # Provide general stats
        try:
            total_vouchers = conn.execute("SELECT COUNT(*) FROM trn_voucher").fetchone()[0]
            total_ledgers = conn.execute("SELECT COUNT(*) FROM mst_ledger").fetchone()[0]
            _dr_p, _dr_g = _nature_ph(conn, 'debtors')
            total_debtors = conn.execute(f"SELECT COUNT(*) FROM mst_ledger WHERE PARENT IN ({_dr_p})", _dr_g).fetchone()[0]
            _cr_p, _cr_g = _nature_ph(conn, 'creditors')
            total_creditors = conn.execute(f"SELECT COUNT(*) FROM mst_ledger WHERE PARENT IN ({_cr_p})", _cr_g).fetchone()[0]
            context_parts.append(f"""GENERAL STATS:
  Total Vouchers: {total_vouchers}
  Total Ledger Accounts: {total_ledgers}
  Total Debtors (customers): {total_debtors}
  Total Creditors (suppliers): {total_creditors}
  Data Period: April 2025 to January 2026 """)
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Error building data context: {e}")
    finally:
        conn.close()

    return "\n\n".join(context_parts)


def ask_gemini(question, conversation_history=None):
    """Send question to Gemini Flash with pre-fetched data context.
    Returns a conversational string response, or None if Gemini is unavailable."""
    if not USE_LLM or not GEMINI_API_KEY:
        return None

    model = _get_gemini_model()
    if model is None:
        return None

    try:
        # Build data context from the database
        data_context = _build_data_context(question)

        # Build the full prompt
        full_prompt = f"""{_get_system_prompt()}

--- RELEVANT FINANCIAL DATA ---
{data_context}
--- END DATA ---

User's question: {question}

Respond naturally as a CA advisor. Use the data above to give an accurate, insightful answer. Format with markdown."""

        # If there's conversation history, include it
        if conversation_history and len(conversation_history) > 0:
            history_text = "\n".join([
                f"{'User' if i % 2 == 0 else 'Assistant'}: {msg}"
                for i, msg in enumerate(conversation_history[-6:])  # Last 3 exchanges
            ])
            full_prompt = f"""{_get_system_prompt()}

--- CONVERSATION HISTORY ---
{history_text}
--- END HISTORY ---

--- RELEVANT FINANCIAL DATA ---
{data_context}
--- END DATA ---

User's question: {question}

Respond naturally as a CA advisor. Use the data above to give an accurate, insightful answer. Format with markdown."""

        response = model.generate_content(full_prompt)
        answer = response.text.strip()

        if answer:
            logger.info(f"Gemini answered: {answer[:100]}...")
            return answer
        return None

    except Exception as e:
        logger.error(f"Gemini API error: {e}")
        return None

# ── SCHEMA CONTEXT FOR LLM ──────────────────────────────────────────────────

SCHEMA_CONTEXT = """
You are an AI assistant for a Chartered Accountant analyzing Tally ERP data.

DATABASE SCHEMA (SQLite):

1. mst_group - Chart of accounts hierarchy (28 groups)
   Columns: NAME, PARENT, ISREVENUE, ISDEEMEDPOSITIVE, AFFECTSGROSSPROFIT, GUID, ALTERID, ISADDABLE, ISSUBLEDGER, RESERVEDNAME
   Root groups (PARENT='Primary'): Capital Account, Current Assets, Current Liabilities, Direct Expenses, Direct Incomes, Fixed Assets, Indirect Expenses, Indirect Incomes, Investments, Loans (Liability), Misc. Expenses (ASSET), Purchase Accounts, Sales Accounts, Suspense A/c
   P&L groups: ISREVENUE='Yes' (Sales Accounts, Purchase Accounts, Direct/Indirect Incomes/Expenses)
   BS groups: ISREVENUE='No' (all others)

2. mst_ledger - All accounts (480 ledgers)
   Columns: NAME, PARENT, OPENINGBALANCE, CLOSINGBALANCE, COMPUTED_CB (preferred), GUID, ALTERID, COUNTRYOFRESIDENCE, ISREVENUE, AFFECTSSTOCK, CREDITLIMIT, LEDGERSTATENAME, LEDGERMOBILE_LIST, LEDGERPHONE_LIST
   Key groups: Sundry Debtors (315), Sundry Creditors (35), Duties & Taxes (30), Indirect Expenses (18)

3. mst_stock_item - Inventory items (185 items)
   Columns: NAME, PARENT, GUID, ALTERID, BASEUNITS, HSNCODE, RESERVEDNAME, etc.

4. trn_voucher - All transactions (3100 vouchers)
   Key columns: GUID, VCHTYPE, VOUCHERTYPENAME, DATE (YYYYMMDD format), VOUCHERNUMBER, PARTYLEDGERNAME, NARRATION, MASTERID, ALTERID
   Voucher types: Sales, Purchase, Payment, Receipt, Journal, Contra, Credit Note, Debit Note

5. trn_accounting - Ledger entries in vouchers (17029 entries)
   Key columns: VOUCHER_GUID, LEDGERNAME, AMOUNT, ISDEEMEDPOSITIVE
   AMOUNT: negative = debit to this ledger, positive = credit to this ledger
   Links to trn_voucher via VOUCHER_GUID = GUID

6. trn_bank - Bank allocations (10176 entries)
   Key columns: VOUCHER_GUID, LEDGERNAME, INSTRUMENTDATE, INSTRUMENTNUMBER, AMOUNT, BANKNAME, TRANSACTIONTYPE

IMPORTANT TALLY CONVENTIONS:
- Dates are stored as YYYYMMDD strings (e.g., '20250401' = April 1, 2025)
- ACTUAL DATA RANGE: April 2025 (20250401) to January 2026 (20260131). This is current FY.
- When generating reports WITHOUT a specific date request from the user, pass null/None for dates to include ALL data. Do NOT assume a financial year date range.
- AMOUNT in trn_accounting: negative = debit side, positive = credit side
- For Sundry Debtors: negative closing balance = they owe us (receivable is positive in accounting terms)
- For Sundry Creditors: positive closing balance = we owe them (payable)
- ISDEEMEDPOSITIVE='Yes' means the natural balance is debit (assets, expenses)

AVAILABLE REPORT FUNCTIONS (use these when possible instead of raw SQL):
1. trial_balance() - Full trial balance
2. profit_and_loss(from_date, to_date) - P&L with group drilldown
3. balance_sheet(as_of_date) - Balance Sheet
4. ledger_detail(ledger_name, from_date, to_date) - Statement of account for a ledger
5. pl_group_drilldown(group_name, from_date, to_date) - All transactions in a P&L group
6. debtor_aging() - Outstanding receivables
7. creditor_aging() - Outstanding payables
8. voucher_summary(from_date, to_date) - Count/total by voucher type
9. search_ledger(query) - Search ledgers by name
"""

SYSTEM_PROMPT = SCHEMA_CONTEXT + """
When the user asks a question:

1. DETERMINE what they need. Think about which report or query answers their question.

2. RESPOND with a JSON action. Your response MUST be valid JSON with this structure:
{
  "thinking": "brief explanation of what the user wants",
  "action": "one of: report_pl, report_bs, report_tb, ledger_detail, pl_drilldown, debtors, creditors, voucher_summary, search, sql_query, chat",
  "params": {parameters for the action},
  "explanation": "brief natural language explanation to show the user"
}

ACTION DETAILS:
- "report_pl": params: {"from_date": "YYYYMMDD" or null, "to_date": "YYYYMMDD" or null}
- "report_bs": params: {"as_of_date": "YYYYMMDD" or null}
- "report_tb": params: {"as_of_date": "YYYYMMDD" or null}
- "ledger_detail": params: {"ledger_name": "exact name", "from_date": null, "to_date": null}
- "pl_drilldown": params: {"group_name": "exact group name", "from_date": null, "to_date": null}
- "debtors": params: {}
- "creditors": params: {}
- "voucher_summary": params: {"from_date": null, "to_date": null}
- "search": params: {"query": "search term"}
- "sql_query": params: {"sql": "SELECT ... (read-only, no modifications)"} — use ONLY for questions not covered by reports
- "chat": params: {"response": "text answer"} — for general questions about the data/company

IMPORTANT:
- Always use exact ledger/group names from the schema
- If the user asks about a party, search for the ledger name first
- For P&L drilldown, use the exact group name (e.g., "Sales Accounts", "Indirect Expenses")
- Dates must be YYYYMMDD format
- SQL queries must be SELECT only — never INSERT, UPDATE, DELETE
- If unsure about a ledger name, use search action first
- Keep explanations concise and professional, suitable for a CA audience
"""


def _fmt_indian(amount):
    """Format amount in Indian style with appropriate scale (Lakhs/Crores)."""
    if amount is None:
        return "₹0"
    amount = abs(float(amount))
    if amount >= 1_00_00_000:
        return f"₹{amount / 1_00_00_000:.2f} Cr"
    elif amount >= 1_00_000:
        return f"₹{amount / 1_00_000:.2f} L"
    elif amount >= 1_000:
        return f"₹{amount:,.0f}"
    else:
        return f"₹{amount:,.2f}"


def _fmt_exact(amount):
    """Format amount with Indian comma style for exact display."""
    if amount is None:
        return "₹0"
    amount = float(amount)
    neg = amount < 0
    amount = abs(amount)
    int_part = int(amount)
    dec_part = f"{amount - int_part:.0f}"
    s = str(int_part)
    if len(s) <= 3:
        result = s
    else:
        result = s[-3:]
        s = s[:-3]
        while s:
            result = s[-2:] + "," + result
            s = s[:-2]
    return f"{'−' if neg else ''}₹{result}"


def _month_label(code):
    """Convert YYYYMM to readable month label."""
    month_names = {
        "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
        "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
        "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec",
    }
    if code and len(code) >= 6:
        mm = code[4:6]
        yy = code[2:4]
        return f"{month_names.get(mm, mm)}'{yy}"
    return code or ""


def _month_full(code):
    """Convert YYYYMM to full month name."""
    month_names = {
        "01": "January", "02": "February", "03": "March", "04": "April",
        "05": "May", "06": "June", "07": "July", "08": "August",
        "09": "September", "10": "October", "11": "November", "12": "December",
    }
    if code and len(code) >= 6:
        mm = code[4:6]
        yyyy = code[:4]
        return f"{month_names.get(mm, mm)} {yyyy}"
    return code or ""


MONTH_MAP = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "jun": "06", "jul": "07", "aug": "08", "sep": "09",
    "oct": "10", "nov": "11", "dec": "12",
}


def _extract_month_code(q):
    """Extract YYYYMM month code from natural language question."""
    for name, mm in MONTH_MAP.items():
        if name in q:
            # Determine year based on current FY (Apr 2025 to Mar 2026)
            m_int = int(mm)
            if m_int >= 4:
                return f"2025{mm}"
            else:
                return f"2026{mm}"
    return None


def _extract_two_months(q):
    """Extract two month codes for comparison questions."""
    found = []
    # Sort by length descending so 'november' matches before 'nov'
    sorted_months = sorted(MONTH_MAP.items(), key=lambda x: len(x[0]), reverse=True)
    matched_positions = set()
    for name, mm in sorted_months:
        idx = q.find(name)
        if idx >= 0:
            # Check this position hasn't been claimed by a longer match
            if any(idx >= mp[0] and idx < mp[1] for mp in matched_positions):
                continue
            m_int = int(mm)
            year = "2025" if m_int >= 4 else "2026"
            found.append((idx, f"{year}{mm}", name.capitalize()))
            matched_positions.add((idx, idx + len(name)))
    found.sort(key=lambda x: x[0])
    if len(found) >= 2:
        return (found[0][1], found[0][2]), (found[1][1], found[1][2])
    return None, None


def _extract_quarter(q):
    """Extract quarter month codes. Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar."""
    q_map = {
        "q1": (["202504", "202505", "202506"], "Q1 (Apr-Jun 2025)"),
        "q2": (["202507", "202508", "202509"], "Q2 (Jul-Sep 2025)"),
        "q3": (["202510", "202511", "202512"], "Q3 (Oct-Dec 2025)"),
        "q4": (["202601", "202602", "202603"], "Q4 (Jan-Mar 2026)"),
        "quarter 1": (["202504", "202505", "202506"], "Q1 (Apr-Jun 2025)"),
        "quarter 2": (["202507", "202508", "202509"], "Q2 (Jul-Sep 2025)"),
        "quarter 3": (["202510", "202511", "202512"], "Q3 (Oct-Dec 2025)"),
        "quarter 4": (["202601", "202602", "202603"], "Q4 (Jan-Mar 2026)"),
        "first quarter": (["202504", "202505", "202506"], "Q1 (Apr-Jun 2025)"),
        "second quarter": (["202507", "202508", "202509"], "Q2 (Jul-Sep 2025)"),
        "third quarter": (["202510", "202511", "202512"], "Q3 (Oct-Dec 2025)"),
        "fourth quarter": (["202601", "202602", "202603"], "Q4 (Jan-Mar 2026)"),
    }
    for key, val in q_map.items():
        if key in q:
            return val
    return None, None


def _extract_amount(q):
    """Extract amount from question like '5 lakhs', '1 lakh', '50000', '2 crore'."""
    # Try "X lakh(s)" pattern
    m = re.search(r'(\d+(?:\.\d+)?)\s*(?:lakh|lac|lacs|lakhs)', q)
    if m:
        return float(m.group(1)) * 100000
    # Try "X crore(s)" pattern
    m = re.search(r'(\d+(?:\.\d+)?)\s*(?:crore|crores|cr)', q)
    if m:
        return float(m.group(1)) * 10000000
    # Try plain number (5 digits+)
    m = re.search(r'(\d{5,})', q)
    if m:
        return float(m.group(1))
    return None


def _fuzzy_match_party(conn, query_name):
    """Fuzzy match a party/ledger name against the database. Returns best match or None."""
    query_name = query_name.strip()
    if not query_name or len(query_name) < 2:
        return None

    # First try exact match (case-insensitive)
    bc = _bal_col(conn)
    row = conn.execute(
        f"SELECT NAME, PARENT, CAST({bc} AS REAL) FROM mst_ledger WHERE LOWER(NAME) = LOWER(?)",
        (query_name,)
    ).fetchone()
    if row:
        return row

    # Try LIKE match
    results = conn.execute(
        f"SELECT NAME, PARENT, CAST({bc} AS REAL) FROM mst_ledger WHERE LOWER(NAME) LIKE LOWER(?) ORDER BY NAME LIMIT 10",
        (f"%{query_name}%",)
    ).fetchall()
    if results:
        # Prefer shorter names (more specific matches)
        results.sort(key=lambda r: len(r[0]))
        return results[0]

    # Try matching individual words
    words = query_name.split()
    if len(words) > 1:
        for word in words:
            if len(word) >= 3:
                results = conn.execute(
                    f"SELECT NAME, PARENT, CAST({bc} AS REAL) FROM mst_ledger WHERE LOWER(NAME) LIKE LOWER(?) ORDER BY NAME LIMIT 5",
                    (f"%{word}%",)
                ).fetchall()
                if results:
                    return results[0]

    return None


def _extract_entity_name(q):
    """Try to extract a party/ledger entity name from the question."""
    # Patterns to extract entity names
    patterns = [
        r"(?:of|for|from|by|with|to)\s+(?:party\s+)?[\"']([^\"']+)[\"']",   # quoted
        r"(?:of|for|from|by|with|to)\s+(?:party\s+)?([A-Z][A-Za-z\s&.\-()]+?)(?:\s*\?|\s*$|\s+(?:owe|ow|balance|ledger|account|total|this|last|in|for|from))",
        r"(?:does|do|did|is|has|have)\s+([A-Z][A-Za-z\s&.\-()]+?)\s+(?:owe|pay|purchase|buy|sell|give)",
        r"(?:show|get|display|tell|give)\s+(?:me\s+)?(?:the\s+)?([A-Z][A-Za-z\s&.\-()]+?)\s+(?:ledger|account|statement|balance|detail)",
        r"(?:ledger|account|statement|balance)\s+(?:of|for)\s+(.+?)(?:\s*\?|\s*$)",
        r"(?:business\s+with)\s+(.+?)(?:\s*\?|\s*$)",
    ]
    for pat in patterns:
        match = re.search(pat, q, re.IGNORECASE)
        if match:
            name = match.group(1).strip().rstrip("?.,!").strip()
            if len(name) >= 2 and name.lower() not in {"me", "my", "the", "all", "this", "that", "it", "i"}:
                return name
    return None


def smart_answer(question):
    """
    Comprehensive conversational layer — multi-stage intent understanding system.

    DEFENSIVE: All database queries wrapped in try/except. Handles missing
    tables/columns gracefully. Never crashes on any input.

    Returns a conversational response string, or None only as absolute last resort.
    """
    try:
        q = (question or "").lower().strip()
        q_original = (question or "").strip()
    except Exception:
        return None

    if not q or len(q) < 2:
        return f"Could you please ask a specific question about your {_get_company_name()} financial data? For example: 'Am I making profit?', 'Who owes me the most?', or 'Show me October sales'."

    try:
        conn = get_conn()
    except Exception:
        return "Database is not available. Please ensure a Tally database is loaded."

    try:
        # ════════════════════════════════════════════════════════════════
        # STAGE 0: SAFETY, OUT-OF-SCOPE, CAPABILITIES
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["delete", "drop table", "update ", "insert ", "create ledger",
                                   "modify", "alter table", "truncate"]):
            return "I'm a **read-only** assistant. I cannot modify your Tally data. Please make changes directly in TallyPrime."

        if any(kw in q for kw in ["weather", "cricket", "movie", "recipe", "news",
                                   "stock market", "share price", "nifty", "sensex",
                                   "bitcoin", "crypto", "sports", "politics"]):
            return (f"I'm your financial assistant for **{_get_company_name()}**. I specialize in your Tally accounting data — "
                    "P&L, Balance Sheet, ledgers, debtors, creditors, GST, and business analytics. "
                    "Ask me anything about your financials!")

        if any(kw in q for kw in ["what can you do", "capabilities", "what do you do",
                                   "what are your feature", "help me understand"]):
            return (f"I'm your intelligent Tally assistant for **{_get_company_name()}**. Here's what I can help with:\n\n"
                    "**Financial Reports:**\n"
                    "- P&L, Balance Sheet, Trial Balance\n"
                    "- Monthly sales/purchase trends\n\n"
                    "**Party Analysis:**\n"
                    "- Who owes you money? How much does a specific party owe?\n"
                    "- Top customers, top suppliers, party-wise ledgers\n\n"
                    "**Cash & Bank:**\n"
                    "- Bank balances, cash position\n"
                    "- Monthly cash flow analysis\n\n"
                    "**Ratios & Analytics:**\n"
                    "- Current ratio, gross/net margin, debtor/creditor days\n"
                    "- EBITDA, working capital, business health score\n\n"
                    "**GST:**\n"
                    "- Monthly GST summary, output vs input tax\n\n"
                    "**Smart Questions:**\n"
                    "- 'How were sales in October?'\n"
                    "- 'Which was my best month?'\n"
                    "- 'Compare October and November sales'\n"
                    "- 'Any red flags in my business?'\n"
                    "- 'How much does XYZ owe me?'\n"
                    "- 'Show me customers owing more than 1 lakh'\n\n"
                    "Just ask naturally — I'll figure out the rest!")

        # ════════════════════════════════════════════════════════════════
        # STAGE 1: BUSINESS SUMMARY / OVERVIEW
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["tell me about my business", "business overview",
                                   "give me a summary", "overall summary", "quick summary",
                                   "summarize my business", "summarise my business",
                                   "how is everything", "what does my data say",
                                   "dashboard", "snapshot", "high level summary"]):
            pl = profit_and_loss(conn)
            from analytics import monthly_sales, working_capital_analysis, key_ratios
            ratios = key_ratios(conn)
            wc = working_capital_analysis(conn)
            ms = monthly_sales(conn)
            total_sales = sum(r[2] for r in ms) if ms else 0
            total_invoices = sum(r[1] for r in ms) if ms else 0
            debtors = debtor_aging(conn)
            total_debtor_bal = sum(b for _, b in debtors)
            creditors = creditor_aging(conn)
            total_creditor_bal = sum(b for _, b in creditors)

            np_ = pl["net_profit"]
            status_emoji = "Profitable" if np_ >= 0 else "In Loss"
            margin = (np_ / pl["total_income"] * 100) if pl["total_income"] else 0

            lines = [f"**{_get_company_name()} — Business Summary **\n"]
            lines.append(f"**Revenue:** {_fmt_indian(pl['total_income'])} across {total_invoices} invoices")
            lines.append(f"**Gross Profit:** {_fmt_indian(pl['gross_profit'])} ({ratios['gross_profit_margin']:.1f}% margin)")
            lines.append(f"**Net Profit:** {_fmt_indian(np_)} ({margin:.1f}% margin) — **{status_emoji}**\n")
            lines.append(f"**Working Capital:** {_fmt_indian(wc['working_capital'])} | Current Ratio: {wc['current_ratio']:.2f}")
            lines.append(f"**Outstanding Receivables:** {_fmt_indian(total_debtor_bal)} from {len(debtors)} parties")
            lines.append(f"**Outstanding Payables:** {_fmt_indian(total_creditor_bal)} to {len(creditors)} suppliers")
            lines.append(f"**Debtor Days:** {ratios['debtor_days']:.0f} | **Creditor Days:** {ratios['creditor_days']:.0f}\n")

            # Quick trend
            if len(ms) >= 2:
                first_half = ms[:len(ms)//2]
                second_half = ms[len(ms)//2:]
                avg_first = sum(r[2] for r in first_half) / len(first_half)
                avg_second = sum(r[2] for r in second_half) / len(second_half)
                trend = "upward" if avg_second > avg_first else "downward"
                lines.append(f"**Sales Trend:** {trend.capitalize()} (avg monthly moved from {_fmt_indian(avg_first)} to {_fmt_indian(avg_second)})")

            return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 2: PROFIT / LOSS / MARGIN QUESTIONS
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["net profit", "am i making profit", "am i profitable",
                                   "are we profitable", "making money", "bottom line",
                                   "net income", "net loss", "are we in profit",
                                   "am i in profit", "am i in loss", "are we in loss",
                                   "final profit", "overall profit"]):
            pl = profit_and_loss(conn)
            np_ = pl["net_profit"]
            ti = pl["total_income"]
            te = pl["total_expense"]
            gp = pl["gross_profit"]
            margin = (np_ / ti * 100) if ti else 0
            gp_margin = (gp / ti * 100) if ti else 0
            status = "profit" if np_ >= 0 else "loss"
            lines = [f"Based on your current FY data, {_get_company_name()} shows a **net {status} of {_fmt_indian(np_)}** (margin: {margin:.1f}%).\n"]
            lines.append(f"- **Total Income:** {_fmt_indian(ti)}")
            lines.append(f"- **Total Expenses:** {_fmt_indian(te)}")
            lines.append(f"- **Gross Profit:** {_fmt_indian(gp)} ({gp_margin:.1f}% margin)")
            lines.append(f"- **Net Profit:** {_fmt_indian(np_)} ({margin:.1f}% margin)\n")
            if np_ < 0:
                lines.append("**Concern:** The company is in a loss position. Expenses are exceeding income. Consider reviewing your purchase costs and indirect expenses.")
            elif margin < 2:
                lines.append("**Note:** Margins are thin for a pharma distributor. Consider negotiating better purchase rates or reducing indirect expenses.")
            else:
                lines.append("**Status:** The company is profitable. Keep monitoring margins month-on-month to ensure stability.")
            return "\n".join(lines)

        if any(kw in q for kw in ["gross profit", "gross margin", "trading profit"]):
            pl = profit_and_loss(conn)
            gp = pl["gross_profit"]
            ti = pl["total_income"]
            margin = (gp / ti * 100) if ti else 0
            # Calculate COGS from direct expenses
            direct_exp = sum(abs(amt) for g, entries in pl.get("expense", {}).items()
                           if g in ("Purchase Accounts", "Direct Expenses")
                           for _, amt in entries)
            lines = [f"**Gross Profit: {_fmt_indian(gp)}** (Margin: {margin:.1f}%)\n"]
            lines.append(f"- Revenue: {_fmt_indian(ti)}")
            lines.append(f"- Cost of Goods (Purchases + Direct Expenses): {_fmt_indian(direct_exp)}\n")
            if margin > 8:
                lines.append("Good gross margin for pharmaceutical distribution.")
            elif margin > 4:
                lines.append("Margins are acceptable but could be improved. Consider negotiating better rates with suppliers.")
            else:
                lines.append("**Warning:** Gross margins are thin. This needs immediate attention — review purchase pricing and sales mix.")
            return "\n".join(lines)

        if any(kw in q for kw in ["operating margin", "operating profit", "ebitda",
                                   "earnings before"]):
            pl = profit_and_loss(conn)
            gp = pl["gross_profit"]
            ti = pl["total_income"]
            # Get indirect expenses (excluding depreciation and interest for EBITDA)
            indirect_total = sum(abs(amt) for g, entries in pl.get("expense", {}).items()
                                if "Indirect" in g for _, amt in entries)
            depr = 0
            interest = 0
            for g, entries in pl.get("expense", {}).items():
                for ledger, amt in entries:
                    if "depreciation" in ledger.lower():
                        depr += abs(amt)
                    if "interest" in ledger.lower():
                        interest += abs(amt)
            operating_profit = gp - indirect_total
            ebitda = operating_profit + depr + interest
            op_margin = (operating_profit / ti * 100) if ti else 0
            ebitda_margin = (ebitda / ti * 100) if ti else 0
            lines = [f"Based on your current FY data:\n"]
            lines.append(f"- **Revenue:** {_fmt_indian(ti)}")
            lines.append(f"- **Gross Profit:** {_fmt_indian(gp)}")
            lines.append(f"- **Indirect Expenses:** {_fmt_indian(indirect_total)}")
            lines.append(f"- **Operating Profit (EBIT):** {_fmt_indian(operating_profit)} ({op_margin:.1f}%)")
            lines.append(f"- **EBITDA:** {_fmt_indian(ebitda)} ({ebitda_margin:.1f}%)")
            lines.append(f"  - Add back: Depreciation {_fmt_indian(depr)}, Interest {_fmt_indian(interest)}")
            return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 3: TOP EXPENSES
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["top expense", "top 5 expense", "biggest expense",
                                   "highest expense", "where am i spending",
                                   "major expense", "main expense", "expense breakup",
                                   "expense breakdown", "where is money going",
                                   "cost breakup", "cost breakdown"]):
            pl = profit_and_loss(conn)
            all_expenses = []
            for group, entries in pl.get("expense", {}).items():
                for ledger, amt in entries:
                    all_expenses.append((ledger, abs(amt), group))
            all_expenses.sort(key=lambda x: x[1], reverse=True)
            total_exp = sum(e[1] for e in all_expenses)
            lines = ["**Top Expenses :**\n"]
            for i, (name, amt, group) in enumerate(all_expenses[:10], 1):
                pct = (amt / total_exp * 100) if total_exp else 0
                lines.append(f"{i}. **{name}** ({group}): {_fmt_indian(amt)} — {pct:.1f}% of total")
            lines.append(f"\n**Total Expenses:** {_fmt_indian(total_exp)}")
            # Insight
            if all_expenses:
                top_name = all_expenses[0][0]
                top_pct = (all_expenses[0][1] / total_exp * 100) if total_exp else 0
                lines.append(f"\n**Insight:** {top_name} is your largest expense at {top_pct:.1f}% of total. Monitor this closely.")
            return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 4: WORKING CAPITAL / CURRENT RATIO / LIQUIDITY
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["current ratio", "working capital", "liquidity",
                                   "quick ratio", "acid test"]):
            from analytics import working_capital_analysis
            wc = working_capital_analysis(conn)
            cr = wc["current_ratio"]
            health = "Healthy" if cr > 1.5 else ("Tight" if cr > 1 else "Deficit — liabilities exceed current assets")
            lines = [f"**Working Capital Analysis ({_get_company_name()})**\n"]
            lines.append(f"**Current Ratio: {cr:.2f}** — {health}\n")
            lines.append(f"**Working Capital: {_fmt_indian(wc['working_capital'])}**\n")
            lines.append("**Current Assets:**")
            for g, v in wc["current_assets"].items():
                lines.append(f"  - {g}: {_fmt_indian(v)}")
            lines.append(f"  - **Total: {_fmt_indian(wc['total_ca'])}**\n")
            lines.append("**Current Liabilities:**")
            for g, v in wc["current_liabilities"].items():
                lines.append(f"  - {g}: {_fmt_indian(v)}")
            lines.append(f"  - **Total: {_fmt_indian(wc['total_cl'])}**\n")
            if cr < 1:
                lines.append("**Recommendation:** Current ratio below 1 is concerning. You may face difficulty meeting short-term obligations. Focus on collecting receivables faster.")
            elif cr < 1.5:
                lines.append("**Recommendation:** Ratio is adequate but tight. Maintain healthy collection cycles and avoid overextending credit.")
            else:
                lines.append("**Assessment:** Healthy liquidity position. The business can comfortably meet its short-term obligations.")
            return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 5: BANK / CASH BALANCE
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["bank balance", "money in bank", "cash in bank",
                                   "how much cash", "money in my bank", "cash balance",
                                   "cash position", "cash available", "bank position",
                                   "cash in hand", "cash on hand"]):
            _bk2_ph, _bk2_g = _nature_ph_bank_cash(conn)
            rows = conn.execute(f"""
                SELECT NAME, PARENT, CAST({_bal_col(conn)} AS REAL) as bal
                FROM mst_ledger
                WHERE PARENT IN ({_bk2_ph})
                ORDER BY ABS(CAST({_bal_col(conn)} AS REAL)) DESC
            """, _bk2_g).fetchall()
            total = sum(abs(r[2] or 0) for r in rows)
            lines = [f"**Cash & Bank Position ({_get_company_name()})**\n"]
            lines.append(f"**Total Available: {_fmt_indian(total)}**\n")
            for name, parent, bal in rows:
                balance = abs(bal or 0)
                lines.append(f"- **{name}** ({parent}): {_fmt_indian(balance)}")
            return "\n".join(lines)

        # Specific bank account query
        bank_keywords = ["hdfc", "axis", "idfc", "indusind", "sbi", "icici", "kotak",
                         "bob", "pnb", "canara", "union"]
        matched_bank = None
        for bk in bank_keywords:
            if bk in q:
                matched_bank = bk
                break
        if matched_bank and any(kw in q for kw in ["balance", "account", "bank", "statement",
                                                     "transaction", "how much"]):
            match = _fuzzy_match_party(conn, matched_bank)
            if match and match[1] in ('Bank Accounts', 'Bank OD A/c'):
                name, parent, bal = match
                lines = [f"**{name}**\n"]
                lines.append(f"- Balance: {_fmt_indian(abs(bal or 0))}")
                lines.append(f"- Type: {parent}\n")
                # Get recent transactions
                recent = conn.execute("""
                    SELECT v.DATE, v.VOUCHERTYPENAME, v.PARTYLEDGERNAME,
                           CAST(a.AMOUNT AS REAL) as amt
                    FROM trn_accounting a JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                    WHERE a.LEDGERNAME = ? ORDER BY v.DATE DESC LIMIT 5
                """, (name,)).fetchall()
                if recent:
                    lines.append("**Recent Transactions:**")
                    for dt, vtype, party, amt in recent:
                        dt_fmt = f"{dt[6:8]}/{dt[4:6]}/{dt[:4]}" if dt and len(dt) == 8 else dt
                        dr_cr = "Dr" if (amt or 0) < 0 else "Cr"
                        lines.append(f"  - {dt_fmt} | {vtype} | {party or '-'} | {_fmt_indian(abs(amt or 0))} {dr_cr}")
                return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 6: GST QUERIES
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["gst", "gstr", "output tax", "input tax", "itc",
                                   "tax liability", "gst return", "cgst", "sgst", "igst",
                                   "input credit", "tax payable", "gst payable",
                                   "gst refund", "gst refundable", "net tax"]):
            try:
                from gst_engine import gst_monthly_comparison
                monthly = gst_monthly_comparison(conn)
                if monthly:
                    lines = [f"**GST Monthly Summary ({_get_company_name()})**\n"]
                    lines.append(f"{'Month':<12} | {'Output Tax':>12} | {'Input Tax':>12} | {'Net':>12} | Status")
                    lines.append("-" * 68)
                    total_out, total_inp, total_net = 0, 0, 0
                    worst_m, worst_net = None, float('-inf')
                    best_m, best_net = None, float('inf')
                    for m in monthly:
                        ml = m.get('month_label', m.get('month', ''))
                        out = m.get('output_tax', 0)
                        inp = m.get('input_tax', 0)
                        net = out - inp
                        total_out += out
                        total_inp += inp
                        total_net += net
                        status = "Payable" if net > 0 else "Refundable"
                        lines.append(f"{ml:<12} | {_fmt_exact(out):>12} | {_fmt_exact(inp):>12} | {_fmt_exact(net):>12} | {status}")
                        if net > worst_net: worst_net, worst_m = net, ml
                        if net < best_net: best_net, best_m = net, ml
                    lines.append("-" * 68)
                    lines.append(f"{'TOTAL':<12} | {_fmt_exact(total_out):>12} | {_fmt_exact(total_inp):>12} | {_fmt_exact(total_net):>12} |")
                    lines.append(f"\n**Highest liability month:** {worst_m} ({_fmt_indian(worst_net)})")
                    if best_net < 0:
                        lines.append(f"**Most ITC surplus:** {best_m} ({_fmt_indian(abs(best_net))} refundable)")
                    return "\n".join(lines)
            except Exception:
                pass
            return "GST data is available. Navigate to the **GST Returns** tab for detailed GSTR-1 and GSTR-3B."

        # ════════════════════════════════════════════════════════════════
        # STAGE 7: DEBTOR / CREDITOR DAYS & TURNOVER
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["debtor days", "creditor days", "collection period",
                                   "payment period", "debtor turnover", "creditor turnover",
                                   "receivable days", "payable days", "dso", "dpo",
                                   "days sales outstanding", "days payable"]):
            from analytics import key_ratios
            ratios = key_ratios(conn)
            dd = ratios['debtor_days']
            cd = ratios['creditor_days']
            lines = [f"**Debtor & Creditor Turnover Analysis**\n"]
            lines.append(f"**Debtor Days (DSO): {dd:.0f} days** — {'Good collection cycle' if dd < 45 else ('Acceptable' if dd < 75 else 'High — push collections aggressively')}")
            lines.append(f"**Creditor Days (DPO): {cd:.0f} days** — {'Good payment cycle' if cd < 60 else ('Acceptable' if cd < 90 else 'High — may affect supplier relationships')}\n")
            lines.append(f"**Total Outstanding Debtors:** {_fmt_indian(ratios['total_debtors'])}")
            lines.append(f"**Total Outstanding Creditors:** {_fmt_indian(ratios['total_creditors'])}\n")
            cash_cycle = dd - cd
            lines.append(f"**Cash Conversion Cycle:** {cash_cycle:.0f} days")
            if cash_cycle > 0:
                lines.append(f"You're funding {cash_cycle:.0f} days of working capital gap. Consider tightening collections or negotiating longer payment terms.")
            else:
                lines.append(f"Positive sign — you're collecting faster than you're paying.")
            return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 8: FINANCIAL RATIOS
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["financial ratio", "key ratio", "ratio analysis",
                                   "all ratio", "business ratio", "important ratio",
                                   "return on equity", "roe", "return on asset", "roa",
                                   "return on capital", "roce", "debt to equity",
                                   "debt equity ratio", "proprietary ratio",
                                   "inventory turnover", "stock turnover"]):
            from analytics import key_ratios, working_capital_analysis
            ratios = key_ratios(conn)
            wc = working_capital_analysis(conn)
            pl = profit_and_loss(conn)
            bs = balance_sheet(conn)

            lines = [f"**Key Financial Ratios — {_get_company_name()} **\n"]
            lines.append("**Profitability:**")
            lines.append(f"  - Gross Profit Margin: {ratios['gross_profit_margin']:.1f}%")
            lines.append(f"  - Net Profit Margin: {ratios['net_profit_margin']:.1f}%")
            lines.append(f"  - Return on Assets: {ratios['roa']:.1f}%\n")
            lines.append("**Liquidity:**")
            lines.append(f"  - Current Ratio: {wc['current_ratio']:.2f}")
            lines.append(f"  - Working Capital: {_fmt_indian(wc['working_capital'])}\n")
            lines.append("**Efficiency:**")
            lines.append(f"  - Debtor Days: {ratios['debtor_days']:.0f}")
            lines.append(f"  - Creditor Days: {ratios['creditor_days']:.0f}")
            lines.append(f"  - Cash Cycle: {ratios['debtor_days'] - ratios['creditor_days']:.0f} days\n")

            # Debt-to-equity
            total_debt = 0
            for g, entries in bs.get("liabilities", {}).items():
                if "loan" in g.lower() or "secured" in g.lower() or "unsecured" in g.lower():
                    total_debt += sum(abs(b) for _, b in entries)
            equity = 0
            for g, entries in bs.get("liabilities", {}).items():
                if "capital" in g.lower():
                    equity += sum(abs(b) for _, b in entries)
            de_ratio = (total_debt / equity) if equity > 0 else 0
            lines.append("**Solvency:**")
            lines.append(f"  - Total Debt: {_fmt_indian(total_debt)}")
            lines.append(f"  - Equity: {_fmt_indian(equity)}")
            lines.append(f"  - Debt-to-Equity Ratio: {de_ratio:.2f}")
            return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 9: BUSINESS HEALTH / RED FLAGS / RECOMMENDATIONS
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["business health", "is my business healthy",
                                   "financial health", "how is my business",
                                   "health check", "health score"]):
            from analytics import key_ratios, working_capital_analysis
            ratios = key_ratios(conn)
            wc = working_capital_analysis(conn)
            pl = profit_and_loss(conn)

            score = 0
            good = []
            issues = []
            if ratios["gross_profit_margin"] > 5:
                score += 20
                good.append(f"Healthy gross margin ({ratios['gross_profit_margin']:.1f}%)")
            else:
                issues.append(f"Low gross margin ({ratios['gross_profit_margin']:.1f}%)")
            if ratios["net_profit_margin"] > 0:
                score += 20
                good.append("Business is profitable")
            else:
                issues.append("Net loss position — expenses exceed income")
            if wc["current_ratio"] > 1:
                score += 20
                good.append(f"Adequate liquidity (CR: {wc['current_ratio']:.2f})")
            else:
                issues.append(f"Working capital deficit (CR: {wc['current_ratio']:.2f})")
            if ratios["debtor_days"] < 90:
                score += 20
                good.append(f"Reasonable collection cycle ({ratios['debtor_days']:.0f} days)")
            else:
                issues.append(f"High debtor days ({ratios['debtor_days']:.0f}) — money stuck with customers")
            if ratios["creditor_days"] < 120:
                score += 20
                good.append(f"Payment cycle under control ({ratios['creditor_days']:.0f} days)")
            else:
                issues.append(f"Very high creditor days ({ratios['creditor_days']:.0f}) — may hurt supplier relations")

            health = "Strong" if score >= 80 else ("Moderate" if score >= 60 else "Needs Attention")
            lines = [f"**Financial Health Score: {score}/100 — {health}**\n"]
            lines.append(f"**Revenue:** {_fmt_indian(pl['total_income'])}")
            lines.append(f"**Net Result:** {_fmt_indian(pl['net_profit'])}\n")
            if good:
                lines.append("**Strengths:**")
                for g in good:
                    lines.append(f"  - {g}")
            if issues:
                lines.append("\n**Areas of Concern:**")
                for i in issues:
                    lines.append(f"  - {i}")
            return "\n".join(lines)

        if any(kw in q for kw in ["red flag", "warning", "risk", "concern",
                                   "problem", "issue in my business", "any issue",
                                   "anything wrong", "what should i worry"]):
            from analytics import key_ratios, working_capital_analysis, collection_efficiency
            ratios = key_ratios(conn)
            wc = working_capital_analysis(conn)
            pl = profit_and_loss(conn)
            flags = []

            if ratios["net_profit_margin"] < 0:
                flags.append(f"**Net Loss:** You're operating at a loss ({ratios['net_profit_margin']:.1f}% margin). Expenses exceed revenue.")
            if ratios["gross_profit_margin"] < 3:
                flags.append(f"**Thin Margins:** Gross margin at {ratios['gross_profit_margin']:.1f}% is dangerously low for pharma distribution.")
            if wc["current_ratio"] < 1:
                flags.append(f"**Liquidity Crisis:** Current ratio {wc['current_ratio']:.2f} means current liabilities exceed current assets.")
            if ratios["debtor_days"] > 60:
                flags.append(f"**Slow Collections:** Debtor days at {ratios['debtor_days']:.0f} — money is stuck with customers too long.")
            if ratios["total_debtors"] > pl["total_income"] * 0.3:
                flags.append(f"**High Receivables:** Outstanding debtors ({_fmt_indian(ratios['total_debtors'])}) are {ratios['total_debtors']/pl['total_income']*100:.0f}% of revenue. Concentration risk.")

            # Check for large individual debtor concentration
            debtors = debtor_aging(conn)
            if debtors:
                total_d = sum(b for _, b in debtors)
                top_debtor = max(debtors, key=lambda x: x[1])
                if total_d > 0 and top_debtor[1] / total_d > 0.25:
                    flags.append(f"**Debtor Concentration:** {top_debtor[0]} owes {_fmt_indian(top_debtor[1])} — that's {top_debtor[1]/total_d*100:.0f}% of all receivables.")

            if not flags:
                return ("**No major red flags found!** Your business looks reasonably healthy.\n\n"
                        f"- GP Margin: {ratios['gross_profit_margin']:.1f}%\n"
                        f"- NP Margin: {ratios['net_profit_margin']:.1f}%\n"
                        f"- Current Ratio: {wc['current_ratio']:.2f}\n"
                        f"- Debtor Days: {ratios['debtor_days']:.0f}\n\n"
                        "Keep monitoring these monthly.")

            lines = [f"**Red Flags Found: {len(flags)}**\n"]
            for i, f in enumerate(flags, 1):
                lines.append(f"{i}. {f}")
            lines.append("\n**Recommendation:** Address these issues in order of priority. The first items are most critical.")
            return "\n".join(lines)

        if any(kw in q for kw in ["how can i improve", "what should i focus",
                                   "advice", "recommendation", "suggest",
                                   "how to improve", "improve my business",
                                   "what to do", "next steps"]):
            from analytics import key_ratios, working_capital_analysis
            ratios = key_ratios(conn)
            wc = working_capital_analysis(conn)
            pl = profit_and_loss(conn)
            recommendations = []

            if ratios["gross_profit_margin"] < 8:
                recommendations.append("**Improve Gross Margins:** Negotiate better purchase rates with suppliers or explore alternative suppliers. Even a 1% improvement on pharma distribution volumes makes a big difference.")
            if ratios["debtor_days"] > 45:
                recommendations.append(f"**Speed Up Collections:** Your debtors are paying in ~{ratios['debtor_days']:.0f} days. Set a 30-day credit policy and follow up rigorously on overdue accounts.")
            if wc["current_ratio"] < 1.5:
                recommendations.append("**Improve Working Capital:** Focus on collecting receivables faster and negotiating longer payment terms with suppliers.")

            # Check indirect expenses
            indirect_total = sum(abs(amt) for g, entries in pl.get("expense", {}).items()
                                if "Indirect" in g for _, amt in entries)
            if indirect_total > 0 and pl["total_income"] > 0 and (indirect_total / pl["total_income"] * 100) > 5:
                recommendations.append(f"**Control Overhead:** Indirect expenses are {indirect_total/pl['total_income']*100:.1f}% of revenue. Review each expense head for optimization opportunities.")

            recommendations.append("**Monitor Monthly:** Track sales, gross margin, and collections monthly. Identify trends early before they become problems.")

            if not recommendations:
                recommendations.append("Your business metrics look good! Maintain current practices and keep a close watch on cash flow.")

            lines = [f"**Recommendations for {_get_company_name()}:**\n"]
            for i, r in enumerate(recommendations, 1):
                lines.append(f"{i}. {r}\n")
            return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 10: SALES TREND / MONTHLY SALES
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["sales trend", "revenue trend", "monthly sales",
                                   "sales this year", "total sales", "sales overview",
                                   "sales growth", "revenue growth", "turnover trend",
                                   "total revenue", "total turnover"]):
            from analytics import monthly_sales
            data = monthly_sales(conn)
            total = sum(r[2] for r in data) if data else 0
            total_inv = sum(r[1] for r in data) if data else 0
            lines = [f"**Monthly Sales Trend — {_get_company_name()} **\n"]
            lines.append(f"**Total Revenue: {_fmt_indian(total)}** | **Invoices: {total_inv}**\n")
            prev = None
            best_month = max(data, key=lambda x: x[2]) if data else None
            worst_month = min(data, key=lambda x: x[2]) if data else None
            for month, count, amt in data:
                change = ""
                if prev:
                    pct = ((amt - prev) / prev * 100) if prev else 0
                    arrow = "+" if pct > 0 else ""
                    change = f" ({arrow}{pct:.1f}%)"
                marker = " **<-- Best**" if best_month and month == best_month[0] else (" **<-- Lowest**" if worst_month and month == worst_month[0] else "")
                lines.append(f"- **{_month_label(month)}:** {_fmt_indian(amt)} ({count} inv){change}{marker}")
                prev = amt

            if data and len(data) >= 2:
                avg = total / len(data)
                lines.append(f"\n**Avg Monthly Sales:** {_fmt_indian(avg)}")
                first_half_avg = sum(r[2] for r in data[:len(data)//2]) / max(len(data)//2, 1)
                second_half_avg = sum(r[2] for r in data[len(data)//2:]) / max(len(data) - len(data)//2, 1)
                if second_half_avg > first_half_avg:
                    lines.append(f"**Trend:** Sales are **increasing** — recent months averaging {_fmt_indian(second_half_avg)} vs earlier {_fmt_indian(first_half_avg)}.")
                else:
                    lines.append(f"**Trend:** Sales are **decreasing** — recent months averaging {_fmt_indian(second_half_avg)} vs earlier {_fmt_indian(first_half_avg)}.")
            return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 11: PURCHASE TREND
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["purchase trend", "monthly purchase", "purchase this year",
                                   "total purchase", "purchase overview", "purchase growth",
                                   "buying trend", "procurement trend"]):
            from analytics import monthly_purchases
            data = monthly_purchases(conn)
            total = sum(r[2] for r in data) if data else 0
            lines = [f"**Monthly Purchase Trend — {_get_company_name()} **\n"]
            lines.append(f"**Total Purchases: {_fmt_indian(total)}**\n")
            prev = None
            for month, count, amt in data:
                change = ""
                if prev:
                    pct = ((amt - prev) / prev * 100) if prev else 0
                    change = f" ({'+' if pct > 0 else ''}{pct:.1f}%)"
                lines.append(f"- **{_month_label(month)}:** {_fmt_indian(amt)} ({count} bills){change}")
                prev = amt
            return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 12: MONTH COMPARISON (must come before single-month)
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["compare", "comparison", "versus", " vs ", " and "
                                   ]) and sum(1 for name in MONTH_MAP if name in q) >= 2:
            (m1_code, m1_name), (m2_code, m2_name) = _extract_two_months(q)
            if m1_code and m2_code:
                from analytics import monthly_sales, monthly_purchases
                sales_data = {r[0]: (r[1], r[2]) for r in monthly_sales(conn)}
                purchase_data = {r[0]: (r[1], r[2]) for r in monthly_purchases(conn)}

                s1_count, s1_amt = sales_data.get(m1_code, (0, 0))
                s2_count, s2_amt = sales_data.get(m2_code, (0, 0))
                p1_count, p1_amt = purchase_data.get(m1_code, (0, 0))
                p2_count, p2_amt = purchase_data.get(m2_code, (0, 0))
                gp1, gp2 = s1_amt - p1_amt, s2_amt - p2_amt

                lines = [f"**{_month_full(m1_code)} vs {_month_full(m2_code)} Comparison:**\n"]
                lines.append(f"| Metric | {_month_full(m1_code)} | {_month_full(m2_code)} | Change |")
                lines.append(f"|--------|-------|-------|--------|")

                s_change = ((s2_amt - s1_amt) / s1_amt * 100) if s1_amt else 0
                p_change = ((p2_amt - p1_amt) / p1_amt * 100) if p1_amt else 0
                gp_change = ((gp2 - gp1) / gp1 * 100) if gp1 else 0

                lines.append(f"| Sales | {_fmt_indian(s1_amt)} | {_fmt_indian(s2_amt)} | {'+' if s_change >= 0 else ''}{s_change:.1f}% |")
                lines.append(f"| Purchases | {_fmt_indian(p1_amt)} | {_fmt_indian(p2_amt)} | {'+' if p_change >= 0 else ''}{p_change:.1f}% |")
                lines.append(f"| Gross Profit | {_fmt_indian(gp1)} | {_fmt_indian(gp2)} | {'+' if gp_change >= 0 else ''}{gp_change:.1f}% |")
                lines.append(f"| Invoices | {s1_count} | {s2_count} | {'+' if s2_count >= s1_count else ''}{s2_count - s1_count} |")

                winner = _month_full(m2_code) if s2_amt > s1_amt else _month_full(m1_code)
                lines.append(f"\n**{winner}** had higher sales.")
                return "\n".join(lines)

        # STAGE 12b: MONTH-SPECIFIC QUESTIONS (How were sales in October?)
        # ════════════════════════════════════════════════════════════════

        month_code = _extract_month_code(q)
        if month_code and any(kw in q for kw in ["sales", "revenue", "turnover", "how were",
                                                   "how was", "how did"]):
            from analytics import monthly_sales, monthly_purchases
            sales_data = {r[0]: (r[1], r[2]) for r in monthly_sales(conn)}
            purchase_data = {r[0]: (r[1], r[2]) for r in monthly_purchases(conn)}
            month_name = _month_full(month_code)

            if month_code in sales_data:
                s_count, s_amt = sales_data[month_code]
                p_count, p_amt = purchase_data.get(month_code, (0, 0))
                gp = s_amt - p_amt
                gp_margin = (gp / s_amt * 100) if s_amt else 0

                # Compare with previous month
                all_months = sorted(sales_data.keys())
                idx = all_months.index(month_code) if month_code in all_months else -1
                comparison = ""
                if idx > 0:
                    prev_code = all_months[idx - 1]
                    prev_amt = sales_data[prev_code][1]
                    change_pct = ((s_amt - prev_amt) / prev_amt * 100) if prev_amt else 0
                    direction = "up" if change_pct > 0 else "down"
                    comparison = f"\n**vs {_month_full(prev_code)}:** {direction} {abs(change_pct):.1f}%"

                lines = [f"**{month_name} Performance:**\n"]
                lines.append(f"- **Sales:** {_fmt_indian(s_amt)} ({s_count} invoices)")
                lines.append(f"- **Purchases:** {_fmt_indian(p_amt)} ({p_count} bills)")
                lines.append(f"- **Gross Profit:** {_fmt_indian(gp)} ({gp_margin:.1f}% margin){comparison}")
                return "\n".join(lines)
            else:
                return f"No data available for **{month_name}**. Available data is from April 2025 to January 2026."

        # Month-specific expenses
        if month_code and any(kw in q for kw in ["expense", "spend", "cost"]):
            month_name = _month_full(month_code)
            _ie_ph, _ie_g = _nature_ph(conn, 'indirect_expense')
            rows = conn.execute(f"""
                SELECT a.LEDGERNAME, ABS(SUM(CAST(a.AMOUNT AS REAL))) as amt
                FROM trn_voucher v
                JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
                JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
                WHERE l.PARENT IN ({_ie_ph}) AND CAST(a.AMOUNT AS REAL) < 0
                  AND SUBSTR(v.DATE,1,6) = ?
                GROUP BY a.LEDGERNAME ORDER BY amt DESC
            """, _ie_g + [month_code]).fetchall()
            if rows:
                total = sum(r[1] for r in rows)
                lines = [f"**Expenses in {month_name}:**\n"]
                for name, amt in rows:
                    lines.append(f"- {name}: {_fmt_indian(amt)}")
                lines.append(f"\n**Total Indirect Expenses:** {_fmt_indian(total)}")
                return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 14: BEST / WORST MONTH
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["best month", "highest month", "peak month",
                                   "strongest month", "best performing"]):
            from analytics import monthly_sales, monthly_gross_profit
            gp_data = monthly_gross_profit(conn)
            if gp_data:
                by_sales = max(gp_data, key=lambda x: x["sales"])
                by_profit = max(gp_data, key=lambda x: x["net_profit"])
                lines = ["**Best Performing Months :**\n"]
                lines.append(f"**Highest Sales:** {_month_full(by_sales['month'])} — {_fmt_indian(by_sales['sales'])} (GP: {by_sales['gp_margin']:.1f}%)")
                lines.append(f"**Highest Net Profit:** {_month_full(by_profit['month'])} — {_fmt_indian(by_profit['net_profit'])} (NP: {by_profit['np_margin']:.1f}%)\n")
                lines.append("**Monthly Ranking by Sales:**")
                ranked = sorted(gp_data, key=lambda x: x["sales"], reverse=True)
                for i, m in enumerate(ranked, 1):
                    lines.append(f"  {i}. {_month_full(m['month'])}: {_fmt_indian(m['sales'])}")
                return "\n".join(lines)

        if any(kw in q for kw in ["worst month", "lowest month", "weakest month",
                                   "worst performing", "slowest month"]):
            from analytics import monthly_gross_profit
            gp_data = monthly_gross_profit(conn)
            if gp_data:
                by_sales = min(gp_data, key=lambda x: x["sales"])
                by_profit = min(gp_data, key=lambda x: x["net_profit"])
                lines = ["**Weakest Months :**\n"]
                lines.append(f"**Lowest Sales:** {_month_full(by_sales['month'])} — {_fmt_indian(by_sales['sales'])}")
                lines.append(f"**Lowest Net Profit:** {_month_full(by_profit['month'])} — {_fmt_indian(by_profit['net_profit'])}\n")
                lines.append("**Monthly Ranking (lowest to highest sales):**")
                ranked = sorted(gp_data, key=lambda x: x["sales"])
                for i, m in enumerate(ranked[:5], 1):
                    lines.append(f"  {i}. {_month_full(m['month'])}: {_fmt_indian(m['sales'])} (NP: {_fmt_indian(m['net_profit'])})")
                return "\n".join(lines)

        # Quarter questions
        if any(kw in q for kw in ["quarter", " q1", " q2", " q3", " q4"]):
            q_months, q_label = _extract_quarter(q)
            if q_months:
                from analytics import monthly_sales, monthly_purchases
                sales_data = {r[0]: (r[1], r[2]) for r in monthly_sales(conn)}
                purchase_data = {r[0]: (r[1], r[2]) for r in monthly_purchases(conn)}

                total_sales = sum(sales_data.get(m, (0, 0))[1] for m in q_months)
                total_inv = sum(sales_data.get(m, (0, 0))[0] for m in q_months)
                total_purch = sum(purchase_data.get(m, (0, 0))[1] for m in q_months)
                gp = total_sales - total_purch
                gp_margin = (gp / total_sales * 100) if total_sales else 0

                lines = [f"**{q_label} Performance:**\n"]
                lines.append(f"- **Sales:** {_fmt_indian(total_sales)} ({total_inv} invoices)")
                lines.append(f"- **Purchases:** {_fmt_indian(total_purch)}")
                lines.append(f"- **Gross Profit:** {_fmt_indian(gp)} ({gp_margin:.1f}% margin)\n")
                lines.append("**Month-wise:**")
                for m in q_months:
                    if m in sales_data:
                        lines.append(f"  - {_month_full(m)}: Sales {_fmt_indian(sales_data[m][1])}, Purchases {_fmt_indian(purchase_data.get(m, (0,0))[1])}")
                return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 15: TREND QUESTIONS (increasing/decreasing)
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["increasing", "decreasing", "growing", "declining",
                                   "going up", "going down", "trending",
                                   "is my business growing", "business growing"]):
            from analytics import monthly_sales, monthly_gross_profit
            ms = monthly_sales(conn)
            gp_data = monthly_gross_profit(conn)

            if ms and len(ms) >= 4:
                mid = len(ms) // 2
                first_half_avg = sum(r[2] for r in ms[:mid]) / mid
                second_half_avg = sum(r[2] for r in ms[mid:]) / (len(ms) - mid)
                sales_trend = "INCREASING" if second_half_avg > first_half_avg else "DECREASING"
                sales_change = ((second_half_avg - first_half_avg) / first_half_avg * 100) if first_half_avg else 0

                # Profit trend
                if gp_data and len(gp_data) >= 4:
                    gp_mid = len(gp_data) // 2
                    first_np = sum(m["net_profit"] for m in gp_data[:gp_mid]) / gp_mid
                    second_np = sum(m["net_profit"] for m in gp_data[gp_mid:]) / (len(gp_data) - gp_mid)
                    profit_trend = "IMPROVING" if second_np > first_np else "DECLINING"
                else:
                    profit_trend = "N/A"
                    first_np = second_np = 0

                lines = ["**Business Trend Analysis :**\n"]
                lines.append(f"**Sales Trend: {sales_trend}** ({'+' if sales_change > 0 else ''}{sales_change:.1f}%)")
                lines.append(f"  - First half avg: {_fmt_indian(first_half_avg)}/month")
                lines.append(f"  - Second half avg: {_fmt_indian(second_half_avg)}/month\n")
                lines.append(f"**Profit Trend: {profit_trend}**")
                lines.append(f"  - First half avg NP: {_fmt_indian(first_np)}/month")
                lines.append(f"  - Second half avg NP: {_fmt_indian(second_np)}/month\n")

                if sales_trend == "INCREASING" and profit_trend == "IMPROVING":
                    lines.append("**Assessment:** Your business is growing healthily — both sales and profits are improving.")
                elif sales_trend == "INCREASING" and profit_trend == "DECLINING":
                    lines.append("**Warning:** Sales are growing but profits are declining. This could mean rising costs or margin pressure. Investigate expense trends.")
                elif sales_trend == "DECREASING":
                    lines.append("**Concern:** Sales are declining. Review your customer base and market positioning.")
                return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 16: PARTY-SPECIFIC QUESTIONS
        # ════════════════════════════════════════════════════════════════

        # "How much does X owe me?" / "What does X owe?"
        if any(kw in q for kw in ["owe me", "owes me", "owe us", "owes us",
                                   "outstanding from", "receivable from",
                                   "balance of", "balance for"]):
            entity = _extract_entity_name(q_original) or _extract_entity_name(q)
            if entity:
                match = _fuzzy_match_party(conn, entity)
                if match:
                    name, parent, bal = match
                    bal = bal or 0
                    if parent == "Sundry Debtors":
                        lines = [f"**{name}** (Debtor)\n"]
                        lines.append(f"**Outstanding Balance: {_fmt_indian(abs(bal))}**")
                        if bal < 0:
                            lines.append("This party owes you money.")
                        else:
                            lines.append("No outstanding amount (or you owe them an advance).")
                        # Show recent transactions
                        recent = conn.execute("""
                            SELECT v.DATE, v.VOUCHERTYPENAME, ABS(CAST(a.AMOUNT AS REAL))
                            FROM trn_accounting a JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                            WHERE a.LEDGERNAME = ? ORDER BY v.DATE DESC LIMIT 5
                        """, (name,)).fetchall()
                        if recent:
                            lines.append("\n**Recent Transactions:**")
                            for dt, vtype, amt in recent:
                                dt_fmt = f"{dt[6:8]}/{dt[4:6]}/{dt[:4]}" if dt and len(dt) == 8 else dt
                                lines.append(f"  - {dt_fmt} | {vtype} | {_fmt_indian(amt)}")
                        return "\n".join(lines)
                    elif parent == "Sundry Creditors":
                        return f"**{name}** is under Sundry Creditors (a supplier). Outstanding: {_fmt_indian(abs(bal))}. They don't owe you — you owe them."
                    else:
                        return f"**{name}** ({parent}) — Closing Balance: {_fmt_indian(abs(bal))}"

        # "Who owes me the most?" / Top debtors
        if any(kw in q for kw in ["who owes", "top debtor", "biggest debtor",
                                   "largest receivable", "most money owe",
                                   "highest receivable", "major debtor",
                                   "largest debtor", "biggest receivable"]):
            data = debtor_aging(conn)
            if data:
                total = sum(b for _, b in data)
                sorted_data = sorted(data, key=lambda x: x[1], reverse=True)
                lines = [f"**Top Debtors — {_get_company_name()}**\n"]
                lines.append(f"**Total Outstanding: {_fmt_indian(total)}** from **{len(data)} parties**\n")
                for i, (name, bal) in enumerate(sorted_data[:15], 1):
                    pct = (bal / total * 100) if total else 0
                    lines.append(f"{i}. **{name}**: {_fmt_indian(bal)} ({pct:.1f}%)")
                # Concentration warning
                top3_total = sum(b for _, b in sorted_data[:3])
                top3_pct = (top3_total / total * 100) if total else 0
                lines.append(f"\n**Top 3 concentration:** {top3_pct:.1f}% of total receivables")
                if top3_pct > 50:
                    lines.append("**Warning:** High concentration risk — top 3 debtors hold over half your receivables.")
                return "\n".join(lines)

        # Top creditors / how much do I owe
        if any(kw in q for kw in ["top creditor", "biggest creditor", "largest payable",
                                   "how much do we owe", "how much do i owe",
                                   "major creditor", "major supplier", "whom do i owe",
                                   "who do i owe", "supplier outstanding"]):
            data = creditor_aging(conn)
            if data:
                total = sum(b for _, b in data)
                sorted_data = sorted(data, key=lambda x: x[1], reverse=True)
                lines = [f"**Top Creditors — {_get_company_name()}**\n"]
                lines.append(f"**Total Payable: {_fmt_indian(total)}** to **{len(data)} suppliers**\n")
                for i, (name, bal) in enumerate(sorted_data[:15], 1):
                    pct = (bal / total * 100) if total else 0
                    lines.append(f"{i}. **{name}**: {_fmt_indian(bal)} ({pct:.1f}%)")
                return "\n".join(lines)

        # "Total business with [party]"
        if any(kw in q for kw in ["total business with", "business with", "transactions with",
                                   "dealing with", "volume with"]):
            entity = _extract_entity_name(q_original) or _extract_entity_name(q)
            if entity:
                match = _fuzzy_match_party(conn, entity)
                if match:
                    name = match[0]
                    # Total sales with this party (recursive sub-groups)
                    _sp, _sg = _nature_ph(conn, 'sales')
                    sales_total = conn.execute(f"""
                        SELECT COUNT(DISTINCT v.GUID), COALESCE(ABS(SUM(CAST(a.AMOUNT AS REAL))), 0)
                        FROM trn_voucher v JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
                        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
                        WHERE v.PARTYLEDGERNAME = ? AND l.PARENT IN ({_sp})
                    """, [name] + _sg).fetchone()
                    # Total purchases (recursive sub-groups)
                    _pp, _pg = _nature_ph(conn, 'purchase')
                    purch_total = conn.execute(f"""
                        SELECT COUNT(DISTINCT v.GUID), COALESCE(ABS(SUM(CAST(a.AMOUNT AS REAL))), 0)
                        FROM trn_voucher v JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
                        JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
                        WHERE v.PARTYLEDGERNAME = ? AND l.PARENT IN ({_pp})
                    """, [name] + _pg).fetchone()
                    # Total all vouchers
                    all_vch = conn.execute("""
                        SELECT COUNT(DISTINCT GUID) FROM trn_voucher WHERE PARTYLEDGERNAME = ?
                    """, (name,)).fetchone()

                    lines = [f"**Total Business with {name}:**\n"]
                    if sales_total and sales_total[1] > 0:
                        lines.append(f"- **Sales:** {_fmt_indian(sales_total[1])} ({sales_total[0]} invoices)")
                    if purch_total and purch_total[1] > 0:
                        lines.append(f"- **Purchases:** {_fmt_indian(purch_total[1])} ({purch_total[0]} bills)")
                    lines.append(f"- **Total Vouchers:** {all_vch[0] if all_vch else 0}")
                    lines.append(f"- **Current Balance:** {_fmt_indian(abs(match[2] or 0))} ({match[1]})")
                    return "\n".join(lines)

        # "Show me the ledger of X" / "X ledger"
        if any(kw in q for kw in ["ledger of", "ledger for", "statement of", "statement for",
                                   "show ledger", "show statement", "account of",
                                   "account for", "transactions of", "transactions for"]):
            entity = _extract_entity_name(q_original) or _extract_entity_name(q)
            if entity:
                match = _fuzzy_match_party(conn, entity)
                if match:
                    name, parent, bal = match
                    opening, txns, closing = ledger_detail(conn, name)
                    lines = [f"**Ledger: {name}** ({parent})\n"]
                    lines.append(f"**Opening Balance:** {_fmt_indian(abs(opening))}")
                    lines.append(f"**Closing Balance:** {_fmt_indian(abs(closing))}")
                    lines.append(f"**Total Transactions:** {len(txns)}\n")
                    if txns:
                        lines.append("**Recent Transactions (last 15):**")
                        for txn in txns[-15:]:
                            dt = txn['date']
                            dt_fmt = f"{dt[6:8]}/{dt[4:6]}/{dt[:4]}" if dt and len(dt) == 8 else dt
                            dr = f"Dr {_fmt_indian(txn['debit'])}" if txn['debit'] else ""
                            cr = f"Cr {_fmt_indian(txn['credit'])}" if txn['credit'] else ""
                            lines.append(f"  - {dt_fmt} | {txn['voucher_type']:10s} | {dr}{cr}")
                    return "\n".join(lines)

        # "Oldest debtor" / "longest outstanding"
        if any(kw in q for kw in ["oldest debtor", "longest outstanding",
                                   "oldest receivable", "most overdue"]):
            # Find debtors with oldest last transaction
            debtors = debtor_aging(conn)
            if debtors:
                debtor_ages = []
                for name, bal in debtors:
                    last_txn = conn.execute("""
                        SELECT MAX(v.DATE) FROM trn_voucher v
                        JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
                        WHERE a.LEDGERNAME = ?
                    """, (name,)).fetchone()
                    debtor_ages.append((name, bal, last_txn[0] if last_txn else "19000101"))
                debtor_ages.sort(key=lambda x: x[2])  # oldest first
                lines = ["**Oldest Outstanding Debtors (by last transaction date):**\n"]
                for i, (name, bal, last_date) in enumerate(debtor_ages[:10], 1):
                    dt_fmt = f"{last_date[6:8]}/{last_date[4:6]}/{last_date[:4]}" if last_date and len(last_date) == 8 else last_date
                    lines.append(f"{i}. **{name}**: {_fmt_indian(bal)} (last txn: {dt_fmt})")
                lines.append("\n**Recommendation:** Follow up with the top entries — older outstanding amounts are harder to collect.")
                return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 17: SPECIFIC LEDGER / EXPENSE QUESTIONS
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["rent expense", "how much rent", "rent paid",
                                   "spend on rent"]):
            return _ledger_expense_answer(conn, "Rent")

        if any(kw in q for kw in ["salary expense", "salary paid", "how much salary",
                                   "spend on salary", "salaries paid", "wage"]):
            rows = search_ledger(conn, "salary")
            if rows:
                total = sum(abs(r[2] or 0) for r in rows)
                lines = [f"**Salary / Staff Expenses:**\n"]
                for name, parent, bal in rows:
                    lines.append(f"- **{name}** ({parent}): {_fmt_indian(abs(bal or 0))}")
                lines.append(f"\n**Total: {_fmt_indian(total)}**")
                return "\n".join(lines)

        if any(kw in q for kw in ["electricity", "power", "bijli"]):
            return _ledger_expense_answer(conn, "Electricity")

        if any(kw in q for kw in ["freight", "transport", "shipping", "carriage"]):
            return _ledger_expense_answer(conn, "freight") or _ledger_expense_answer(conn, "transport")

        if any(kw in q for kw in ["insurance expense", "insurance cost", "insurance paid",
                                   "how much insurance"]):
            return _ledger_expense_answer(conn, "Insurance")

        if any(kw in q for kw in ["interest expense", "interest paid", "interest cost",
                                   "how much interest", "bank interest", "interest on loan"]):
            return _ledger_expense_answer(conn, "Interest")

        if any(kw in q for kw in ["depreciation", "dep expense"]):
            return _ledger_expense_answer(conn, "Depreciation")

        if any(kw in q for kw in ["audit fee", "audit cost", "ca fee", "ca charges"]):
            return _ledger_expense_answer(conn, "Audit")

        if any(kw in q for kw in ["bank charge", "bank fee"]):
            return _ledger_expense_answer(conn, "bank charge")

        if any(kw in q for kw in ["professional fee", "professional charge", "consultancy"]):
            return _ledger_expense_answer(conn, "professional") or _ledger_expense_answer(conn, "consultancy")

        # How much spent on [X]?
        spent_match = re.search(r"(?:how much|what).+(?:spent?|paid|cost|expense).+(?:on|for|towards)\s+(.+?)(?:\s*\?|\s*$)", q)
        if spent_match:
            term = spent_match.group(1).strip().rstrip("?")
            result = _ledger_expense_answer(conn, term)
            if result:
                return result

        # ════════════════════════════════════════════════════════════════
        # STAGE 18: INVOICE / VOUCHER STATISTICS
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["how many invoice", "how many bill", "invoice count",
                                   "total invoice", "number of invoice",
                                   "how many sales invoice"]):
            row = conn.execute("""
                SELECT COUNT(DISTINCT GUID) FROM trn_voucher
                WHERE VOUCHERTYPENAME IN ('Sales', 'SALE INVOICE')
            """).fetchone()
            total_sales_inv = row[0] if row else 0
            row2 = conn.execute("""
                SELECT COUNT(DISTINCT GUID) FROM trn_voucher WHERE VOUCHERTYPENAME = 'Purchase'
            """).fetchone()
            total_purch_inv = row2[0] if row2 else 0
            return (f"**Invoice Count :**\n\n"
                    f"- **Sales Invoices:** {total_sales_inv}\n"
                    f"- **Purchase Bills:** {total_purch_inv}\n"
                    f"- **Total:** {total_sales_inv + total_purch_inv}")

        if any(kw in q for kw in ["average invoice", "avg invoice", "per invoice",
                                   "invoice value", "average bill value",
                                   "average sale value"]):
            from analytics import monthly_sales
            data = monthly_sales(conn)
            total_amt = sum(r[2] for r in data) if data else 0
            total_count = sum(r[1] for r in data) if data else 0
            avg = total_amt / total_count if total_count else 0
            # Find highest and lowest invoice
            _sp2, _sg2 = _nature_ph(conn, 'sales')
            highest = conn.execute(f"""
                SELECT v.VOUCHERNUMBER, v.PARTYLEDGERNAME, v.DATE,
                       ABS(SUM(CAST(a.AMOUNT AS REAL))) as amt
                FROM trn_voucher v JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
                JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
                WHERE l.PARENT IN ({_sp2})
                GROUP BY v.GUID ORDER BY amt DESC LIMIT 1
            """, _sg2).fetchone()
            lines = [f"**Sales Invoice Analysis:**\n"]
            lines.append(f"- **Total Sales:** {_fmt_indian(total_amt)}")
            lines.append(f"- **Number of Invoices:** {total_count}")
            lines.append(f"- **Average Invoice Value:** {_fmt_indian(avg)}")
            if highest:
                lines.append(f"- **Highest Single Invoice:** {_fmt_indian(highest[3])} (#{highest[0]}, {highest[1]})")
            return "\n".join(lines)

        if any(kw in q for kw in ["highest invoice", "biggest invoice", "largest invoice",
                                   "highest single", "biggest single", "largest single",
                                   "highest sale"]):
            _sp3, _sg3 = _nature_ph(conn, 'sales')
            row = conn.execute(f"""
                SELECT v.VOUCHERNUMBER, v.PARTYLEDGERNAME, v.DATE,
                       ABS(SUM(CAST(a.AMOUNT AS REAL))) as amt
                FROM trn_voucher v JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
                JOIN mst_ledger l ON l.NAME = a.LEDGERNAME
                WHERE l.PARENT IN ({_sp3})
                GROUP BY v.GUID ORDER BY amt DESC LIMIT 1
            """, _sg3).fetchone()
            if row:
                dt_fmt = f"{row[2][6:8]}/{row[2][4:6]}/{row[2][:4]}" if row[2] and len(row[2]) == 8 else row[2]
                return (f"**Highest Single Sales Invoice:**\n\n"
                        f"- **Invoice #:** {row[0]}\n"
                        f"- **Party:** {row[1]}\n"
                        f"- **Date:** {dt_fmt}\n"
                        f"- **Amount:** {_fmt_indian(row[3])}")

        if any(kw in q for kw in ["how many customer", "how many buyer",
                                   "customer count", "number of customer",
                                   "total customer", "how many parties"]):
            _dp, _dg = _nature_ph(conn, 'debtors')
            row = conn.execute(f"SELECT COUNT(*) FROM mst_ledger WHERE PARENT IN ({_dp})", _dg).fetchone()
            debtor_count = row[0] if row else 0
            active = conn.execute("""
                SELECT COUNT(DISTINCT PARTYLEDGERNAME) FROM trn_voucher
                WHERE VOUCHERTYPENAME IN ('Sales', 'SALE INVOICE')
                  AND PARTYLEDGERNAME IS NOT NULL AND PARTYLEDGERNAME != ''
            """).fetchone()
            active_count = active[0] if active else 0
            return (f"**Customer Analysis:**\n\n"
                    f"- **Total Customer Ledgers:** {debtor_count}\n"
                    f"- **Active Customers (with sales):** {active_count}\n"
                    f"- **Inactive:** {debtor_count - active_count}")

        if any(kw in q for kw in ["how many supplier", "how many vendor",
                                   "supplier count", "number of supplier",
                                   "total supplier"]):
            _cp, _cg = _nature_ph(conn, 'creditors')
            row = conn.execute(f"SELECT COUNT(*) FROM mst_ledger WHERE PARENT IN ({_cp})", _cg).fetchone()
            creditor_count = row[0] if row else 0
            active = conn.execute("""
                SELECT COUNT(DISTINCT PARTYLEDGERNAME) FROM trn_voucher
                WHERE VOUCHERTYPENAME = 'Purchase'
                  AND PARTYLEDGERNAME IS NOT NULL AND PARTYLEDGERNAME != ''
            """).fetchone()
            active_count = active[0] if active else 0
            return (f"**Supplier Analysis:**\n\n"
                    f"- **Total Supplier Ledgers:** {creditor_count}\n"
                    f"- **Active Suppliers (with purchases):** {active_count}")

        if any(kw in q for kw in ["total transaction", "how many transaction",
                                   "transaction count", "total voucher",
                                   "how many voucher", "voucher count"]):
            vch_data = voucher_summary(conn)
            total_count = sum(c for _, c, _ in vch_data)
            lines = ["**Transaction Summary :**\n"]
            lines.append(f"**Total Vouchers: {total_count}**\n")
            for vtype, count, total in vch_data:
                lines.append(f"- **{vtype}:** {count} vouchers ({_fmt_indian(total or 0)})")
            return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 19: AMOUNT-BASED QUERIES
        # ════════════════════════════════════════════════════════════════

        threshold = _extract_amount(q)
        if threshold:
            if any(kw in q for kw in ["debtor", "customer", "owe", "receivable",
                                       "outstanding"]) and any(kw in q for kw in [
                                           "more than", "above", "over", "exceeding",
                                           "greater than", "lakh", "crore"]):
                debtors = debtor_aging(conn)
                filtered = [(n, b) for n, b in debtors if b >= threshold]
                filtered.sort(key=lambda x: x[1], reverse=True)
                if filtered:
                    total = sum(b for _, b in filtered)
                    lines = [f"**Debtors owing more than {_fmt_indian(threshold)}:**\n"]
                    for i, (name, bal) in enumerate(filtered, 1):
                        lines.append(f"{i}. **{name}**: {_fmt_indian(bal)}")
                    lines.append(f"\n**Total:** {_fmt_indian(total)} across {len(filtered)} parties")
                    return "\n".join(lines)
                else:
                    return f"No debtors with outstanding above {_fmt_indian(threshold)}."

            if any(kw in q for kw in ["creditor", "supplier", "payable", "bill",
                                       "unpaid"]) and any(kw in q for kw in [
                                           "more than", "above", "over", "exceeding",
                                           "greater than", "lakh", "crore"]):
                creditors = creditor_aging(conn)
                filtered = [(n, b) for n, b in creditors if b >= threshold]
                filtered.sort(key=lambda x: x[1], reverse=True)
                if filtered:
                    total = sum(b for _, b in filtered)
                    lines = [f"**Creditors with payable above {_fmt_indian(threshold)}:**\n"]
                    for i, (name, bal) in enumerate(filtered, 1):
                        lines.append(f"{i}. **{name}**: {_fmt_indian(bal)}")
                    lines.append(f"\n**Total:** {_fmt_indian(total)} across {len(filtered)} suppliers")
                    return "\n".join(lines)
                else:
                    return f"No creditors with outstanding above {_fmt_indian(threshold)}."

        # ════════════════════════════════════════════════════════════════
        # STAGE 20: STOCK / INVENTORY
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["stock item", "inventory", "how many item",
                                   "how many product", "stock count", "product count",
                                   "product list", "stock list", "inventory count"]):
            count = conn.execute("SELECT COUNT(*) FROM mst_stock_item").fetchone()[0]
            groups = conn.execute("""
                SELECT PARENT, COUNT(*) as cnt FROM mst_stock_item
                WHERE PARENT IS NOT NULL GROUP BY PARENT ORDER BY cnt DESC LIMIT 10
            """).fetchall()
            lines = [f"**Inventory Summary — {_get_company_name()}**\n"]
            lines.append(f"**Total Stock Items: {count}**\n")
            if groups:
                lines.append("**By Category:**")
                for g, c in groups:
                    lines.append(f"  - {g}: {c} items")
            return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 21: CASH FLOW
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["cash flow", "cashflow", "cash inflow", "cash outflow",
                                   "receipt vs payment", "receipts and payments",
                                   "collection vs payment", "money coming in",
                                   "money going out"]):
            from analytics import monthly_receipts_payments
            receipts, payments = monthly_receipts_payments(conn)
            r_dict = {r[0]: r[2] for r in receipts}
            p_dict = {r[0]: r[2] for r in payments}
            all_months = sorted(set(list(r_dict.keys()) + list(p_dict.keys())))
            total_r = sum(r_dict.values())
            total_p = sum(p_dict.values())
            lines = [f"**Cash Flow Summary **\n"]
            lines.append(f"**Total Receipts:** {_fmt_indian(total_r)}")
            lines.append(f"**Total Payments:** {_fmt_indian(total_p)}")
            lines.append(f"**Net Cash Flow:** {_fmt_indian(total_r - total_p)}\n")
            lines.append(f"{'Month':<10} | {'Receipts':>12} | {'Payments':>12} | {'Net':>12}")
            lines.append("-" * 55)
            for m in all_months:
                r = r_dict.get(m, 0)
                p = p_dict.get(m, 0)
                lines.append(f"{_month_label(m):<10} | {_fmt_exact(r):>12} | {_fmt_exact(p):>12} | {_fmt_exact(r - p):>12}")
            return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 22: TOP CUSTOMERS / TOP SUPPLIERS
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["top customer", "best customer", "biggest customer",
                                   "largest customer", "highest sales party",
                                   "top buyer", "biggest buyer"]):
            from analytics import top_customers_by_sales
            data = top_customers_by_sales(conn, 15)
            if data:
                total_sales = sum(r[2] for r in data)
                lines = [f"**Top Customers by Sales :**\n"]
                for i, (party, count, amt) in enumerate(data, 1):
                    pct = (amt / total_sales * 100) if total_sales else 0
                    lines.append(f"{i}. **{party}**: {_fmt_indian(amt)} ({count} invoices, {pct:.1f}%)")
                lines.append(f"\n**Total from top 15:** {_fmt_indian(total_sales)}")
                return "\n".join(lines)

        if any(kw in q for kw in ["top supplier", "biggest supplier", "largest supplier",
                                   "highest purchase party", "top vendor"]):
            from analytics import top_suppliers_by_purchase
            data = top_suppliers_by_purchase(conn, 15)
            if data:
                total_purch = sum(r[2] for r in data)
                lines = [f"**Top Suppliers by Purchase :**\n"]
                for i, (party, count, amt) in enumerate(data, 1):
                    pct = (amt / total_purch * 100) if total_purch else 0
                    lines.append(f"{i}. **{party}**: {_fmt_indian(amt)} ({count} bills, {pct:.1f}%)")
                lines.append(f"\n**Total from top 15:** {_fmt_indian(total_purch)}")
                return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 23: COLLECTION EFFICIENCY
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["collection efficiency", "how well am i collecting",
                                   "receipt vs sales", "collection rate",
                                   "recovery rate", "collection ratio"]):
            from analytics import collection_efficiency
            data = collection_efficiency(conn)
            if data:
                lines = ["**Collection Efficiency (Receipts as % of Sales):**\n"]
                for m in data:
                    month = m["month"]
                    eff = m["efficiency"]
                    bar = "=" * int(min(eff, 100) / 5)
                    lines.append(f"- {_month_label(month)}: {eff:.0f}% [{bar}]")
                avg_eff = sum(m["efficiency"] for m in data) / len(data) if data else 0
                lines.append(f"\n**Average Efficiency:** {avg_eff:.0f}%")
                if avg_eff < 80:
                    lines.append("**Concern:** Collection efficiency below 80% indicates receivables are building up. Tighten your follow-up process.")
                return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 24: MONTHLY P&L / PROFITABILITY BREAKDOWN
        # ════════════════════════════════════════════════════════════════

        if any(kw in q for kw in ["monthly profit", "monthly p&l", "month wise profit",
                                   "monthwise profit", "monthly profitability",
                                   "profit month by month", "profit by month"]):
            from analytics import monthly_gross_profit
            data = monthly_gross_profit(conn)
            if data:
                lines = ["**Monthly P&L Summary :**\n"]
                lines.append(f"{'Month':<10} | {'Sales':>12} | {'Purchases':>12} | {'GP':>12} | {'NP':>12} | {'GP%':>5}")
                lines.append("-" * 75)
                for m in data:
                    lines.append(f"{_month_label(m['month']):<10} | {_fmt_exact(m['sales']):>12} | {_fmt_exact(m['purchases']):>12} | {_fmt_exact(m['gross_profit']):>12} | {_fmt_exact(m['net_profit']):>12} | {m['gp_margin']:.1f}%")
                total_s = sum(m['sales'] for m in data)
                total_p = sum(m['purchases'] for m in data)
                total_gp = sum(m['gross_profit'] for m in data)
                total_np = sum(m['net_profit'] for m in data)
                lines.append("-" * 75)
                lines.append(f"{'TOTAL':<10} | {_fmt_exact(total_s):>12} | {_fmt_exact(total_p):>12} | {_fmt_exact(total_gp):>12} | {_fmt_exact(total_np):>12} |")
                return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 25: FLEXIBLE ENTITY SEARCH (catch-all for party/ledger)
        # ════════════════════════════════════════════════════════════════

        # Try to find entity in the question and show relevant info
        entity = _extract_entity_name(q_original) or _extract_entity_name(q)
        if entity:
            match = _fuzzy_match_party(conn, entity)
            if match:
                name, parent, bal = match
                lines = [f"**{name}** ({parent})\n"]
                lines.append(f"**Closing Balance:** {_fmt_indian(abs(bal or 0))}")
                if parent == "Sundry Debtors":
                    lines.append("Type: Customer/Debtor — this party owes you money." if (bal or 0) < 0 else "Type: Customer/Debtor")
                elif parent == "Sundry Creditors":
                    lines.append("Type: Supplier/Creditor — you owe this party.")
                # Recent transactions
                recent = conn.execute("""
                    SELECT v.DATE, v.VOUCHERTYPENAME, ABS(CAST(a.AMOUNT AS REAL))
                    FROM trn_accounting a JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                    WHERE a.LEDGERNAME = ? ORDER BY v.DATE DESC LIMIT 5
                """, (name,)).fetchall()
                if recent:
                    lines.append("\n**Last 5 Transactions:**")
                    for dt, vtype, amt in recent:
                        dt_fmt = f"{dt[6:8]}/{dt[4:6]}/{dt[:4]}" if dt and len(dt) == 8 else dt
                        lines.append(f"  - {dt_fmt} | {vtype} | {_fmt_indian(amt)}")
                return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 26: GENERIC KEYWORD → LEDGER SEARCH
        # ════════════════════════════════════════════════════════════════

        # Strip common words and search for remaining terms as ledger names
        stop_words = {"show", "me", "the", "a", "an", "of", "for", "what", "is", "are",
                      "how", "much", "many", "get", "display", "give", "tell", "about",
                      "amount", "please", "can", "you", "do", "does", "did", "will",
                      "have", "has", "been", "this", "that", "which", "there", "my",
                      "our", "with", "from", "all", "list", "any", "some", "no", "not",
                      "and", "or", "but", "in", "on", "at", "to", "by", "up", "its",
                      "it", "i", "we", "they", "total", "balance", "detail", "statement",
                      "account", "ledger", "party"}
        words = [w for w in re.findall(r'\w+', q) if w.lower() not in stop_words and len(w) > 2]
        if words:
            search_term = " ".join(words)
            results = search_ledger(conn, search_term)
            if results:
                if len(results) == 1:
                    name, parent, bal = results[0]
                    return f"Found: **{name}** ({parent}) — Balance: {_fmt_indian(abs(bal or 0))}. Ask me 'show ledger of {name}' for full details."
                else:
                    lines = [f"I found **{len(results)}** matching ledgers for '{search_term}':\n"]
                    for name, parent, bal in results[:10]:
                        lines.append(f"- **{name}** ({parent}): {_fmt_indian(abs(bal or 0))}")
                    lines.append("\nAsk about any specific one for more details.")
                    return "\n".join(lines)

            # Try individual words
            for word in words:
                if len(word) >= 3:
                    results = search_ledger(conn, word)
                    if results:
                        if len(results) == 1:
                            name, parent, bal = results[0]
                            return f"Found: **{name}** ({parent}) — Balance: {_fmt_indian(abs(bal or 0))}. Ask me for the full ledger details."
                        elif len(results) <= 5:
                            lines = [f"I found **{len(results)}** ledgers matching '{word}':\n"]
                            for name, parent, bal in results:
                                lines.append(f"- **{name}** ({parent}): {_fmt_indian(abs(bal or 0))}")
                            lines.append("\nWhich one would you like to explore?")
                            return "\n".join(lines)

        # ════════════════════════════════════════════════════════════════
        # STAGE 27: LAST RESORT — try to be helpful anyway
        # ════════════════════════════════════════════════════════════════

        # If we got here, we truly don't understand. But rather than returning None,
        # provide a helpful response with what we CAN do
        pl = profit_and_loss(conn)
        np_ = pl["net_profit"]
        return (f"I'm not sure I fully understood your question: \"{q_original}\"\n\n"
                f"But here's a quick snapshot of **{_get_company_name()}**:\n"
                f"- Net Profit: {_fmt_indian(np_)}\n"
                f"- Revenue: {_fmt_indian(pl['total_income'])}\n\n"
                f"Try asking me things like:\n"
                f"- \"Am I making profit?\"\n"
                f"- \"Who owes me the most?\"\n"
                f"- \"How were sales in October?\"\n"
                f"- \"Show me the HDFC Bank ledger\"\n"
                f"- \"Any red flags in my business?\"\n"
                f"- \"Compare October and November sales\"\n"
                f"- \"Which customers owe more than 1 lakh?\"")

    except Exception as e:
        return (f"I encountered an issue processing your question. "
                f"Please try rephrasing. Error details: {str(e)[:100]}")
    finally:
        conn.close()


def _ledger_expense_answer(conn, search_term):
    """Helper to answer questions about specific expense/ledger items."""
    results = search_ledger(conn, search_term)
    if not results:
        return None
    total = sum(abs(r[2] or 0) for r in results)
    if len(results) == 1:
        name, parent, bal = results[0]
        return (f"**{name}** ({parent})\n\n"
                f"**Total this period:** {_fmt_indian(abs(bal or 0))}\n\n"
                f"Ask 'show ledger of {name}' for transaction-level details.")
    lines = [f"**Ledgers matching '{search_term}':**\n"]
    for name, parent, bal in results:
        lines.append(f"- **{name}** ({parent}): {_fmt_indian(abs(bal or 0))}")
    lines.append(f"\n**Combined Total:** {_fmt_indian(total)}")
    return "\n".join(lines)


def execute_action(action_data):
    """Execute the action determined by the LLM.
    DEFENSIVE: Always returns a valid result dict. Never crashes.
    """
    if not action_data or not isinstance(action_data, dict):
        return {"type": "chat", "message": "Could not determine action.", "explanation": ""}
    try:
        conn = get_conn()
    except Exception:
        return {"type": "error", "message": "Database connection failed."}
    action = action_data.get("action", "chat")
    params = action_data.get("params", {}) or {}
    explanation = action_data.get("explanation", "")

    try:
        if action == "report_pl":
            data = profit_and_loss(conn, params.get("from_date"), params.get("to_date"))
            return {"type": "pl", "data": data, "explanation": explanation}

        elif action == "report_bs":
            data = balance_sheet(conn, params.get("as_of_date"))
            return {"type": "bs", "data": data, "explanation": explanation}

        elif action == "report_tb":
            data = trial_balance(conn, params.get("as_of_date"))
            return {"type": "tb", "data": data, "explanation": explanation}

        elif action == "ledger_detail":
            ledger_name = params.get("ledger_name", "")
            opening, txns, closing = ledger_detail(
                conn, ledger_name, params.get("from_date"), params.get("to_date")
            )
            return {
                "type": "ledger",
                "ledger_name": ledger_name,
                "opening": opening,
                "transactions": txns,
                "closing": closing,
                "explanation": explanation,
            }

        elif action == "pl_drilldown":
            data = pl_group_drilldown(
                conn, params.get("group_name", ""),
                params.get("from_date"), params.get("to_date")
            )
            return {
                "type": "pl_drilldown",
                "group_name": params.get("group_name", ""),
                "transactions": data,
                "explanation": explanation,
            }

        elif action == "debtors":
            data = debtor_aging(conn)
            return {"type": "debtors", "data": data, "explanation": explanation}

        elif action == "creditors":
            data = creditor_aging(conn)
            return {"type": "creditors", "data": data, "explanation": explanation}

        elif action == "voucher_summary":
            data = voucher_summary(conn, params.get("from_date"), params.get("to_date"))
            return {"type": "voucher_summary", "data": data, "explanation": explanation}

        elif action == "search":
            results = search_ledger(conn, params.get("query", ""))
            return {"type": "search", "data": results, "explanation": explanation}

        elif action == "sql_query":
            sql = params.get("sql", "")
            # Safety: only allow SELECT
            if not sql.strip().upper().startswith("SELECT"):
                return {"type": "error", "message": "Only SELECT queries are allowed."}
            cursor = conn.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            return {
                "type": "sql_result",
                "columns": columns,
                "rows": rows[:500],  # limit results
                "total": len(rows),
                "explanation": explanation,
            }

        elif action == "chat":
            return {"type": "chat", "message": params.get("response", explanation)}

        else:
            return {"type": "error", "message": f"Unknown action: {action}"}

    except Exception as e:
        return {"type": "error", "message": str(e)}
    finally:
        conn.close()


def classify_intent(question):
    """Local intent classifier — no API needed. Maps natural language to actions.
    DEFENSIVE: Never crashes on any input. Always returns a valid action dict.
    """
    try:
        q = (question or "").lower().strip()
    except Exception:
        return _fallback()

    if not q:
        return _fallback()

    try:
        conn = get_conn()
    except Exception:
        return _fallback()

    # ── 1. EXACT PHRASE MATCHES (highest confidence) ────────────────────────

    # P&L — exact phrases
    if any(kw in q for kw in ["profit and loss", "p&l", "p & l", "profit & loss",
                               "income statement", "income and expense", "p/l",
                               "profit loss", "pandl", "mujhe p&l dikhao"]):
        conn.close()
        return {"action": "report_pl", "params": {},
                "explanation": f"Profit & Loss Account for {_get_company_name()}"}

    # Balance Sheet — exact phrases
    if any(kw in q for kw in ["balance sheet", "b/s", "assets and liabilities",
                               "financial position"]):
        conn.close()
        return {"action": "report_bs", "params": {},
                "explanation": f"Balance Sheet for {_get_company_name()}"}

    # Trial Balance — exact phrases
    if any(kw in q for kw in ["trial balance", "t/b"]):
        conn.close()
        return {"action": "report_tb", "params": {},
                "explanation": f"Trial Balance for {_get_company_name()}"}

    # ── 2. DEBTORS / CREDITORS ──────────────────────────────────────────────

    # Debtors — expanded with client language
    if any(kw in q for kw in ["debtor", "receivable", "sundry debtor", "who owes",
                               "owes us", "owe me", "owe us", "people owe",
                               "outstanding from customer", "money owe me",
                               "how much money do people owe"]):
        conn.close()
        return {"action": "debtors", "params": {},
                "explanation": "Sundry Debtors — Outstanding Receivables"}

    # Creditors — expanded with client language
    if any(kw in q for kw in ["creditor", "payable", "sundry creditor", "we owe",
                               "i owe", "owe to supplier", "owe supplier",
                               "outstanding to supplier", "how much do i owe",
                               "how much do we owe"]):
        conn.close()
        return {"action": "creditors", "params": {},
                "explanation": "Sundry Creditors — Outstanding Payables"}

    # ── 2b. GST QUERIES ───────────────────────────────────────────────────

    if any(kw in q for kw in ["gst", "gstr", "gstr-1", "gstr-3b", "gstr1", "gstr3b",
                               "output tax", "input tax", "itc", "input credit",
                               "tax liability", "tax payable", "tax refund",
                               "gst return", "gst filing", "gst summary",
                               "output liability", "input liability",
                               "worst month.*gst", "best month.*gst",
                               "cgst", "sgst", "igst",
                               "net gst", "gst payable", "gst refundable"]):
        # Try to compute GST summary
        try:
            from gst_engine import gst_monthly_comparison
            gst_conn = get_conn()
            monthly = gst_monthly_comparison(gst_conn)
            gst_conn.close()
            if monthly:
                lines = ["GST Monthly Summary (Output vs Input Tax):\n"]
                lines.append(f"{'Month':<10} {'Output':>12} {'Input':>12} {'Net':>12} {'Status'}")
                lines.append("-" * 60)
                worst_month = None
                worst_net = float('-inf')
                for m in monthly:
                    month_label = m.get('month_label', m.get('month', ''))
                    output = m.get('output_tax', m.get('total_output', 0))
                    input_t = m.get('input_tax', m.get('total_input', 0))
                    net = output - input_t
                    status = "Payable" if net > 0 else "Refundable"
                    lines.append(f"{month_label:<10} {output:>12,.0f} {input_t:>12,.0f} {net:>12,.0f} {status}")
                    if net > worst_net:
                        worst_net = net
                        worst_month = month_label
                lines.append(f"\nHighest liability month: {worst_month} (Net: ₹{worst_net:,.0f})")
                conn.close()
                return {"action": "chat",
                        "params": {"response": "\n".join(lines)},
                        "explanation": "GST Monthly Summary"}
        except Exception:
            pass
        conn.close()
        return {"action": "chat",
                "params": {"response": "GST data is available on the GST Returns page. "
                           "Navigate to the GST Returns tab in the sidebar for detailed "
                           "GSTR-1, GSTR-3B, and monthly tax analysis."},
                "explanation": "GST Returns"}

    # ── 3. VOUCHER / INVOICE QUERIES ────────────────────────────────────────

    if any(kw in q for kw in ["voucher summary", "voucher count", "how many vouchers",
                               "transaction summary", "voucher type",
                               "how many sales invoice", "how many purchase invoice",
                               "invoice count", "journal entries", "journal entry",
                               "contra entries", "contra entry", "credit note",
                               "debit note", "day book", "receipt voucher",
                               "payment voucher", "voucher number gap",
                               "which voucher types"]):
        conn.close()
        return {"action": "voucher_summary", "params": {},
                "explanation": "Voucher Summary by Type"}

    # ── 4. P&L DRILLDOWN — group-level analysis ────────────────────────────

    pl_groups = {
        "sales": "Sales Accounts",
        "purchase": "Purchase Accounts",
        "direct expense": "Direct Expenses",
        "direct income": "Direct Incomes",
        "indirect expense": "Indirect Expenses",
        "indirect income": "Indirect Incomes",
    }
    # Relaxed trigger: group keyword + any action/analysis word
    drilldown_triggers = ["drill", "detail", "break", "show", "transactions",
                          "entries", "what", "total", "trend", "monthly",
                          "summary", "list", "all", "how much", "this month",
                          "this year", "this fy", "party-wise", "partywise"]
    for keyword, group_name in pl_groups.items():
        if keyword in q and any(kw in q for kw in drilldown_triggers):
            conn.close()
            return {"action": "pl_drilldown",
                    "params": {"group_name": group_name},
                    "explanation": f"Drilldown into {group_name}"}

    # ── 5. SEMANTIC P&L CONCEPTS ────────────────────────────────────────────
    # Questions about profit, margin, revenue, expenses → report_pl

    pl_concept_keywords = [
        "net profit", "gross profit", "operating profit", "net loss",
        "gross margin", "net margin", "profit margin", "operating margin",
        "ebitda", "revenue", "total income", "total expense",
        "am i making profit", "is my business profitable", "are we profitable",
        "am i profitable", "how is my business", "is the company profitable",
        "top 5 expense", "top five expense", "what are my expense",
        "expense as percentage", "month-on-month revenue", "month on month revenue",
        "revenue trend", "compare p&l", "compare profit",
        "which expense grew", "expense grew the most",
        "what percentage of revenue", "salary as percentage",
        "break-even", "breakeven",
    ]
    if any(kw in q for kw in pl_concept_keywords):
        conn.close()
        return {"action": "report_pl", "params": {},
                "explanation": f"Profit & Loss Account for {_get_company_name()}"}

    # ── 6. SEMANTIC BS CONCEPTS ─────────────────────────────────────────────
    # Questions about ratios, assets, capital, net worth → report_bs

    bs_concept_keywords = [
        "current ratio", "quick ratio", "acid test", "debt-equity",
        "debt equity", "working capital", "net worth", "total capital",
        "fixed asset", "total asset", "return on asset", "roa",
        "return on equity", "roe", "return on capital", "roce",
        "proprietary ratio", "asset turnover", "cash ratio",
        "book value", "fund flow", "capital employed",
        "cash and bank", "bank balance", "how much cash",
        "money in my bank", "money in bank",
        "interest coverage", "debtor turnover", "creditor turnover",
        "debtor days", "creditor days", "receivable turnover",
        "payable turnover", "working capital turnover",
        "can i afford", "is my business healthy", "financial health",
    ]
    if any(kw in q for kw in bs_concept_keywords):
        conn.close()
        return {"action": "report_bs", "params": {},
                "explanation": f"Balance Sheet for {_get_company_name()}"}

    # ── 7. COMMON EXPENSE/ACCOUNT LEDGER NAMES ─────────────────────────────
    # Direct match for well-known account names → ledger_detail

    known_accounts = {
        "rent": "Rent",
        "salary": "Salary",
        "salaries": "Salary",
        "depreciation": "Depreciation",
        "professional fee": "Professional Fees",
        "professional charges": "Professional Fees",
        "electricity": "Electricity",
        "power and fuel": "Power & Fuel",
        "repairs": "Repairs & Maintenance",
        "maintenance": "Repairs & Maintenance",
        "telephone": "Telephone",
        "internet": "Internet",
        "travel": "Travel",
        "conveyance": "Conveyance",
        "travelling": "Travelling",
        "printing": "Printing & Stationery",
        "stationery": "Printing & Stationery",
        "insurance": "Insurance",
        "audit fee": "Audit Fees",
        "legal": "Legal Charges",
        "staff welfare": "Staff Welfare",
        "bonus": "Bonus",
        "incentive": "Incentive",
        "petty cash": "Petty Cash",
        "cash": "Cash",
        "cgst": "CGST",
        "sgst": "SGST",
        "igst": "IGST",
        "advance tax": "Advance Tax",
        "tds": "TDS",
    }
    for keyword, account_hint in known_accounts.items():
        if keyword in q:
            results = search_ledger(conn, account_hint)
            if results:
                conn.close()
                return {"action": "ledger_detail",
                        "params": {"ledger_name": results[0][0]},
                        "explanation": f"Statement of Account: {results[0][0]}"}

    # ── 8. SEARCH / FIND explicit requests ──────────────────────────────────

    if any(kw in q for kw in ["search for", "find ledger", "find all ledger",
                               "look up", "is there a ledger", "search ledger",
                               "which group does"]):
        # Extract the search term
        for pattern in [r"search (?:for )?(?:ledger )?[\"']?(.+?)[\"']?\s*$",
                        r"find (?:all )?ledger[s]? (?:with |named |called )?[\"']?(.+?)[\"']?\s*$",
                        r"is there a ledger (?:for |called |named )?[\"']?(.+?)[\"']?\s*$",
                        r"look up [\"']?(.+?)[\"']?\s*$"]:
            match = re.search(pattern, q)
            if match:
                term = match.group(1).strip().strip("'\"")
                results = search_ledger(conn, term)
                conn.close()
                return {"action": "search", "params": {"query": term},
                        "explanation": f"Search results for: {term}"}
        # Fallback: search with remaining words
        words = re.findall(r'\w+', q)
        term = " ".join(w for w in words if w not in {"search", "for", "find", "ledger", "all", "look", "up", "is", "there", "a"})
        if term:
            conn.close()
            return {"action": "search", "params": {"query": term},
                    "explanation": f"Search results for: {term}"}

    # ── 9. CLIENT LANGUAGE — simple business questions ──────────────────────

    if any(kw in q for kw in ["am i making profit", "making money", "are we making",
                               "is my business doing", "how is my business",
                               "business growing", "is my business growing"]):
        conn.close()
        return {"action": "report_pl", "params": {},
                "explanation": "Let me check your profitability..."}

    if any(kw in q for kw in ["highest sale", "biggest sale", "largest sale",
                               "best sale", "highest invoice"]):
        conn.close()
        return {"action": "voucher_summary", "params": {},
                "explanation": "Sales analysis"}

    if any(kw in q for kw in ["how many customer", "how many buyer",
                               "customer count", "buyer count"]):
        conn.close()
        return {"action": "voucher_summary", "params": {},
                "explanation": "Customer transaction summary"}

    # ── 10. CAPABILITIES question ───────────────────────────────────────────

    if any(kw in q for kw in ["what can you do", "help me", "capabilities",
                               "what do you do", "what are your feature"]):
        conn.close()
        return _fallback()

    # ── 11. SAFETY — refuse destructive requests ────────────────────────────

    if any(kw in q for kw in ["delete", "drop", "update ", "insert ", "create ledger",
                               "modify", "change the", "update the"]):
        conn.close()
        return {"action": "chat",
                "params": {"response": "I'm a read-only assistant. I cannot modify your Tally data. "
                           "Please make changes directly in TallyPrime."},
                "explanation": "Read-only mode"}

    # ── 12. OUT-OF-SCOPE detection ──────────────────────────────────────────

    if any(kw in q for kw in ["weather", "cricket", "movie", "recipe", "news",
                               "stock market", "share price"]):
        conn.close()
        return {"action": "chat",
                "params": {"response": "I'm your Tally financial assistant. "
                           "I can help with P&L, Balance Sheet, ledgers, debtors, creditors, "
                           "and other accounting queries. Try asking about your financials!"},
                "explanation": ""}

    # ── 13. LEDGER DETAIL — regex pattern extraction ────────────────────────

    ledger_patterns = [
        r"(?:ledger|account|statement|transactions?)\s+(?:of|for)\s+(.+?)(?:\s*$|\s+from|\s+between|\s+for\s+\w+\s+\d)",
        r"(?:show|get|display)\s+(.+?)\s+(?:ledger|account|statement)",
        r"(?:show|get|display)\s+(?:me\s+)?(?:the\s+)?(.+?)\s+(?:transactions|entries)",
        r"(?:closing balance|opening balance|balance)\s+(?:of|for|in)\s+(.+?)(?:\s*$|\?)",
        r"(?:what|how much).+(?:spend|spent|paid).+(?:on|for)\s+(.+?)(?:\s*$|\?|\s+this|\s+last)",
        r"(?:show|give|tell).+(?:all\s+)?(?:entries|transactions)\s+.+(?:made|posted)\s+(?:for|in|to)\s+(.+?)(?:\s*$|\?)",
    ]
    for pattern in ledger_patterns:
        match = re.search(pattern, q, re.IGNORECASE)
        if match:
            search_term = match.group(1).strip().strip("'\"")
            # Clean up common trailing words
            search_term = re.sub(r'\s+(this|last|current|previous)\s+(month|year|quarter|fy).*$', '', search_term)
            if len(search_term) > 2:
                results = search_ledger(conn, search_term)
                if results:
                    conn.close()
                    return {"action": "ledger_detail",
                            "params": {"ledger_name": results[0][0]},
                            "explanation": f"Statement of Account: {results[0][0]}"}

    # ── 14. GENERIC SEARCH FALLBACK ─────────────────────────────────────────

    stop_words = {"show", "me", "the", "a", "an", "of", "for", "what", "is", "are",
                  "how", "much", "many", "get", "display", "give", "tell", "about",
                  "amount", "please", "can", "you", "do", "does", "did", "will",
                  "have", "has", "been", "this", "that", "which", "there", "my",
                  "our", "with", "from", "all", "list", "any", "some", "no", "not",
                  "and", "or", "but", "in", "on", "at", "to", "by", "up", "its"}
    words = [w for w in re.findall(r'\w+', q) if w.lower() not in stop_words and len(w) > 2]
    if words:
        search_term = " ".join(words)
        results = search_ledger(conn, search_term)
        if results:
            if len(results) == 1 or (results and len(search_term) > 4):
                conn.close()
                return {"action": "ledger_detail",
                        "params": {"ledger_name": results[0][0]},
                        "explanation": f"Statement of Account: {results[0][0]}"}
            conn.close()
            return {"action": "search",
                    "params": {"query": search_term},
                    "explanation": f"Search results for: {search_term}"}

    conn.close()
    return _fallback()


def _fallback():
    """Return the help/capabilities message."""
    return {
        "action": "chat",
        "params": {"response": (
            "I can help you with:\n"
            "- **Profit & Loss** — 'show P&L', 'what is my net profit?'\n"
            "- **Balance Sheet** — 'show balance sheet', 'what is my current ratio?'\n"
            "- **Trial Balance** — 'show trial balance'\n"
            "- **Debtors** — 'show debtors', 'who owes me money?'\n"
            "- **Creditors** — 'show creditors', 'how much do I owe?'\n"
            "- **Ledger Detail** — 'show ledger of HDFC Bank', 'rent expense this year'\n"
            "- **P&L Drilldown** — 'total sales this month', 'show indirect expenses'\n"
            "- **Voucher Summary** — 'voucher summary', 'how many invoices?'\n"
            "- **Search** — 'search for pharma', 'find ledger transport'\n"
        )},
        "explanation": ""
    }


def ask(question, conversation_history=None):
    """Process a natural language question and return structured results.

    DEFENSIVE: Always returns a valid dict with at minimum {"type": "chat", "message": "..."}.
    Never crashes on any input.

    Flow:
    1. Try Gemini Flash LLM first — real conversational AI (free tier)
    2. If Gemini fails/unavailable, fall back to smart_answer() local engine
    3. If smart_answer returns None, fall back to classify_intent → execute_action
    """
    # Ensure we always have a string
    if not isinstance(question, str):
        question = str(question) if question is not None else ""

    if not question.strip():
        return {
            "type": "chat",
            "message": "Please ask a question about your financial data.",
            "raw_action": {"action": "empty_question"},
            "explanation": "",
        }

    try:
        # ── STEP 1: Try Gemini Flash (primary brain) ──
        if USE_LLM:
            try:
                gemini_response = ask_gemini(question, conversation_history)
                if gemini_response:
                    return {
                        "type": "chat",
                        "message": gemini_response,
                        "raw_action": {"action": "gemini_llm", "question": question},
                        "explanation": "",
                    }
            except Exception as e:
                logger.error(f"Gemini fallback triggered: {e}")

        # ── STEP 2: Fall back to local smart_answer ──
        try:
            smart = smart_answer(question)
            if smart is not None:
                return {
                    "type": "chat",
                    "message": smart,
                    "raw_action": {"action": "smart_answer", "question": question},
                    "explanation": "",
                }
        except Exception as e:
            logger.error(f"smart_answer fallback triggered: {e}")

        # ── STEP 3: Fall back to keyword classifier → action executor ──
        try:
            action_data = classify_intent(question)
            result = execute_action(action_data)
            result["raw_action"] = action_data
            # Ensure result always has required keys
            if "type" not in result:
                result["type"] = "chat"
            if "message" not in result and result["type"] == "chat":
                result["message"] = result.get("explanation", "I processed your question.")
            return result
        except Exception as e:
            logger.error(f"classify_intent fallback triggered: {e}")

        # Absolute last resort
        return {
            "type": "chat",
            "message": f"I couldn't process that question. Try asking about profit, sales, debtors, or creditors.",
            "raw_action": {"action": "all_fallback"},
            "explanation": "",
        }

    except Exception as e:
        logger.error(f"ask() fatal error: {e}")
        return {
            "type": "chat",
            "message": "An unexpected error occurred. Please try rephrasing your question.",
            "raw_action": {"action": "error", "error": str(e)[:200]},
            "explanation": "",
        }


def format_result_as_text(result):
    """Format a result dict as readable text for terminal/chat display."""
    rtype = result.get("type", "")
    explanation = result.get("explanation", "")
    output = []

    if explanation:
        output.append(explanation)
        output.append("")

    if rtype == "pl":
        data = result["data"]
        output.append("═══ PROFIT & LOSS ACCOUNT ═══")
        output.append("")
        output.append("INCOME:")
        for group, entries in data["income"].items():
            group_total = sum(abs(amt) for _, amt in entries)
            output.append(f"  {group}: ₹{group_total:,.2f}")
            for ledger, amt in entries:
                output.append(f"    {ledger}: ₹{abs(amt):,.2f}")
        output.append(f"  TOTAL INCOME: ₹{data['total_income']:,.2f}")
        output.append("")
        output.append("EXPENSES:")
        for group, entries in data["expense"].items():
            group_total = sum(abs(amt) for _, amt in entries)
            output.append(f"  {group}: ₹{group_total:,.2f}")
            for ledger, amt in entries:
                output.append(f"    {ledger}: ₹{abs(amt):,.2f}")
        output.append(f"  TOTAL EXPENSES: ₹{data['total_expense']:,.2f}")
        output.append("")
        output.append(f"  GROSS PROFIT: ₹{data['gross_profit']:,.2f}")
        output.append(f"  NET PROFIT:   ₹{data['net_profit']:,.2f}")

    elif rtype == "bs":
        data = result["data"]
        output.append("═══ BALANCE SHEET ═══")
        output.append("")
        output.append("ASSETS:")
        for group, entries in data["assets"].items():
            group_total = sum(abs(b) for _, b in entries)
            output.append(f"  {group}: ₹{group_total:,.2f}")
            for ledger, bal in entries[:5]:
                output.append(f"    {ledger}: ₹{abs(bal):,.2f}")
            if len(entries) > 5:
                output.append(f"    ... and {len(entries) - 5} more")
        output.append(f"  TOTAL ASSETS: ₹{data['total_assets']:,.2f}")
        output.append("")
        output.append("LIABILITIES:")
        for group, entries in data["liabilities"].items():
            group_total = sum(abs(b) for _, b in entries)
            output.append(f"  {group}: ₹{group_total:,.2f}")
            for ledger, bal in entries[:5]:
                output.append(f"    {ledger}: ₹{abs(bal):,.2f}")
            if len(entries) > 5:
                output.append(f"    ... and {len(entries) - 5} more")
        output.append(f"  TOTAL LIABILITIES: ₹{data['total_liabilities']:,.2f}")

    elif rtype == "tb":
        data = result["data"]
        output.append("═══ TRIAL BALANCE ═══")
        total_dr = sum(d for _, _, d, _ in data)
        total_cr = sum(c for _, _, _, c in data)
        for group, name, dr, cr in data[:30]:
            if dr > 0:
                output.append(f"  {name:40s} Dr: ₹{dr:>12,.2f}")
            else:
                output.append(f"  {name:40s} Cr: ₹{cr:>12,.2f}")
        if len(data) > 30:
            output.append(f"  ... and {len(data) - 30} more entries")
        output.append(f"\n  TOTAL DEBIT:  ₹{total_dr:>12,.2f}")
        output.append(f"  TOTAL CREDIT: ₹{total_cr:>12,.2f}")

    elif rtype == "ledger":
        output.append(f"═══ LEDGER: {result['ledger_name']} ═══")
        output.append(f"Opening Balance: ₹{result['opening']:,.2f}")
        output.append("")
        for txn in result["transactions"][:50]:
            dr = f"Dr: ₹{txn['debit']:,.2f}" if txn['debit'] else ""
            cr = f"Cr: ₹{txn['credit']:,.2f}" if txn['credit'] else ""
            date = txn['date']
            if date and len(date) == 8:
                date = f"{date[6:8]}/{date[4:6]}/{date[:4]}"
            output.append(f"  {date}  {txn['voucher_type']:12s} {txn['voucher_number']:10s} {dr}{cr}  Bal: ₹{txn['balance']:,.2f}")
        if len(result["transactions"]) > 50:
            output.append(f"  ... and {len(result['transactions']) - 50} more transactions")
        output.append(f"\nClosing Balance: ₹{result['closing']:,.2f}")

    elif rtype == "pl_drilldown":
        output.append(f"═══ P&L DRILLDOWN: {result['group_name']} ═══")
        total = sum(abs(t['amount'] or 0) for t in result['transactions'])
        output.append(f"Total: ₹{total:,.2f} ({len(result['transactions'])} transactions)")
        output.append("")
        for txn in result["transactions"][:30]:
            date = txn['date']
            if date and len(date) == 8:
                date = f"{date[6:8]}/{date[4:6]}/{date[:4]}"
            output.append(f"  {date}  {txn['ledger']:30s} ₹{abs(txn['amount'] or 0):>12,.2f}  {txn['party'] or ''}")
        if len(result["transactions"]) > 30:
            output.append(f"  ... and {len(result['transactions']) - 30} more")

    elif rtype == "debtors":
        output.append("═══ SUNDRY DEBTORS (Outstanding) ═══")
        total = sum(bal for _, bal in result["data"])
        for name, bal in result["data"]:
            output.append(f"  {name:40s} ₹{bal:>12,.2f}")
        output.append(f"\n  TOTAL OUTSTANDING: ₹{total:,.2f}")

    elif rtype == "creditors":
        output.append("═══ SUNDRY CREDITORS (Outstanding) ═══")
        total = sum(bal for _, bal in result["data"])
        for name, bal in result["data"]:
            output.append(f"  {name:40s} ₹{bal:>12,.2f}")
        output.append(f"\n  TOTAL PAYABLE: ₹{total:,.2f}")

    elif rtype == "voucher_summary":
        output.append("═══ VOUCHER SUMMARY ═══")
        for vtype, count, total in result["data"]:
            output.append(f"  {vtype:20s} {count:>5d} vouchers  ₹{total or 0:>12,.2f}")

    elif rtype == "search":
        output.append("═══ SEARCH RESULTS ═══")
        for name, parent, bal in result["data"]:
            output.append(f"  {name:40s} ({parent})  ₹{bal or 0:>12,.2f}")

    elif rtype == "sql_result":
        output.append(f"Query returned {result['total']} rows")
        if result["columns"]:
            output.append("  " + " | ".join(result["columns"]))
            output.append("  " + "-" * 60)
            for row in result["rows"][:20]:
                output.append("  " + " | ".join(str(v) for v in row))
            if result["total"] > 20:
                output.append(f"  ... and {result['total'] - 20} more rows")

    elif rtype == "chat":
        output.append(result.get("message", ""))

    elif rtype == "error":
        output.append(f"Error: {result.get('message', 'Unknown error')}")

    return "\n".join(output)


# ── CLI CHAT ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print()
    print("  Seven Labs Vision — Tally AI Assistant")
    print("  ═══════════════════════════════════════")
    print(f"  Company: {_get_company_name()}")
    print("  Type your question. Type 'quit' to exit.")
    print()

    history = []

    while True:
        try:
            question = input("  You: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n  Goodbye!")
            break

        if not question:
            continue
        if question.lower() in ("quit", "exit", "q"):
            print("  Goodbye!")
            break

        result = ask(question, history)
        text = format_result_as_text(result)
        print()
        print(text)
        print()

        # Add to history
        history.append({"role": "user", "content": question})
        history.append({"role": "model", "content": text[:500]})  # keep history concise
