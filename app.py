"""
Seven Labs Vision — Interactive Tally Dashboard
Drill-down: Group → Ledger → Transactions
"""

import streamlit as st
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from tally_reports import (
    get_conn, profit_and_loss, balance_sheet, ledger_detail,
    pl_group_drilldown, debtor_aging, creditor_aging,
    get_all_groups_under, search_ledger,
    BS_ASSET_ROOTS, BS_LIABILITY_ROOTS,
    PL_INCOME_ROOTS, PL_EXPENSE_ROOTS,
)
from styles import inject_base_styles, page_header, section_header, metric_card, breadcrumb_html, fmt, fmt_full

st.set_page_config(page_title="Seven Labs Vision", page_icon="📊", layout="wide")

# ── STYLES ───────────────────────────────────────────────────────────────────
inject_base_styles()


# ── STATE MANAGEMENT ─────────────────────────────────────────────────────────

def init_state():
    if "view" not in st.session_state:
        st.session_state.view = "home"
    if "report" not in st.session_state:
        st.session_state.report = None
    if "drill_group" not in st.session_state:
        st.session_state.drill_group = None
    if "drill_ledger" not in st.session_state:
        st.session_state.drill_ledger = None

init_state()


def go_home():
    st.session_state.view = "home"
    st.session_state.report = None
    st.session_state.drill_group = None
    st.session_state.drill_ledger = None

def go_report(report_type):
    st.session_state.view = "report"
    st.session_state.report = report_type
    st.session_state.drill_group = None
    st.session_state.drill_ledger = None

def go_group(group_name):
    st.session_state.view = "group"
    st.session_state.drill_group = group_name
    st.session_state.drill_ledger = None

def go_ledger(ledger_name):
    st.session_state.view = "ledger"
    st.session_state.drill_ledger = ledger_name



# ── SIDEBAR ──────────────────────────────────────────────────────────────────

conn = get_conn()
company_row = conn.execute("SELECT value FROM _metadata WHERE key='company_name'").fetchone()
company_name = company_row[0] if company_row else "Unknown"

with st.sidebar:
    st.markdown(f"### 📊 Seven Labs Vision")
    st.markdown(f"**{company_name}**")
    st.markdown("---")

    if st.button("🏠 Home", use_container_width=True):
        go_home()
    if st.button("📋 Profit & Loss", use_container_width=True):
        go_report("pl")
    if st.button("📊 Balance Sheet", use_container_width=True):
        go_report("bs")
    if st.button("👥 Debtors Outstanding", use_container_width=True):
        go_report("debtors")
    if st.button("🏢 Creditors Outstanding", use_container_width=True):
        go_report("creditors")

    st.markdown("---")
    st.markdown("**Search Ledger**")
    search_q = st.text_input("", placeholder="Type party name...")
    if search_q:
        results = search_ledger(conn, search_q)
        for name, parent, bal in results[:8]:
            if st.button(f"{name} ({parent})", key=f"search_{name}"):
                go_report("pl")  # set report context
                go_ledger(name)

    st.markdown("---")
    stats = {
        "Ledgers": conn.execute("SELECT COUNT(*) FROM mst_ledger").fetchone()[0],
        "Vouchers": conn.execute("SELECT COUNT(*) FROM trn_voucher").fetchone()[0],
        "Entries": conn.execute("SELECT COUNT(*) FROM trn_accounting").fetchone()[0],
    }
    for label, val in stats.items():
        st.caption(f"{label}: {val:,}")


# ── BREADCRUMB ───────────────────────────────────────────────────────────────

def show_breadcrumb():
    parts = []
    report_names = {"pl": "Profit & Loss", "bs": "Balance Sheet",
                    "debtors": "Debtors", "creditors": "Creditors"}

    if st.session_state.report:
        parts.append(report_names.get(st.session_state.report, "Report"))
    if st.session_state.drill_group:
        parts.append(st.session_state.drill_group)
    if st.session_state.drill_ledger:
        parts.append(st.session_state.drill_ledger)

    if parts:
        cols = st.columns([1, 8])
        with cols[0]:
            if st.button("← Back"):
                if st.session_state.drill_ledger:
                    st.session_state.drill_ledger = None
                    st.session_state.view = "group"
                elif st.session_state.drill_group:
                    st.session_state.drill_group = None
                    st.session_state.view = "report"
                else:
                    go_home()
                st.rerun()
        with cols[1]:
            breadcrumb_html(parts)


# ── HOME VIEW ────────────────────────────────────────────────────────────────

def show_home():
    page_header("Dashboard", company_name)

    pl = profit_and_loss(conn)
    bs = balance_sheet(conn)

    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Revenue", fmt(pl["total_income"]))
    col2.metric("Expenses", fmt(pl["total_expense"]))
    profit_delta = "Profit" if pl["net_profit"] >= 0 else "Loss"
    col3.metric("Net Result", fmt(pl["net_profit"]), delta=profit_delta,
                delta_color="normal" if pl["net_profit"] >= 0 else "inverse")
    col4.metric("Total Assets", fmt(bs["total_assets"]))

    section_header("Overview")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Profit & Loss")
        if st.button("📋  Open Full P&L  →", use_container_width=True, key="home_pl"):
            go_report("pl")
            st.rerun()

        st.markdown("**Income**")
        for group, entries in pl["income"].items():
            total = sum(abs(a) for _, a in entries)
            if st.button(f"▸ {group}   —   {fmt(total)}", key=f"home_ig_{group}"):
                go_report("pl")
                go_group(group)
                st.rerun()

        st.markdown("**Expenses**")
        for group, entries in pl["expense"].items():
            total = sum(abs(a) for _, a in entries)
            if st.button(f"▸ {group}   —   {fmt(total)}", key=f"home_eg_{group}"):
                go_report("pl")
                go_group(group)
                st.rerun()

    with col2:
        st.markdown("### Balance Sheet")
        if st.button("📊  Open Full Balance Sheet  →", use_container_width=True, key="home_bs"):
            go_report("bs")
            st.rerun()

        st.markdown("**Assets**")
        for group, entries in bs["assets"].items():
            total = sum(abs(b) for _, b in entries)
            if st.button(f"▸ {group}   —   {fmt(total)}", key=f"home_ag_{group}"):
                go_report("bs")
                go_group(group)
                st.rerun()

        st.markdown("**Liabilities**")
        for group, entries in bs["liabilities"].items():
            total = sum(abs(b) for _, b in entries)
            if st.button(f"▸ {group}   —   {fmt(total)}", key=f"home_lg_{group}"):
                go_report("bs")
                go_group(group)
                st.rerun()


# ── P&L VIEW ─────────────────────────────────────────────────────────────────

def show_pl():
    page_header("Profit & Loss Account", company_name)

    pl = profit_and_loss(conn)

    # Totals bar
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Income", fmt(pl["total_income"]))
    col2.metric("Total Expenses", fmt(pl["total_expense"]))
    col3.metric("Gross Profit", fmt(pl["gross_profit"]),
                delta="Profit" if pl["gross_profit"] >= 0 else "Loss",
                delta_color="normal" if pl["gross_profit"] >= 0 else "inverse")
    col4.metric("Net Profit", fmt(pl["net_profit"]),
                delta="Profit" if pl["net_profit"] >= 0 else "Loss",
                delta_color="normal" if pl["net_profit"] >= 0 else "inverse")

    section_header("Breakdown")

    inc_col, exp_col = st.columns(2)

    with inc_col:
        st.markdown("### 📈 Income")
        for group, entries in pl["income"].items():
            group_total = sum(abs(a) for _, a in entries)
            st.markdown(f"**{group}** — {fmt(group_total)}")
            for ledger, amt in sorted(entries, key=lambda x: abs(x[1]), reverse=True):
                if st.button(f"  {ledger}   —   {fmt_full(amt)}", key=f"pl_i_{ledger}"):
                    go_ledger(ledger)
                    st.rerun()
            st.markdown("")

    with exp_col:
        st.markdown("### 📉 Expenses")
        for group, entries in pl["expense"].items():
            group_total = sum(abs(a) for _, a in entries)
            st.markdown(f"**{group}** — {fmt(group_total)}")
            for ledger, amt in sorted(entries, key=lambda x: abs(x[1]), reverse=True):
                if st.button(f"  {ledger}   —   {fmt_full(amt)}", key=f"pl_e_{ledger}"):
                    go_ledger(ledger)
                    st.rerun()
            st.markdown("")


# ── BALANCE SHEET VIEW ───────────────────────────────────────────────────────

def show_bs():
    page_header("Balance Sheet", company_name)

    bs = balance_sheet(conn)

    col1, col2 = st.columns(2)
    col1.metric("Total Assets", fmt(bs["total_assets"]))
    col2.metric("Total Liabilities", fmt(bs["total_liabilities"]))

    section_header("Details")

    a_col, l_col = st.columns(2)

    with a_col:
        st.markdown("### 🏦 Assets")
        for group, entries in bs["assets"].items():
            group_total = sum(abs(b) for _, b in entries)
            # Group header — clickable to expand
            if st.button(f"▸ {group}   —   {fmt(group_total)}   ({len(entries)} ledgers)",
                        key=f"bs_ag_{group}"):
                go_group(group)
                st.rerun()

    with l_col:
        st.markdown("### 📋 Liabilities")
        for group, entries in bs["liabilities"].items():
            group_total = sum(abs(b) for _, b in entries)
            if st.button(f"▸ {group}   —   {fmt(group_total)}   ({len(entries)} ledgers)",
                        key=f"bs_lg_{group}"):
                go_group(group)
                st.rerun()


# ── GROUP DRILL-DOWN VIEW ───────────────────────────────────────────────────

def show_group():
    group_name = st.session_state.drill_group
    page_header(group_name, "Ledger-wise breakup — Click any ledger to see transactions")

    # Get ledgers in this group
    rows = conn.execute("""
        SELECT NAME, CAST(CLOSINGBALANCE AS REAL) as balance
        FROM mst_ledger
        WHERE PARENT = ?
        ORDER BY ABS(CAST(CLOSINGBALANCE AS REAL)) DESC
    """, (group_name,)).fetchall()

    if not rows:
        # Maybe it's a parent group — check subgroups
        all_groups = get_all_groups_under(conn, [group_name])
        all_groups.discard(group_name)
        if all_groups:
            st.markdown("**Subgroups:**")
            for sg in sorted(all_groups):
                sg_total = conn.execute("""
                    SELECT SUM(ABS(CAST(CLOSINGBALANCE AS REAL)))
                    FROM mst_ledger WHERE PARENT = ?
                """, (sg,)).fetchone()[0] or 0
                if sg_total > 0:
                    if st.button(f"▸ {sg}   —   {fmt(sg_total)}", key=f"sg_{sg}"):
                        go_group(sg)
                        st.rerun()
        return

    total = sum(abs(b or 0) for _, b in rows)
    st.metric(f"Total — {group_name}", fmt(total), f"{len(rows)} ledgers")
    section_header("Ledgers")

    # Show all ledgers as clickable buttons
    for name, balance in rows:
        bal = balance or 0
        if bal == 0:
            continue
        if st.button(f"{'📗' if bal < 0 else '📕'}  {name}   —   {fmt_full(bal)}",
                    key=f"grp_l_{name}"):
            go_ledger(name)
            st.rerun()


# ── LEDGER TRANSACTION VIEW ─────────────────────────────────────────────────

def show_ledger():
    ledger_name = st.session_state.drill_ledger
    page_header(ledger_name)

    # Get ledger info
    info = conn.execute("""
        SELECT PARENT, CAST(OPENINGBALANCE AS REAL), CAST(CLOSINGBALANCE AS REAL)
        FROM mst_ledger WHERE NAME = ?
    """, (ledger_name,)).fetchone()

    if info:
        parent, opening, closing = info
        st.caption(f"Group: {parent}")

        col1, col2, col3 = st.columns(3)
        col1.metric("Opening Balance", fmt_full(opening or 0))
        col2.metric("Closing Balance", fmt_full(closing or 0))
        movement = (closing or 0) - (opening or 0)
        col3.metric("Net Movement", fmt_full(movement),
                    delta=f"{'Dr' if movement < 0 else 'Cr'}",
                    delta_color="off")

    section_header("Transactions")

    # Get all transactions
    opening_bal, transactions, closing_bal = ledger_detail(conn, ledger_name)

    if not transactions:
        st.info("No transactions found for this ledger.")
        return

    st.markdown(f"**{len(transactions)} transactions**")

    # Build dataframe
    rows = []
    for txn in transactions:
        date = txn["date"]
        if date and len(date) == 8:
            date = f"{date[6:8]}/{date[4:6]}/{date[:4]}"
        rows.append({
            "Date": date,
            "Type": txn["voucher_type"],
            "Vch No": txn["voucher_number"],
            "Party / Narration": txn["party"] or txn["narration"] or "",
            "Debit (₹)": txn["debit"] if txn["debit"] else None,
            "Credit (₹)": txn["credit"] if txn["credit"] else None,
            "Balance (₹)": txn["balance"],
        })

    df = pd.DataFrame(rows)

    st.dataframe(
        df.style.format({
            "Debit (₹)": lambda x: f"{x:,.2f}" if pd.notna(x) and x else "",
            "Credit (₹)": lambda x: f"{x:,.2f}" if pd.notna(x) and x else "",
            "Balance (₹)": "{:,.2f}",
        }),
        use_container_width=True,
        height=min(len(rows) * 35 + 40, 600),
    )

    # Summary at bottom
    total_dr = sum(t["debit"] for t in transactions if t["debit"])
    total_cr = sum(t["credit"] for t in transactions if t["credit"])
    section_header("Summary")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Debit", fmt_full(total_dr))
    col2.metric("Total Credit", fmt_full(total_cr))
    col3.metric("Closing Balance", fmt_full(closing_bal))


# ── DEBTORS / CREDITORS VIEW ────────────────────────────────────────────────

def show_debtors_creditors(report_type):
    is_debtors = report_type == "debtors"
    title = "Sundry Debtors — Outstanding Receivables" if is_debtors else "Sundry Creditors — Outstanding Payables"
    page_header(title, f"{company_name} — Click any party to see transactions")

    data = debtor_aging(conn) if is_debtors else creditor_aging(conn)

    if not data:
        st.info("No outstanding balances found.")
        return

    total = sum(b for _, b in data)
    st.metric(f"Total {'Receivable' if is_debtors else 'Payable'}",
              fmt(total), f"{len(data)} parties")
    section_header("Parties")

    # Sort by amount descending
    data_sorted = sorted(data, key=lambda x: x[1], reverse=True)

    for name, bal in data_sorted:
        if st.button(f"{'👤' if is_debtors else '🏢'}  {name}   —   {fmt_full(bal)}",
                    key=f"dc_{name}"):
            go_ledger(name)
            st.rerun()


# ── CHAT HANDLER ─────────────────────────────────────────────────────────────

def handle_chat(question):
    """Route a chat question to the appropriate view."""
    from chat_engine import classify_intent, execute_action
    action_data = classify_intent(question)
    action = action_data.get("action", "")
    params = action_data.get("params", {})

    if action == "report_pl":
        go_report("pl")
    elif action == "report_bs":
        go_report("bs")
    elif action == "report_tb":
        go_report("tb")
    elif action == "debtors":
        go_report("debtors")
    elif action == "creditors":
        go_report("creditors")
    elif action == "ledger_detail":
        ledger_name = params.get("ledger_name", "")
        if ledger_name:
            go_report("pl")
            go_ledger(ledger_name)
    elif action == "pl_drilldown":
        group_name = params.get("group_name", "")
        if group_name:
            go_report("pl")
            go_group(group_name)
    elif action == "search":
        query = params.get("query", "")
        results = search_ledger(conn, query)
        if results and len(results) == 1:
            go_report("pl")
            go_ledger(results[0][0])
        else:
            st.session_state.chat_results = results
            st.session_state.chat_query = query
    elif action == "voucher_summary":
        st.session_state.show_voucher_summary = True
    elif action == "chat":
        st.session_state.chat_message = params.get("response", "")

    return action_data


# ── MAIN ROUTER ──────────────────────────────────────────────────────────────

show_breadcrumb()

view = st.session_state.view

if view == "ledger" and st.session_state.drill_ledger:
    show_ledger()
elif view == "group" and st.session_state.drill_group:
    show_group()
elif view == "report" and st.session_state.report == "pl":
    show_pl()
elif view == "report" and st.session_state.report == "bs":
    show_bs()
elif view == "report" and st.session_state.report in ("debtors", "creditors"):
    show_debtors_creditors(st.session_state.report)
else:
    show_home()

# Show search results from chat if any
if "chat_results" in st.session_state and st.session_state.chat_results:
    st.markdown("")
    st.markdown(f"**Search results for: {st.session_state.get('chat_query', '')}**")
    for name, parent, bal in st.session_state.chat_results:
        if st.button(f"{name} ({parent}) — {fmt_full(bal)}", key=f"chat_sr_{name}"):
            go_report("pl")
            go_ledger(name)
            del st.session_state["chat_results"]
            del st.session_state["chat_query"]
            st.rerun()

# Show chat message if any
if "chat_message" in st.session_state and st.session_state.chat_message:
    st.markdown("")
    st.info(st.session_state.chat_message)
    del st.session_state["chat_message"]

# Show voucher summary if requested
if st.session_state.get("show_voucher_summary"):
    st.markdown("")
    from tally_reports import voucher_summary
    data = voucher_summary(conn)
    if data:
        st.markdown("### Voucher Summary")
        df = pd.DataFrame(data, columns=["Voucher Type", "Count", "Amount"])
        st.dataframe(df.style.format({"Amount": "₹{:,.2f}"}), use_container_width=True)
    del st.session_state["show_voucher_summary"]

conn.close()

# ── PERSISTENT CHAT BAR AT BOTTOM ────────────────────────────────────────────

st.markdown("")
chat_input = st.chat_input("Ask anything — P&L, Balance Sheet, ledger of [party], debtors, creditors...")
if chat_input:
    from chat_engine import ask, format_result_as_text
    q_lower = chat_input.lower().strip()

    # Only use navigation for EXPLICIT commands like "show me P&L", "open balance sheet"
    # These are view-switching requests, not conversational questions
    nav_triggers = ["show me", "open ", "go to ", "switch to ", "navigate to ",
                    "display ", "pull up ", "take me to "]
    is_navigation = any(q_lower.startswith(t) for t in nav_triggers)

    if is_navigation:
        conn2 = get_conn()
        action_data = handle_chat(chat_input)
        conn2.close()
        if action_data.get("action") not in ("chat", None, ""):
            st.rerun()
        else:
            # Navigation didn't match, fall through to Gemini
            result = ask(chat_input)
            text = result.get("message") or format_result_as_text(result)
            st.session_state.chat_response = text
            st.session_state.chat_question = chat_input
    else:
        # Conversational question — Gemini first, then smart_answer, then classifier
        result = ask(chat_input)
        text = result.get("message") or format_result_as_text(result)
        st.session_state.chat_response = text
        st.session_state.chat_question = chat_input

# Show conversational response if any
if st.session_state.get("chat_response"):
    st.markdown(f"**You:** {st.session_state.get('chat_question', '')}")
    st.markdown(st.session_state.chat_response)
    del st.session_state["chat_response"]
    if "chat_question" in st.session_state:
        del st.session_state["chat_question"]
