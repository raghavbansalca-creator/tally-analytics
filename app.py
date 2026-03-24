"""
Seven Labs Vision -- Interactive Tally Dashboard
Drill-down: Group -> Ledger -> Transactions
DEFENSIVE CODING: Works with ANY company's Tally data.
"""

import streamlit as st
import pandas as pd
import sys
import os
import datetime

sys.path.insert(0, os.path.dirname(__file__))
from tally_reports import (
    get_conn, profit_and_loss, balance_sheet, ledger_detail,
    pl_group_drilldown, debtor_aging, creditor_aging,
    get_all_groups_under, search_ledger, _bal_col,
    BS_ASSET_ROOTS, BS_LIABILITY_ROOTS,
    PL_INCOME_ROOTS, PL_EXPENSE_ROOTS,
    voucher_summary, stock_summary, godown_summary,
)
from sidebar_filters import render_sidebar_filters

st.set_page_config(page_title="Seven Labs Vision", page_icon="", layout="wide")

# -- STYLES -------------------------------------------------------------------
from styles import inject_base_styles, fmt as sfmt, page_header, section_header, metric_card, sidebar_company_card, sidebar_section_label
inject_base_styles()


# -- SAFE HELPERS -------------------------------------------------------------

def _safe_cols(conn, table):
    """Return set of column names for a table."""
    try:
        return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        return set()


def _safe_parse_date(date_str, fallback=None):
    """Safely parse YYYYMMDD date string."""
    try:
        if date_str and len(date_str) >= 8:
            return datetime.date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
    except (ValueError, TypeError):
        pass
    return fallback


# -- STATE MANAGEMENT ---------------------------------------------------------

def init_state():
    for key, default in [("view", "home"), ("report", None), ("drill_group", None), ("drill_ledger", None)]:
        if key not in st.session_state:
            st.session_state[key] = default

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


def fmt(amount):
    """Format amount in Indian numbering."""
    if amount is None:
        return "Rs 0"
    abs_amt = abs(amount)
    if abs_amt >= 10000000:
        return f"Rs {abs_amt/10000000:,.2f} Cr"
    elif abs_amt >= 100000:
        return f"Rs {abs_amt/100000:,.2f} L"
    elif abs_amt >= 1000:
        return f"Rs {abs_amt:,.0f}"
    else:
        return f"Rs {abs_amt:,.2f}"


def fmt_full(amount):
    """Full formatted amount."""
    if amount is None:
        return "Rs 0.00"
    return f"Rs {abs(amount):,.2f}"


# -- SIDEBAR ------------------------------------------------------------------

conn = get_conn()

# Safe company name
try:
    company_row = conn.execute("SELECT value FROM _metadata WHERE key='company_name'").fetchone()
    company_name = company_row[0] if company_row else "Unknown"
except Exception:
    company_name = "Unknown"

with st.sidebar:
    st.markdown("#### Seven Labs Vision")

    _profile_info = {"entity_type": "", "business_nature": "", "complexity": ""}
    try:
        _profile_rows = conn.execute("SELECT key, value FROM _company_profile").fetchall()
        for k, v in _profile_rows:
            if k in _profile_info:
                _profile_info[k] = v
    except Exception:
        pass
    sidebar_company_card(
        company_name,
        entity_type=_profile_info["entity_type"],
        business_nature=_profile_info["business_nature"],
        complexity=_profile_info["complexity"],
    )

    sidebar_section_label("REPORTS")
    if st.button("Dashboard", use_container_width=True, key="nav_home"):
        go_home()
    if st.button("Profit & Loss", use_container_width=True, key="nav_pl"):
        go_report("pl")
    if st.button("Balance Sheet", use_container_width=True, key="nav_bs"):
        go_report("bs")
    if st.button("Debtors Outstanding", use_container_width=True, key="nav_debtors"):
        go_report("debtors")
    if st.button("Creditors Outstanding", use_container_width=True, key="nav_creditors"):
        go_report("creditors")

    st.markdown("---")

    # -- DATE FILTER (safe) --
    _min_date_row = conn.execute("SELECT MIN(DATE) FROM trn_voucher").fetchone()
    _max_date_row = conn.execute("SELECT MAX(DATE) FROM trn_voucher").fetchone()
    _min_dt = _safe_parse_date(_min_date_row[0] if _min_date_row else None, fallback=datetime.date(2025, 4, 1))
    _max_dt = _safe_parse_date(_max_date_row[0] if _max_date_row else None, fallback=datetime.date.today())
    if "global_start_date" not in st.session_state:
        st.session_state.global_start_date = _min_dt
    if "global_end_date" not in st.session_state:
        st.session_state.global_end_date = _max_dt

    with st.expander(f"Date: {st.session_state.global_start_date.strftime('%d %b %Y')} - {st.session_state.global_end_date.strftime('%d %b %Y')}", expanded=False):
        _from = st.date_input("From", value=st.session_state.global_start_date, min_value=_min_dt, max_value=_max_dt, key="app_filter_from")
        _to = st.date_input("To", value=st.session_state.global_end_date, min_value=_min_dt, max_value=_max_dt, key="app_filter_to")
        st.session_state.global_start_date = _from
        st.session_state.global_end_date = _to
        if st.button("Reset to Full Period", key="app_reset_dates"):
            st.session_state.global_start_date = _min_dt
            st.session_state.global_end_date = _max_dt
            st.rerun()

    DATE_FROM = st.session_state.global_start_date.strftime("%Y%m%d")
    DATE_TO = st.session_state.global_end_date.strftime("%Y%m%d")

    with st.expander("Search Ledger", expanded=False):
        search_q = st.text_input("Search", placeholder="Type party name...", label_visibility="collapsed", key="app_search")
        if search_q:
            try:
                results = search_ledger(conn, search_q)
                for name, parent, bal in (results or [])[:8]:
                    if st.button(f"{name} ({parent})", key=f"search_{name}"):
                        go_report("pl")
                        go_ledger(name)
            except Exception:
                pass

    try:
        _ledger_count = conn.execute("SELECT COUNT(*) FROM mst_ledger").fetchone()[0]
        _voucher_count = conn.execute("SELECT COUNT(*) FROM trn_voucher").fetchone()[0]
        _entry_count = conn.execute("SELECT COUNT(*) FROM trn_accounting").fetchone()[0]
        st.caption(f"{_ledger_count:,} ledgers  |  {_voucher_count:,} vouchers  |  {_entry_count:,} entries")
    except Exception:
        st.caption("Data loaded")

_filters = render_sidebar_filters(conn, page_key="app")
_vch_types_filter = _filters.get("voucher_types")
_ledger_groups_filter = _filters.get("ledger_groups")


# -- BREADCRUMB ---------------------------------------------------------------

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
            if st.button("< Back"):
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
            st.markdown(f'<div class="breadcrumb">{"  >  ".join(parts)}</div>', unsafe_allow_html=True)


# -- HOME VIEW ----------------------------------------------------------------

def show_home():
    st.markdown(f'<div class="report-title">Dashboard</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="company-name">{company_name}</div>', unsafe_allow_html=True)

    try:
        pl = profit_and_loss(conn, from_date=DATE_FROM, to_date=DATE_TO,
                             voucher_types=_vch_types_filter, ledger_groups=_ledger_groups_filter)
        bs = balance_sheet(conn, date_from=DATE_FROM, date_to=DATE_TO)
    except Exception as e:
        st.warning(f"Could not load financial data: {e}")
        return

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Revenue", fmt(pl.get("total_income", 0)))
    col2.metric("Expenses", fmt(pl.get("total_expense", 0)))
    net_profit = pl.get("net_profit", 0) or 0
    col3.metric("Net Result", fmt(net_profit),
                delta="Profit" if net_profit >= 0 else "Loss",
                delta_color="normal" if net_profit >= 0 else "inverse")
    col4.metric("Total Assets", fmt(bs.get("total_assets", 0)))

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Profit & Loss")
        if st.button("Open Full P&L", use_container_width=True, key="home_pl"):
            go_report("pl")
            st.rerun()
        st.markdown("**Income**")
        for group, entries in pl.get("income", {}).items():
            entries = entries or []
            total = sum(abs(a) for _, a in entries)
            if st.button(f"> {group}   --   {fmt(total)}", key=f"home_ig_{group}"):
                go_report("pl"); go_group(group); st.rerun()
        st.markdown("**Expenses**")
        for group, entries in pl.get("expense", {}).items():
            entries = entries or []
            total = sum(abs(a) for _, a in entries)
            if st.button(f"> {group}   --   {fmt(total)}", key=f"home_eg_{group}"):
                go_report("pl"); go_group(group); st.rerun()

    with col2:
        st.markdown("### Balance Sheet")
        if st.button("Open Full Balance Sheet", use_container_width=True, key="home_bs"):
            go_report("bs"); st.rerun()
        st.markdown("**Assets**")
        for group, entries in bs.get("assets", {}).items():
            entries = entries or []
            total = sum(abs(b) for _, b in entries)
            if st.button(f"> {group}   --   {fmt(total)}", key=f"home_ag_{group}"):
                go_report("bs"); go_group(group); st.rerun()
        st.markdown("**Liabilities**")
        for group, entries in bs.get("liabilities", {}).items():
            entries = entries or []
            total = sum(abs(b) for _, b in entries)
            if st.button(f"> {group}   --   {fmt(total)}", key=f"home_lg_{group}"):
                go_report("bs"); go_group(group); st.rerun()

    # -- VOUCHER SUMMARY --
    try:
        st.markdown("---")
        st.subheader("Voucher Summary")
        _vch_data = voucher_summary(conn, from_date=DATE_FROM, to_date=DATE_TO, voucher_types=_vch_types_filter)
        if _vch_data:
            _vch_df = pd.DataFrame(_vch_data, columns=["Voucher Type", "Count", "Amount"])
            _vch_df["Amount"] = _vch_df["Amount"].apply(lambda x: x if x else 0)
            _vch_df["Formatted Amount"] = _vch_df["Amount"].apply(fmt)
            _num_cols = min(len(_vch_data), 4)
            if _num_cols > 0:
                _vch_cols = st.columns(_num_cols)
                for _vi, (_vtype, _vcnt, _vamt) in enumerate(_vch_data[:4]):
                    _vch_cols[_vi].metric(_vtype or "Unknown", f"{_vcnt:,}", fmt(_vamt or 0))
            if len(_vch_data) > 4:
                with st.expander(f"All {len(_vch_data)} voucher types"):
                    st.dataframe(_vch_df[["Voucher Type", "Count", "Formatted Amount"]].rename(
                        columns={"Formatted Amount": "Total Amount"}), use_container_width=True, hide_index=True)
    except Exception as e:
        st.warning(f"Could not load voucher summary: {e}")

    # -- STOCK --
    try:
        if _filters.get("has_stock"):
            st.markdown("---")
            st.subheader("Inventory Overview")
            _stock_data = stock_summary(conn)
            if _stock_data:
                _stock_df = pd.DataFrame(_stock_data)
                _total_stock_value = sum(s.get("closing_value", 0) or 0 for s in _stock_data)
                _stock_groups = list(set(s.get("group", "") for s in _stock_data if s.get("group")))
                _st_c1, _st_c2, _st_c3 = st.columns(3)
                _st_c1.metric("Stock Items", f"{len(_stock_data)}")
                _st_c2.metric("Stock Groups", f"{len(_stock_groups)}")
                _st_c3.metric("Total Stock Value", fmt(_total_stock_value))
                if _filters.get("has_godowns"):
                    _godown_data = godown_summary(conn)
                    if _godown_data:
                        st.markdown(f"**Godowns:** {', '.join(g.get('name', '') for g in _godown_data)}")
                with st.expander("Stock Item Details"):
                    _stock_df["closing_value"] = _stock_df["closing_value"].apply(fmt)
                    st.dataframe(_stock_df[["name", "group", "closing_value"]].rename(
                        columns={"name": "Item", "group": "Group", "closing_value": "Closing Value"}),
                        use_container_width=True, hide_index=True)
    except Exception as e:
        st.warning(f"Could not load inventory: {e}")


# -- P&L VIEW -----------------------------------------------------------------

def show_pl():
    st.markdown(f'<div class="report-title">Profit & Loss Account</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="company-name">{company_name}</div>', unsafe_allow_html=True)
    try:
        pl = profit_and_loss(conn, from_date=DATE_FROM, to_date=DATE_TO,
                             voucher_types=_vch_types_filter, ledger_groups=_ledger_groups_filter)
    except Exception as e:
        st.error(f"Could not load P&L: {e}"); return

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Income", fmt(pl.get("total_income", 0)))
    col2.metric("Total Expenses", fmt(pl.get("total_expense", 0)))
    gp = pl.get("gross_profit", 0) or 0
    col3.metric("Gross Profit", fmt(gp), delta="Profit" if gp >= 0 else "Loss",
                delta_color="normal" if gp >= 0 else "inverse")
    np_val = pl.get("net_profit", 0) or 0
    col4.metric("Net Profit", fmt(np_val), delta="Profit" if np_val >= 0 else "Loss",
                delta_color="normal" if np_val >= 0 else "inverse")
    st.markdown("---")

    inc_col, exp_col = st.columns(2)
    with inc_col:
        st.markdown("### Income")
        for group, entries in pl.get("income", {}).items():
            entries = entries or []
            group_total = sum(abs(a) for _, a in entries)
            st.markdown(f"**{group}** -- {fmt(group_total)}")
            for ledger, amt in sorted(entries, key=lambda x: abs(x[1]), reverse=True):
                if st.button(f"  {ledger}   --   {fmt_full(amt)}", key=f"pl_i_{ledger}"):
                    go_ledger(ledger); st.rerun()
            st.markdown("")
    with exp_col:
        st.markdown("### Expenses")
        for group, entries in pl.get("expense", {}).items():
            entries = entries or []
            group_total = sum(abs(a) for _, a in entries)
            st.markdown(f"**{group}** -- {fmt(group_total)}")
            for ledger, amt in sorted(entries, key=lambda x: abs(x[1]), reverse=True):
                if st.button(f"  {ledger}   --   {fmt_full(amt)}", key=f"pl_e_{ledger}"):
                    go_ledger(ledger); st.rerun()
            st.markdown("")


# -- BALANCE SHEET VIEW -------------------------------------------------------

def show_bs():
    st.markdown(f'<div class="report-title">Balance Sheet</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="company-name">{company_name}</div>', unsafe_allow_html=True)
    try:
        bs = balance_sheet(conn, date_from=DATE_FROM, date_to=DATE_TO)
    except Exception as e:
        st.error(f"Could not load Balance Sheet: {e}"); return

    col1, col2 = st.columns(2)
    col1.metric("Total Assets", fmt(bs.get("total_assets", 0)))
    col2.metric("Total Liabilities", fmt(bs.get("total_liabilities", 0)))
    st.markdown("---")

    a_col, l_col = st.columns(2)
    with a_col:
        st.markdown("### Assets")
        for group, entries in bs.get("assets", {}).items():
            entries = entries or []
            group_total = sum(abs(b) for _, b in entries)
            if st.button(f"> {group}   --   {fmt(group_total)}   ({len(entries)} ledgers)", key=f"bs_ag_{group}"):
                go_group(group); st.rerun()
    with l_col:
        st.markdown("### Liabilities")
        for group, entries in bs.get("liabilities", {}).items():
            entries = entries or []
            group_total = sum(abs(b) for _, b in entries)
            if st.button(f"> {group}   --   {fmt(group_total)}   ({len(entries)} ledgers)", key=f"bs_lg_{group}"):
                go_group(group); st.rerun()


# -- GROUP DRILL-DOWN VIEW ----------------------------------------------------

def show_group():
    group_name = st.session_state.drill_group
    st.markdown(f'<div class="report-title">{group_name}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="company-name">Ledger-wise breakup</div>', unsafe_allow_html=True)

    try:
        bc = _bal_col(conn)
        all_sub_groups = list(get_all_groups_under(conn, [group_name]))
        _ph = ",".join(["?"] * len(all_sub_groups))
        rows = conn.execute(f"""
            SELECT NAME, CAST({bc} AS REAL) as balance
            FROM mst_ledger WHERE PARENT IN ({_ph})
            ORDER BY ABS(CAST({bc} AS REAL)) DESC
        """, all_sub_groups).fetchall()
    except Exception as e:
        st.error(f"Could not load group: {e}"); return

    if not rows:
        try:
            bc = _bal_col(conn)
            all_groups = get_all_groups_under(conn, [group_name])
            all_groups.discard(group_name)
            if all_groups:
                st.markdown("**Subgroups:**")
                for sg in sorted(all_groups):
                    _sg_subs = list(get_all_groups_under(conn, [sg]))
                    _sg_ph = ",".join(["?"] * len(_sg_subs))
                    sg_row = conn.execute(f"SELECT SUM(ABS(CAST({bc} AS REAL))) FROM mst_ledger WHERE PARENT IN ({_sg_ph})", _sg_subs).fetchone()
                    sg_total = (sg_row[0] if sg_row else 0) or 0
                    if sg_total > 0:
                        if st.button(f"> {sg}   --   {fmt(sg_total)}", key=f"sg_{sg}"):
                            go_group(sg); st.rerun()
        except Exception:
            pass
        return

    total = sum(abs(b or 0) for _, b in rows)
    st.metric(f"Total -- {group_name}", fmt(total), f"{len(rows)} ledgers")
    st.markdown("---")
    for name, balance in rows:
        bal = balance or 0
        if bal == 0:
            continue
        if st.button(f"  {name}   --   {fmt_full(bal)}", key=f"grp_l_{name}"):
            go_ledger(name); st.rerun()


# -- LEDGER TRANSACTION VIEW --------------------------------------------------

def show_ledger():
    ledger_name = st.session_state.drill_ledger
    st.markdown(f'<div class="report-title">{ledger_name}</div>', unsafe_allow_html=True)

    try:
        bc = _bal_col(conn)
        info = conn.execute(f"SELECT PARENT, CAST(OPENINGBALANCE AS REAL), CAST({bc} AS REAL) FROM mst_ledger WHERE NAME = ?", (ledger_name,)).fetchone()
    except Exception:
        info = None

    if info:
        parent, opening, closing = info
        st.markdown(f'<div class="company-name">Group: {parent}</div>', unsafe_allow_html=True)
        col1, col2, col3 = st.columns(3)
        col1.metric("Opening Balance", fmt_full(opening or 0))
        col2.metric("Closing Balance", fmt_full(closing or 0))
        movement = (closing or 0) - (opening or 0)
        col3.metric("Net Movement", fmt_full(movement), delta=f"{'Dr' if movement < 0 else 'Cr'}", delta_color="off")

    st.markdown("---")
    try:
        opening_bal, transactions, closing_bal = ledger_detail(conn, ledger_name, from_date=DATE_FROM, to_date=DATE_TO, voucher_types=_vch_types_filter)
    except Exception as e:
        st.warning(f"Could not load transactions: {e}"); return

    if not transactions:
        st.info("No transactions found for this ledger."); return

    st.markdown(f"**{len(transactions)} transactions**")
    rows = []
    for txn in transactions:
        date = txn.get("date", "")
        if date and len(date) == 8:
            date = f"{date[6:8]}/{date[4:6]}/{date[:4]}"
        rows.append({
            "Date": date, "Type": txn.get("voucher_type", ""), "Vch No": txn.get("voucher_number", ""),
            "Party / Narration": txn.get("party") or txn.get("narration") or "",
            "Debit (Rs)": txn.get("debit") if txn.get("debit") else None,
            "Credit (Rs)": txn.get("credit") if txn.get("credit") else None,
            "Balance (Rs)": txn.get("balance", 0),
        })
    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df.style.format({
            "Debit (Rs)": lambda x: f"{x:,.2f}" if pd.notna(x) and x else "",
            "Credit (Rs)": lambda x: f"{x:,.2f}" if pd.notna(x) and x else "",
            "Balance (Rs)": "{:,.2f}",
        }), use_container_width=True, height=min(len(rows) * 35 + 40, 600))

    total_dr = sum(t.get("debit", 0) or 0 for t in transactions)
    total_cr = sum(t.get("credit", 0) or 0 for t in transactions)
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Debit", fmt_full(total_dr))
    col2.metric("Total Credit", fmt_full(total_cr))
    col3.metric("Closing Balance", fmt_full(closing_bal))


# -- DEBTORS / CREDITORS VIEW -------------------------------------------------

def show_debtors_creditors(report_type):
    is_debtors = report_type == "debtors"
    title = "Sundry Debtors -- Outstanding Receivables" if is_debtors else "Sundry Creditors -- Outstanding Payables"
    st.markdown(f'<div class="report-title">{title}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="company-name">{company_name}</div>', unsafe_allow_html=True)

    try:
        data = debtor_aging(conn, date_from=DATE_FROM, date_to=DATE_TO) if is_debtors else creditor_aging(conn, date_from=DATE_FROM, date_to=DATE_TO)
    except Exception as e:
        st.warning(f"Could not load data: {e}"); return

    if not data:
        st.info("No outstanding balances found."); return

    total = sum(b for _, b in data)
    st.metric(f"Total {'Receivable' if is_debtors else 'Payable'}", fmt(total), f"{len(data)} parties")
    st.markdown("---")
    for name, bal in sorted(data, key=lambda x: x[1], reverse=True):
        if st.button(f"  {name}   --   {fmt_full(bal)}", key=f"dc_{name}"):
            go_ledger(name); st.rerun()


# -- CHAT HANDLER -------------------------------------------------------------

def handle_chat(question):
    try:
        from chat_engine import classify_intent, execute_action
        action_data = classify_intent(question)
    except Exception:
        return {"action": "chat", "params": {"response": "Chat engine not available."}}
    action = action_data.get("action", "")
    params = action_data.get("params", {})

    if action == "report_pl": go_report("pl")
    elif action == "report_bs": go_report("bs")
    elif action == "report_tb": go_report("tb")
    elif action == "debtors": go_report("debtors")
    elif action == "creditors": go_report("creditors")
    elif action == "ledger_detail":
        if params.get("ledger_name"): go_report("pl"); go_ledger(params["ledger_name"])
    elif action == "pl_drilldown":
        if params.get("group_name"): go_report("pl"); go_group(params["group_name"])
    elif action == "search":
        results = search_ledger(conn, params.get("query", ""))
        if results and len(results) == 1: go_report("pl"); go_ledger(results[0][0])
        else: st.session_state.chat_results = results; st.session_state.chat_query = params.get("query", "")
    elif action == "voucher_summary": st.session_state.show_voucher_summary = True
    elif action == "chat": st.session_state.chat_message = params.get("response", "")
    return action_data


# -- MAIN ROUTER --------------------------------------------------------------

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

# Chat results
if "chat_results" in st.session_state and st.session_state.chat_results:
    st.markdown("---")
    st.markdown(f"**Search results for: {st.session_state.get('chat_query', '')}**")
    for name, parent, bal in st.session_state.chat_results:
        if st.button(f"{name} ({parent}) -- {fmt_full(bal)}", key=f"chat_sr_{name}"):
            go_report("pl"); go_ledger(name)
            del st.session_state["chat_results"]; del st.session_state["chat_query"]; st.rerun()

if "chat_message" in st.session_state and st.session_state.chat_message:
    st.markdown("---"); st.info(st.session_state.chat_message); del st.session_state["chat_message"]

if st.session_state.get("show_voucher_summary"):
    st.markdown("---")
    try:
        data = voucher_summary(conn, from_date=DATE_FROM, to_date=DATE_TO)
        if data:
            st.markdown("### Voucher Summary")
            df = pd.DataFrame(data, columns=["Voucher Type", "Count", "Amount"])
            st.dataframe(df.style.format({"Amount": "Rs {:,.2f}"}), use_container_width=True)
    except Exception:
        pass
    del st.session_state["show_voucher_summary"]

conn.close()

# -- CHAT BAR --
st.markdown("---")
chat_input = st.chat_input("Ask anything -- P&L, Balance Sheet, ledger of [party], debtors, creditors...")
if chat_input:
    try:
        from chat_engine import ask, format_result_as_text
        q_lower = chat_input.lower().strip()
        nav_triggers = ["show me", "open ", "go to ", "switch to ", "navigate to ", "display ", "pull up ", "take me to "]
        is_navigation = any(q_lower.startswith(t) for t in nav_triggers)
        if is_navigation:
            conn2 = get_conn(); action_data = handle_chat(chat_input); conn2.close()
            if action_data.get("action") not in ("chat", None, ""): st.rerun()
            else:
                result = ask(chat_input); text = result.get("message") or format_result_as_text(result)
                st.session_state.chat_response = text; st.session_state.chat_question = chat_input
        else:
            result = ask(chat_input); text = result.get("message") or format_result_as_text(result)
            st.session_state.chat_response = text; st.session_state.chat_question = chat_input
    except Exception as e:
        st.session_state.chat_response = f"Error: {e}"; st.session_state.chat_question = chat_input

if st.session_state.get("chat_response"):
    st.markdown(f"**You:** {st.session_state.get('chat_question', '')}")
    st.markdown(st.session_state.chat_response)
    del st.session_state["chat_response"]
    if "chat_question" in st.session_state: del st.session_state["chat_question"]
