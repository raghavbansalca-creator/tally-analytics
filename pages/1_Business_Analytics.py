"""
Seven Labs Vision — Comprehensive Business Analytics Dashboard
Month-on-month analysis of Sales, Purchases, Expenses, Bank, Cash Flow & Projections.
Interactive drill-down into invoices, vouchers, parties, and bank statements.
"""

import streamlit as st
import pandas as pd
import sys, os
import plotly.graph_objects as go

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from analytics import (
    get_conn, monthly_sales, monthly_purchases, monthly_receipts_payments,
    monthly_gross_profit, monthly_expenses, top_customers_by_sales,
    top_suppliers_by_purchase, customer_monthly_sales, bank_balances,
    monthly_bank_movement, cash_flow_statement, project_cash_flow,
    working_capital_analysis, key_ratios, collection_efficiency,
    drill_monthly_invoices, drill_party_invoices, drill_voucher_entries,
    drill_voucher_header, drill_expense_transactions,
    drill_receipt_payment_vouchers, drill_bank_transactions,
)
from styles import inject_base_styles, page_header, section_header, metric_card, fmt, fmt_full

st.set_page_config(page_title="Business Analytics — SLV", page_icon="📈", layout="wide")
inject_base_styles()

MONTH_LABELS = {
    "202504": "Apr 25", "202505": "May 25", "202506": "Jun 25",
    "202507": "Jul 25", "202508": "Aug 25", "202509": "Sep 25",
    "202510": "Oct 25", "202511": "Nov 25", "202512": "Dec 25",
    "202601": "Jan 26", "202602": "Feb 26", "202603": "Mar 26",
    "202604": "Apr 26",
}

def ml(month_code):
    return MONTH_LABELS.get(month_code, month_code)

def fmt_date(date_str):
    """Convert YYYYMMDD to DD/MM/YYYY."""
    if not date_str or len(date_str) < 8:
        return date_str or ""
    return f"{date_str[6:8]}/{date_str[4:6]}/{date_str[:4]}"


# ── SESSION STATE FOR DRILL-DOWN NAVIGATION ──────────────────────────────────

if "analytics_view" not in st.session_state:
    st.session_state.analytics_view = "main"
if "analytics_drill_month" not in st.session_state:
    st.session_state.analytics_drill_month = None
if "analytics_drill_type" not in st.session_state:
    st.session_state.analytics_drill_type = None
if "analytics_drill_party" not in st.session_state:
    st.session_state.analytics_drill_party = None
if "analytics_drill_ledger" not in st.session_state:
    st.session_state.analytics_drill_ledger = None
if "analytics_drill_voucher" not in st.session_state:
    st.session_state.analytics_drill_voucher = None
if "analytics_drill_bank" not in st.session_state:
    st.session_state.analytics_drill_bank = None


def go_main():
    st.session_state.analytics_view = "main"
    st.session_state.analytics_drill_month = None
    st.session_state.analytics_drill_type = None
    st.session_state.analytics_drill_party = None
    st.session_state.analytics_drill_ledger = None
    st.session_state.analytics_drill_voucher = None
    st.session_state.analytics_drill_bank = None


def go_month_detail(month_code, drill_type):
    st.session_state.analytics_view = "month_detail"
    st.session_state.analytics_drill_month = month_code
    st.session_state.analytics_drill_type = drill_type
    st.session_state.analytics_drill_party = None
    st.session_state.analytics_drill_voucher = None


def go_party_detail(party_name, drill_type):
    st.session_state.analytics_view = "party_detail"
    st.session_state.analytics_drill_party = party_name
    st.session_state.analytics_drill_type = drill_type
    st.session_state.analytics_drill_voucher = None


def go_voucher_detail(voucher_guid):
    st.session_state.analytics_view = "voucher_detail"
    st.session_state.analytics_drill_voucher = voucher_guid


def go_expense_detail(ledger_name, month_code):
    st.session_state.analytics_view = "expense_detail"
    st.session_state.analytics_drill_ledger = ledger_name
    st.session_state.analytics_drill_month = month_code


def go_bank_detail(bank_name):
    st.session_state.analytics_view = "bank_detail"
    st.session_state.analytics_drill_bank = bank_name


# ── INIT ────────────────────────────────────────────────────────────────────

conn = get_conn()
company_row = conn.execute("SELECT value FROM _metadata WHERE key='company_name'").fetchone()
company = company_row[0] if company_row else "Company"


# ══════════════════════════════════════════════════════════════════════════════
# DRILL-DOWN VIEWS
# ══════════════════════════════════════════════════════════════════════════════

def render_back_button(label="Back to Dashboard", key="back_main"):
    st.button(f"← {label}", on_click=go_main, key=key, type="primary")


def render_voucher_detail_view():
    """Show full accounting entries for a single voucher."""
    guid = st.session_state.analytics_drill_voucher
    header = drill_voucher_header(conn, guid)
    if not header:
        st.error("Voucher not found.")
        render_back_button()
        return

    _guid, date, vch_no, vch_type, party, narration = header

    st.button("← Back", on_click=lambda: st.session_state.update({"analytics_view": st.session_state.get("_prev_view", "main")}),
              key="back_from_voucher", type="primary")

    st.markdown(f"## Voucher Detail — {vch_type} #{vch_no}")
    c1, c2, c3 = st.columns(3)
    c1.markdown(f"**Date:** {fmt_date(date)}")
    c2.markdown(f"**Type:** {vch_type}")
    c3.markdown(f"**Party:** {party or 'N/A'}")
    if narration:
        st.markdown(f"**Narration:** {narration}")

    st.markdown("")
    section_header("Accounting Entries")

    entries = drill_voucher_entries(conn, guid)
    if entries:
        debit_total = 0
        credit_total = 0
        entry_rows = []
        for ledger, amount, is_positive in entries:
            abs_amt = abs(amount) if amount else 0
            if is_positive == "Yes":
                entry_rows.append({"Ledger": ledger, "Debit (₹)": fmt(abs_amt), "Credit (₹)": ""})
                debit_total += abs_amt
            else:
                entry_rows.append({"Ledger": ledger, "Debit (₹)": "", "Credit (₹)": fmt(abs_amt)})
                credit_total += abs_amt

        st.dataframe(pd.DataFrame(entry_rows), use_container_width=True, hide_index=True)
        t1, t2 = st.columns(2)
        t1.metric("Total Debit", fmt(debit_total))
        t2.metric("Total Credit", fmt(credit_total))
    else:
        st.info("No accounting entries found for this voucher.")


def render_month_detail_view():
    """Show invoices/vouchers for a month — sales, purchases, receipts, or payments."""
    month_code = st.session_state.analytics_drill_month
    drill_type = st.session_state.analytics_drill_type

    render_back_button()

    type_config = {
        "sales": ("Sales Invoices", "Sales Accounts"),
        "purchases": ("Purchase Bills", "Purchase Accounts"),
        "receipts": ("Receipt Vouchers", "Receipt"),
        "payments": ("Payment Vouchers", "Payment"),
    }

    title, param = type_config.get(drill_type, ("Invoices", "Sales Accounts"))
    st.markdown(f"## {title} — {ml(month_code)}")

    if drill_type in ("sales", "purchases"):
        invoices = drill_monthly_invoices(conn, month_code, param)
        if invoices:
            total = sum(r[4] for r in invoices)
            st.metric(f"Total ({len(invoices)} invoices)", fmt(total))
            st.markdown("")

            for i, (guid, date, vch_no, party, amount) in enumerate(invoices):
                col_a, col_b, col_c, col_d = st.columns([2, 3, 2, 1])
                col_a.markdown(f"**{fmt_date(date)}**")
                col_b.markdown(f"{party or 'N/A'}")
                col_c.markdown(f"#{vch_no} — **{fmt(amount)}**")
                col_d.button("View", key=f"vch_{drill_type}_{i}",
                             on_click=go_voucher_detail, args=(guid,))
        else:
            st.info(f"No {drill_type} invoices found for {ml(month_code)}.")

    elif drill_type in ("receipts", "payments"):
        vouchers = drill_receipt_payment_vouchers(conn, month_code, param)
        if vouchers:
            total = sum(r[4] for r in vouchers)
            st.metric(f"Total ({len(vouchers)} vouchers)", fmt(total))
            st.markdown("")

            for i, (guid, date, vch_no, party, amount) in enumerate(vouchers):
                col_a, col_b, col_c, col_d = st.columns([2, 3, 2, 1])
                col_a.markdown(f"**{fmt_date(date)}**")
                col_b.markdown(f"{party or 'N/A'}")
                col_c.markdown(f"#{vch_no} — **{fmt(amount)}**")
                col_d.button("View", key=f"vch_{drill_type}_{i}",
                             on_click=go_voucher_detail, args=(guid,))
        else:
            st.info(f"No {drill_type} vouchers found for {ml(month_code)}.")


def render_party_detail_view():
    """Show all invoices for a specific party."""
    party = st.session_state.analytics_drill_party
    drill_type = st.session_state.analytics_drill_type

    render_back_button()

    ledger_parent = "Sales Accounts" if drill_type == "customer" else "Purchase Accounts"
    label = "Customer" if drill_type == "customer" else "Supplier"

    st.markdown(f"## {label} Detail — {party}")

    invoices = drill_party_invoices(conn, party, ledger_parent)
    if invoices:
        total = sum(r[4] for r in invoices)
        st.metric(f"Total ({len(invoices)} invoices)", fmt(total))
        st.markdown("")

        for i, (guid, date, vch_no, vch_type, amount) in enumerate(invoices):
            col_a, col_b, col_c, col_d = st.columns([2, 2, 3, 1])
            col_a.markdown(f"**{fmt_date(date)}**")
            col_b.markdown(f"{vch_type}")
            col_c.markdown(f"#{vch_no} — **{fmt(amount)}**")
            col_d.button("View", key=f"party_vch_{i}",
                         on_click=go_voucher_detail, args=(guid,))
    else:
        st.info(f"No invoices found for {party}.")


def render_expense_detail_view():
    """Show all transactions for an expense ledger in a month."""
    ledger = st.session_state.analytics_drill_ledger
    month_code = st.session_state.analytics_drill_month

    render_back_button()

    st.markdown(f"## Expense Detail — {ledger}")
    st.markdown(f"**Month:** {ml(month_code)}")

    txns = drill_expense_transactions(conn, ledger, month_code)
    if txns:
        total = sum(r[5] for r in txns)
        st.metric(f"Total ({len(txns)} entries)", fmt(total))
        st.markdown("")

        for i, (guid, date, vch_no, vch_type, party, amount) in enumerate(txns):
            col_a, col_b, col_c, col_d, col_e = st.columns([2, 2, 2, 2, 1])
            col_a.markdown(f"**{fmt_date(date)}**")
            col_b.markdown(f"{vch_type}")
            col_c.markdown(f"{party or 'N/A'}")
            col_d.markdown(f"#{vch_no} — **{fmt(amount)}**")
            col_e.button("View", key=f"exp_vch_{i}",
                         on_click=go_voucher_detail, args=(guid,))
    else:
        st.info(f"No transactions found for {ledger} in {ml(month_code)}.")


def render_bank_detail_view():
    """Show bank statement / all transactions for a bank account."""
    bank_name = st.session_state.analytics_drill_bank

    render_back_button()

    st.markdown(f"## Bank Statement — {bank_name}")

    txns = drill_bank_transactions(conn, bank_name)
    if txns:
        rows = []
        running = 0
        total_debit = 0
        total_credit = 0
        for date, vch_no, vch_type, party, guid, debit, credit in txns:
            running += debit - credit
            total_debit += debit
            total_credit += credit
            rows.append({
                "Date": fmt_date(date),
                "Vch #": vch_no,
                "Type": vch_type,
                "Party": (party or "")[:30],
                "Debit": fmt(debit) if debit > 0 else "",
                "Credit": fmt(credit) if credit > 0 else "",
                "Running": fmt(abs(running)),
                "_guid": guid,
            })

        m1, m2, m3 = st.columns(3)
        m1.metric("Total Debits", fmt(total_debit))
        m2.metric("Total Credits", fmt(total_credit))
        m3.metric(f"Transactions", f"{len(rows)}")

        st.markdown("")

        # Show as interactive list with view buttons
        for i, row in enumerate(rows):
            c1, c2, c3, c4, c5, c6, c7 = st.columns([1.5, 1, 1.2, 2.5, 1.5, 1.5, 0.8])
            c1.markdown(f"{row['Date']}")
            c2.markdown(f"{row['Vch #']}")
            c3.markdown(f"{row['Type']}")
            c4.markdown(f"{row['Party']}")
            c5.markdown(f"{row['Debit']}")
            c6.markdown(f"{row['Credit']}")
            c7.button("↗", key=f"bank_vch_{i}",
                      on_click=go_voucher_detail, args=(row["_guid"],))
    else:
        st.info(f"No transactions found for {bank_name}.")


# ══════════════════════════════════════════════════════════════════════════════
# VIEW ROUTER
# ══════════════════════════════════════════════════════════════════════════════

current_view = st.session_state.analytics_view

if current_view == "voucher_detail":
    page_header("Business Analytics", f"{company} | FY 2025-26")
    render_voucher_detail_view()
    conn.close()
    st.stop()

elif current_view == "month_detail":
    page_header("Business Analytics", f"{company} | FY 2025-26")
    render_month_detail_view()
    conn.close()
    st.stop()

elif current_view == "party_detail":
    page_header("Business Analytics", f"{company} | FY 2025-26")
    render_party_detail_view()
    conn.close()
    st.stop()

elif current_view == "expense_detail":
    page_header("Business Analytics", f"{company} | FY 2025-26")
    render_expense_detail_view()
    conn.close()
    st.stop()

elif current_view == "bank_detail":
    page_header("Business Analytics", f"{company} | FY 2025-26")
    render_bank_detail_view()
    conn.close()
    st.stop()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN DASHBOARD VIEW (unchanged layout, with clickable drill-down buttons)
# ══════════════════════════════════════════════════════════════════════════════

page_header("Business Analytics", f"{company} | FY 2025-26")
st.markdown("")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1: EXECUTIVE SUMMARY KPIs
# ══════════════════════════════════════════════════════════════════════════════

section_header("Executive Summary")

pnl = monthly_gross_profit(conn)
ratios = key_ratios(conn)
wc = working_capital_analysis(conn)

total_sales = sum(m["sales"] for m in pnl)
total_purchases = sum(m["purchases"] for m in pnl)
total_gp = sum(m["gross_profit"] for m in pnl)
total_np = sum(m["net_profit"] for m in pnl)
total_expenses = sum(m["indirect_expenses"] for m in pnl)

# MoM change for latest month
if len(pnl) >= 2:
    latest = pnl[-1]
    prev = pnl[-2]
    sales_change = ((latest["sales"] - prev["sales"]) / prev["sales"] * 100) if prev["sales"] else 0
    gp_change = ((latest["gross_profit"] - prev["gross_profit"]) / abs(prev["gross_profit"]) * 100) if prev["gross_profit"] else 0
else:
    sales_change = gp_change = 0

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Total Revenue", fmt(total_sales), f"{sales_change:+.1f}% MoM")
k2.metric("Total Purchases", fmt(total_purchases))
k3.metric("Gross Profit", fmt(total_gp), f"{ratios['gross_profit_margin']:.1f}% margin")
k4.metric("Net Result", fmt(total_np), "Profit" if total_np >= 0 else "Loss",
          delta_color="normal" if total_np >= 0 else "inverse")
k5.metric("Working Capital", fmt(wc["working_capital"]))
k6.metric("Current Ratio", f"{wc['current_ratio']:.2f}")

st.markdown("")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2: MONTHLY SALES & PURCHASE TREND (Drillable)
# ══════════════════════════════════════════════════════════════════════════════

section_header("Monthly Sales vs Purchases — Trend Analysis")
st.caption("Click any month to drill into individual invoices")

df_pnl = pd.DataFrame(pnl)
df_pnl["month_label"] = df_pnl["month"].map(ml)
df_pnl["sales_cr"] = df_pnl["sales"] / 10000000
df_pnl["purchases_cr"] = df_pnl["purchases"] / 10000000
df_pnl["gp_cr"] = df_pnl["gross_profit"] / 10000000
df_pnl["np_cr"] = df_pnl["net_profit"] / 10000000
df_pnl["expenses_l"] = df_pnl["indirect_expenses"] / 100000

# Sales vs Purchases chart — Plotly grouped bar
fig = go.Figure()
fig.add_trace(go.Bar(name='Sales', x=df_pnl['month_label'], y=df_pnl['sales_cr'], marker_color='#3b82f6'))
fig.add_trace(go.Bar(name='Purchases', x=df_pnl['month_label'], y=df_pnl['purchases_cr'], marker_color='#f59e0b'))
fig.update_layout(barmode='group', template='plotly_white', height=350, margin=dict(t=30, b=40, l=40, r=20),
                  font=dict(family='Inter', size=12), legend=dict(orientation='h', y=-0.15),
                  yaxis_title='Amount (Cr)', plot_bgcolor='white')
st.plotly_chart(fig, use_container_width=True)

# Detailed table with drill-down buttons
col1, col2 = st.columns(2)

with col1:
    st.markdown("### Month-on-Month Sales")
    for i, m in enumerate(pnl):
        mom_change = ((m["sales"] - pnl[i-1]["sales"]) / pnl[i-1]["sales"] * 100) if i > 0 and pnl[i-1]["sales"] else 0
        trend = "📈" if mom_change > 0 else ("📉" if mom_change < 0 else "➡️")
        c_month, c_amt, c_chg, c_btn = st.columns([2, 2, 1.5, 1])
        c_month.markdown(f"**{ml(m['month'])}**")
        c_amt.markdown(f"₹{m['sales']/100000:.2f} L")
        c_chg.markdown(f"{trend} {mom_change:+.1f}%")
        c_btn.button("Drill ↗", key=f"drill_sales_{i}",
                     on_click=go_month_detail, args=(m["month"], "sales"))

with col2:
    st.markdown("### Month-on-Month Purchases")
    for i, m in enumerate(pnl):
        mom_change = ((m["purchases"] - pnl[i-1]["purchases"]) / pnl[i-1]["purchases"] * 100) if i > 0 and pnl[i-1]["purchases"] else 0
        trend = "📈" if mom_change > 0 else ("📉" if mom_change < 0 else "➡️")
        c_month, c_amt, c_chg, c_btn = st.columns([2, 2, 1.5, 1])
        c_month.markdown(f"**{ml(m['month'])}**")
        c_amt.markdown(f"₹{m['purchases']/100000:.2f} L")
        c_chg.markdown(f"{trend} {mom_change:+.1f}%")
        c_btn.button("Drill ↗", key=f"drill_purch_{i}",
                     on_click=go_month_detail, args=(m["month"], "purchases"))

st.markdown("")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3: PROFITABILITY ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════

section_header("Profitability — Gross & Net Margin Trends")

fig2 = go.Figure()
fig2.add_trace(go.Scatter(x=df_pnl['month_label'], y=df_pnl['gp_margin'], mode='lines+markers', name='GP Margin %', line=dict(color='#059669', width=2), marker=dict(size=6)))
fig2.add_trace(go.Scatter(x=df_pnl['month_label'], y=df_pnl['np_margin'], mode='lines+markers', name='NP Margin %', line=dict(color='#dc2626', width=2), marker=dict(size=6)))
fig2.update_layout(template='plotly_white', height=300, margin=dict(t=30, b=40, l=40, r=20),
                   font=dict(family='Inter', size=12), legend=dict(orientation='h', y=-0.15),
                   yaxis_title='Margin %', plot_bgcolor='white')
st.plotly_chart(fig2, use_container_width=True)

# Profit table
profit_rows = []
for m in pnl:
    profit_rows.append({
        "Month": ml(m["month"]),
        "Sales": fmt(m["sales"]),
        "Purchases": fmt(m["purchases"]),
        "Gross Profit": fmt(m["gross_profit"]),
        "GP %": f"{m['gp_margin']:.1f}%",
        "Expenses": fmt(m["indirect_expenses"]),
        "Net Profit": fmt(m["net_profit"]),
        "NP %": f"{m['np_margin']:.1f}%",
        "Signal": "✅" if m["net_profit"] > 0 else "⚠️",
    })
st.dataframe(pd.DataFrame(profit_rows), use_container_width=True, hide_index=True)

st.markdown("")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4: COLLECTION & PAYMENT EFFICIENCY (Drillable)
# ══════════════════════════════════════════════════════════════════════════════

section_header("Cash Collection & Payment Analysis")
st.caption("Click Receipts/Payments to drill into individual vouchers")

receipts_data, payments_data = monthly_receipts_payments(conn)
eff_data = collection_efficiency(conn)

col1, col2 = st.columns(2)

with col1:
    st.markdown("### Monthly Receipts vs Payments")
    r_dict = {r[0]: r[2] for r in receipts_data}
    p_dict = {r[0]: r[2] for r in payments_data}
    all_months = sorted(set(list(r_dict.keys()) + list(p_dict.keys())))

    cash_flow_chart = []
    cf_months_list = []
    cf_receipts_list = []
    cf_payments_list = []
    for m in all_months:
        r = r_dict.get(m, 0)
        p = p_dict.get(m, 0)
        cf_months_list.append(ml(m))
        cf_receipts_list.append(r / 100000)
        cf_payments_list.append(p / 100000)

    fig_cf = go.Figure()
    fig_cf.add_trace(go.Bar(name='Receipts', x=cf_months_list, y=cf_receipts_list, marker_color='#059669'))
    fig_cf.add_trace(go.Bar(name='Payments', x=cf_months_list, y=cf_payments_list, marker_color='#ef4444'))
    fig_cf.update_layout(barmode='group', template='plotly_white', height=300, margin=dict(t=30, b=40, l=40, r=20),
                         font=dict(family='Inter', size=12), legend=dict(orientation='h', y=-0.15),
                         yaxis_title='Amount (L)', plot_bgcolor='white')
    st.plotly_chart(fig_cf, use_container_width=True)

    for idx, m in enumerate(all_months):
        r = r_dict.get(m, 0)
        p = p_dict.get(m, 0)
        net = r - p
        flow_icon = "🟢" if net > 0 else "🔴"
        c1, c2, c3, c4, c5 = st.columns([1.5, 1.5, 1.5, 1.5, 1.5])
        c1.markdown(f"**{ml(m)}**")
        c2.button(f"R: {fmt(r)}", key=f"drill_rcpt_{idx}",
                  on_click=go_month_detail, args=(m, "receipts"))
        c3.button(f"P: {fmt(p)}", key=f"drill_pymt_{idx}",
                  on_click=go_month_detail, args=(m, "payments"))
        c4.markdown(f"{flow_icon} {fmt(net)}")

with col2:
    st.markdown("### Collection Efficiency (Receipts / Sales)")
    eff_months = [ml(e["month"]) for e in eff_data]
    eff_values = [e["efficiency"] for e in eff_data]

    fig_eff = go.Figure()
    fig_eff.add_trace(go.Scatter(x=eff_months, y=eff_values, mode='lines+markers', name='Collection %',
                                 line=dict(color='#3b82f6', width=2), marker=dict(size=6)))
    fig_eff.add_hline(y=60, line_dash="dash", line_color="#059669", annotation_text="Target 60%",
                      annotation_position="top right")
    fig_eff.update_layout(template='plotly_white', height=300, margin=dict(t=30, b=40, l=40, r=20),
                          font=dict(family='Inter', size=12), legend=dict(orientation='h', y=-0.15),
                          yaxis_title='Efficiency %', plot_bgcolor='white')
    st.plotly_chart(fig_eff, use_container_width=True)

    eff_rows = []
    for e in eff_data:
        eff_rows.append({
            "Month": ml(e["month"]),
            "Sales": fmt(e["sales"]),
            "Collections": fmt(e["collections"]),
            "Efficiency": f"{e['efficiency']:.0f}%",
            "Rating": "✅" if e["efficiency"] >= 60 else ("⚠️" if e["efficiency"] >= 40 else "🔴"),
        })
    st.dataframe(pd.DataFrame(eff_rows), use_container_width=True, hide_index=True)

st.markdown("")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5: BANK & CASH POSITION (Drillable)
# ══════════════════════════════════════════════════════════════════════════════

section_header("Bank & Cash Position")
st.caption("Click any bank account to view its full statement")

balances = bank_balances(conn)
total_bank = 0

st.metric("Total Bank + Cash Balance", fmt(sum(abs(b[3] or 0) for b in balances)))

# Column headers
hdr_c1, hdr_c2, hdr_c3, hdr_c4, hdr_c5, hdr_c6, hdr_c7 = st.columns([3, 1.5, 1.5, 1.5, 1.5, 1.5, 1])
hdr_c1.markdown("**Account**")
hdr_c2.markdown("**Type**")
hdr_c3.markdown("**Opening**")
hdr_c4.markdown("**Closing**")
hdr_c5.markdown("**Net Movement**")
hdr_c6.markdown("**Direction**")
hdr_c7.markdown("**Action**")

for idx, (name, parent, opening, closing) in enumerate(balances):
    movement = (closing or 0) - (opening or 0)
    direction = "📈 Inflow" if movement < 0 else "📉 Outflow"
    bc1, bc2, bc3, bc4, bc5, bc6, bc7 = st.columns([3, 1.5, 1.5, 1.5, 1.5, 1.5, 1])
    bc1.markdown(f"**{name}**")
    bc2.markdown(f"{parent}")
    bc3.markdown(f"{fmt(abs(opening or 0))}")
    bc4.markdown(f"{fmt(abs(closing or 0))}")
    bc5.markdown(f"{fmt(abs(movement))}")
    bc6.markdown(f"{direction}")
    bc7.button("View ↗", key=f"drill_bank_{idx}",
               on_click=go_bank_detail, args=(name,))

st.markdown("")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6: TOP CUSTOMERS & SUPPLIERS (Drillable)
# ══════════════════════════════════════════════════════════════════════════════

section_header("Customer & Supplier Analysis")
st.caption("Click a customer or supplier name to see all their invoices")

col1, col2 = st.columns(2)

with col1:
    st.markdown("### Top 15 Customers by Sales")
    customers = top_customers_by_sales(conn, 15)
    if customers:
        total_top = sum(c[2] for c in customers)
        cumulative = 0
        for i, (name, count, total) in enumerate(customers, 0):
            cumulative += total
            cc1, cc2, cc3, cc4 = st.columns([3.5, 1.5, 2, 1])
            cc1.button(f"{name[:35]}", key=f"drill_cust_{i}",
                       on_click=go_party_detail, args=(name, "customer"))
            cc2.markdown(f"{count} inv")
            cc3.markdown(f"**{fmt(total)}** ({total/total_sales*100:.1f}%)")
        st.caption(f"Top 15 contribute {cumulative/total_sales*100:.1f}% of total sales — {'High' if cumulative/total_sales > 0.6 else 'Low'} concentration")

with col2:
    st.markdown("### Top 15 Suppliers by Purchases")
    suppliers = top_suppliers_by_purchase(conn, 15)
    if suppliers:
        total_top_sup = sum(s[2] for s in suppliers)
        cumulative = 0
        for i, (name, count, total) in enumerate(suppliers, 0):
            cumulative += total
            sc1, sc2, sc3, sc4 = st.columns([3.5, 1.5, 2, 1])
            sc1.button(f"{name[:35]}", key=f"drill_supp_{i}",
                       on_click=go_party_detail, args=(name, "supplier"))
            sc2.markdown(f"{count} bills")
            sc3.markdown(f"**{fmt(total)}** ({total/total_purchases*100:.1f}%)")
        st.caption(f"Top 15 contribute {cumulative/total_purchases*100:.1f}% of total purchases")

st.markdown("")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 7: EXPENSE BREAKDOWN (Drillable)
# ══════════════════════════════════════════════════════════════════════════════

section_header("Indirect Expense Breakdown — Monthly")
st.caption("Click an expense head for a specific month to see all underlying transactions")

exp_data = monthly_expenses(conn)
exp_pivot = {}
exp_totals = {}
for month, ledger, amt in exp_data:
    if ledger not in exp_pivot:
        exp_pivot[ledger] = {}
    exp_pivot[ledger][month] = amt
    exp_totals[ledger] = exp_totals.get(ledger, 0) + amt

# Sort by total
sorted_expenses = sorted(exp_totals.items(), key=lambda x: x[1], reverse=True)
all_months_exp = sorted(set(r[0] for r in exp_data))

# Show month selector for expense drill-down
expense_month_sel = st.selectbox(
    "Select month to drill into expense details:",
    options=all_months_exp,
    format_func=ml,
    key="expense_month_selector"
)

exp_table = []
for ledger, total in sorted_expenses[:20]:
    row = {"Expense Head": ledger, "Total": fmt(total)}
    for m in all_months_exp:
        row[ml(m)] = fmt(exp_pivot.get(ledger, {}).get(m, 0))
    exp_table.append(row)

if exp_table:
    st.dataframe(pd.DataFrame(exp_table), use_container_width=True, hide_index=True, height=400)

    st.markdown(f"#### Drill into expenses for **{ml(expense_month_sel)}**:")
    # Show clickable buttons for each expense head
    exp_cols_per_row = 4
    for row_start in range(0, min(len(sorted_expenses), 20), exp_cols_per_row):
        cols = st.columns(exp_cols_per_row)
        for j in range(exp_cols_per_row):
            idx = row_start + j
            if idx < min(len(sorted_expenses), 20):
                ledger, total = sorted_expenses[idx]
                month_amt = exp_pivot.get(ledger, {}).get(expense_month_sel, 0)
                if month_amt > 0:
                    cols[j].button(
                        f"{ledger[:25]}\n{fmt(month_amt)}",
                        key=f"drill_exp_{idx}",
                        on_click=go_expense_detail,
                        args=(ledger, expense_month_sel)
                    )

st.markdown("")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 8: WORKING CAPITAL & KEY RATIOS
# ══════════════════════════════════════════════════════════════════════════════

section_header("Working Capital & Key Financial Ratios")

col1, col2 = st.columns(2)

with col1:
    st.markdown("### Working Capital Composition")
    st.markdown("**Current Assets:**")
    for g, v in wc["current_assets"].items():
        st.markdown(f"- {g}: **{fmt(v)}**")
    st.metric("Total Current Assets", fmt(wc["total_ca"]))

    st.markdown("**Current Liabilities:**")
    for g, v in wc["current_liabilities"].items():
        st.markdown(f"- {g}: **{fmt(v)}**")
    st.metric("Total Current Liabilities", fmt(wc["total_cl"]))

with col2:
    st.markdown("### Key Ratios")
    ratio_rows = [
        {"Ratio": "Gross Profit Margin", "Value": f"{ratios['gross_profit_margin']:.1f}%",
         "Signal": "✅" if ratios["gross_profit_margin"] > 5 else "⚠️"},
        {"Ratio": "Net Profit Margin", "Value": f"{ratios['net_profit_margin']:.1f}%",
         "Signal": "✅" if ratios["net_profit_margin"] > 0 else "🔴"},
        {"Ratio": "Current Ratio", "Value": f"{ratios['current_ratio']:.2f}",
         "Signal": "✅" if ratios["current_ratio"] > 1.5 else ("⚠️" if ratios["current_ratio"] > 1 else "🔴")},
        {"Ratio": "Debtor Days", "Value": f"{ratios['debtor_days']:.0f} days",
         "Signal": "✅" if ratios["debtor_days"] < 45 else ("⚠️" if ratios["debtor_days"] < 90 else "🔴")},
        {"Ratio": "Creditor Days", "Value": f"{ratios['creditor_days']:.0f} days",
         "Signal": "✅" if ratios["creditor_days"] < 60 else ("⚠️" if ratios["creditor_days"] < 120 else "🔴")},
        {"Ratio": "Return on Assets", "Value": f"{ratios['roa']:.1f}%",
         "Signal": "✅" if ratios["roa"] > 5 else ("⚠️" if ratios["roa"] > 0 else "🔴")},
        {"Ratio": "Total Debtors (Receivable)", "Value": fmt(ratios["total_debtors"]), "Signal": ""},
        {"Ratio": "Total Creditors (Payable)", "Value": fmt(ratios["total_creditors"]), "Signal": ""},
    ]
    st.dataframe(pd.DataFrame(ratio_rows), use_container_width=True, hide_index=True)

    # Health score
    score = 0
    if ratios["gross_profit_margin"] > 5: score += 20
    if ratios["net_profit_margin"] > 0: score += 20
    if ratios["current_ratio"] > 1: score += 20
    if ratios["debtor_days"] < 90: score += 20
    if ratios["creditor_days"] < 120: score += 20

    st.markdown(f"### Overall Financial Health Score: **{score}/100**")
    if score >= 80:
        st.success("Strong financial health")
    elif score >= 60:
        st.warning("Moderate — some areas need attention")
    else:
        st.error("Needs immediate attention")

st.markdown("")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 9: CASH FLOW STATEMENT + PROJECTIONS (Drillable)
# ══════════════════════════════════════════════════════════════════════════════

section_header("Cash Flow Statement & Future Projections")
st.caption("Click Receipts or Payments in any row to drill down")

cf = cash_flow_statement(conn)
projections = project_cash_flow(conn, months_ahead=3)

# Historical cash flow
st.markdown("### Historical Cash Flow (Month-on-Month)")

cumulative_cf = 0
for idx, m in enumerate(cf):
    cumulative_cf += m["net_cash_flow"]
    c1, c2, c3, c4, c5, c6 = st.columns([1.5, 1.5, 1.5, 1.5, 1.5, 1.5])
    c1.markdown(f"**{ml(m['month'])}**")
    c2.markdown(f"NP: {fmt(m['net_profit'])}")
    c3.button(f"R: {fmt(m['receipts'])}", key=f"cf_rcpt_{idx}",
              on_click=go_month_detail, args=(m["month"], "receipts"))
    c4.button(f"P: {fmt(m['payments'])}", key=f"cf_pymt_{idx}",
              on_click=go_month_detail, args=(m["month"], "payments"))
    flow_icon = "🟢" if m["net_cash_flow"] > 0 else "🔴"
    c5.markdown(f"{flow_icon} Net: {fmt(m['net_cash_flow'])}")
    c6.markdown(f"Cum: {fmt(cumulative_cf)}")

# Cash flow chart — Plotly grouped bar
cf_chart_months = [ml(m["month"]) for m in cf]
cf_chart_receipts = [m["receipts"] / 100000 for m in cf]
cf_chart_payments = [m["payments"] / 100000 for m in cf]

fig_cf2 = go.Figure()
fig_cf2.add_trace(go.Bar(name='Receipts', x=cf_chart_months, y=cf_chart_receipts, marker_color='#059669'))
fig_cf2.add_trace(go.Bar(name='Payments', x=cf_chart_months, y=cf_chart_payments, marker_color='#ef4444'))
fig_cf2.update_layout(barmode='group', template='plotly_white', height=300, margin=dict(t=30, b=40, l=40, r=20),
                      font=dict(family='Inter', size=12), legend=dict(orientation='h', y=-0.15),
                      yaxis_title='Amount (L)', plot_bgcolor='white')
st.plotly_chart(fig_cf2, use_container_width=True)

# Net cash flow trend — Plotly line
net_cf_months = [ml(m["month"]) for m in cf]
net_cf_values = [m["net_cash_flow"] / 100000 for m in cf]

fig_net_cf = go.Figure()
fig_net_cf.add_trace(go.Scatter(x=net_cf_months, y=net_cf_values, mode='lines+markers', name='Net Cash Flow',
                                line=dict(color='#3b82f6', width=2), marker=dict(size=6)))
fig_net_cf.add_hline(y=0, line_dash="dash", line_color="#94a3b8")
fig_net_cf.update_layout(template='plotly_white', height=250, margin=dict(t=30, b=40, l=40, r=20),
                         font=dict(family='Inter', size=12), legend=dict(orientation='h', y=-0.15),
                         yaxis_title='Net Cash Flow (L)', plot_bgcolor='white')
st.plotly_chart(fig_net_cf, use_container_width=True)


st.markdown("### Projected Cash Flow — Next 3 Months")
st.caption("Based on weighted average of last 3 months with trend adjustment")

if projections:
    proj_rows = []
    for p in projections:
        proj_rows.append({
            "Month": ml(p["month"]),
            "Projected Receipts": fmt(p["projected_receipts"]),
            "Projected Payments": fmt(p["projected_payments"]),
            "Projected Net CF": fmt(p["projected_net_cf"]),
            "Projected Cash Balance": fmt(p["projected_cash_balance"]),
            "Confidence": f"{p['confidence']*100:.0f}%",
        })
    st.dataframe(pd.DataFrame(proj_rows), use_container_width=True, hide_index=True)

    # Combined historical + projected chart — Plotly line
    actual_months = [ml(m["month"]) for m in cf]
    actual_values = [m["net_cash_flow"] / 100000 for m in cf]
    proj_months = [ml(p["month"]) for p in projections]
    proj_values = [p["projected_net_cf"] / 100000 for p in projections]

    fig_combined = go.Figure()
    fig_combined.add_trace(go.Scatter(x=actual_months, y=actual_values, mode='lines+markers',
                                      name='Actual Net CF', line=dict(color='#3b82f6', width=2), marker=dict(size=6)))
    fig_combined.add_trace(go.Scatter(x=proj_months, y=proj_values, mode='lines+markers',
                                      name='Projected Net CF', line=dict(color='#f59e0b', width=2, dash='dash'), marker=dict(size=6)))
    fig_combined.update_layout(template='plotly_white', height=300, margin=dict(t=30, b=40, l=40, r=20),
                               font=dict(family='Inter', size=12), legend=dict(orientation='h', y=-0.15),
                               yaxis_title='Net Cash Flow (L)', plot_bgcolor='white')
    st.plotly_chart(fig_combined, use_container_width=True)

    # Cash balance projection
    st.markdown("### Projected Cash Balance Trajectory")
    current_bal = sum(abs(b[3]) for b in bank_balances(conn))
    bal_months = [ml(p["month"]) for p in projections]
    bal_values = [p["projected_cash_balance"] / 100000 for p in projections]

    if bal_months:
        fig_bal = go.Figure()
        fig_bal.add_trace(go.Scatter(x=bal_months, y=bal_values, mode='lines+markers', name='Projected Balance',
                                     line=dict(color='#7c3aed', width=2), marker=dict(size=6),
                                     fill='tozeroy', fillcolor='rgba(124, 58, 237, 0.1)'))
        fig_bal.update_layout(template='plotly_white', height=250, margin=dict(t=30, b=40, l=40, r=20),
                              font=dict(family='Inter', size=12), legend=dict(orientation='h', y=-0.15),
                              yaxis_title='Balance (L)', plot_bgcolor='white')
        st.plotly_chart(fig_bal, use_container_width=True)

    # Risk assessment
    st.markdown("### Cash Flow Risk Assessment")
    avg_monthly_burn = sum(m["payments"] for m in cf) / len(cf)
    months_of_runway = current_bal / avg_monthly_burn if avg_monthly_burn > 0 else 999

    if months_of_runway > 6:
        st.success(f"Runway: ~{months_of_runway:.1f} months at current burn rate — Comfortable position")
    elif months_of_runway > 3:
        st.warning(f"Runway: ~{months_of_runway:.1f} months — Monitor closely")
    else:
        st.error(f"Runway: ~{months_of_runway:.1f} months — Cash crunch risk! Accelerate collections.")

st.markdown("")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 10: INSIGHTS & OBSERVATIONS
# ══════════════════════════════════════════════════════════════════════════════

section_header("Key Insights & Observations")

insights = []

# Sales trend
first_3_avg = sum(m["sales"] for m in pnl[:3]) / 3
last_3_avg = sum(m["sales"] for m in pnl[-3:]) / 3
sales_trend_pct = ((last_3_avg - first_3_avg) / first_3_avg * 100)
if sales_trend_pct < -10:
    insights.append(f"⚠️ **Sales declining**: Average monthly sales dropped {abs(sales_trend_pct):.0f}% from first 3 months ({fmt(first_3_avg)}/month) to last 3 months ({fmt(last_3_avg)}/month)")
elif sales_trend_pct > 10:
    insights.append(f"✅ **Sales growing**: Average monthly sales grew {sales_trend_pct:.0f}% from first 3 months to last 3 months")

# Margin trend
first_3_gp = sum(m["gp_margin"] for m in pnl[:3]) / 3
last_3_gp = sum(m["gp_margin"] for m in pnl[-3:]) / 3
if last_3_gp < first_3_gp - 2:
    insights.append(f"⚠️ **Margin compression**: GP margin declined from {first_3_gp:.1f}% to {last_3_gp:.1f}%")
elif last_3_gp > first_3_gp + 2:
    insights.append(f"✅ **Margin improvement**: GP margin improved from {first_3_gp:.1f}% to {last_3_gp:.1f}%")

# Collection efficiency
avg_eff = sum(e["efficiency"] for e in eff_data) / len(eff_data) if eff_data else 0
if avg_eff < 50:
    insights.append(f"🔴 **Poor collection efficiency**: Only {avg_eff:.0f}% of sales collected on average — significant credit buildup")
elif avg_eff < 70:
    insights.append(f"⚠️ **Moderate collection**: {avg_eff:.0f}% of sales collected — room for improvement")

# Creditor concentration
if suppliers:
    top1_pct = suppliers[0][2] / total_purchases * 100
    if top1_pct > 30:
        insights.append(f"⚠️ **Supplier concentration risk**: {suppliers[0][0]} accounts for {top1_pct:.0f}% of all purchases")

# Working capital
if wc["current_ratio"] < 1:
    insights.append(f"🔴 **Working capital deficit**: Current ratio {wc['current_ratio']:.2f} — liabilities exceed current assets")

# Debtor days
if ratios["debtor_days"] > 60:
    insights.append(f"⚠️ **High debtor days**: {ratios['debtor_days']:.0f} days — push for faster collections")

# Net loss months
loss_months = [m for m in pnl if m["net_profit"] < 0]
if loss_months:
    insights.append(f"📉 **Loss-making months**: {len(loss_months)} out of {len(pnl)} months show net loss")

# Cash flow pattern
negative_cf_months = [m for m in cf if m["net_cash_flow"] < 0]
if len(negative_cf_months) > len(cf) / 2:
    insights.append(f"🔴 **Negative cash flow**: {len(negative_cf_months)} out of {len(cf)} months had net cash outflow")

# January spike
if len(pnl) >= 2 and pnl[-1]["purchases"] > pnl[-2]["purchases"] * 1.4:
    insights.append(f"📦 **Purchase spike in {ml(pnl[-1]['month'])}**: Purchases jumped {((pnl[-1]['purchases']-pnl[-2]['purchases'])/pnl[-2]['purchases']*100):.0f}% — bulk stocking or supplier pressure?")

for insight in insights:
    st.markdown(insight)

if not insights:
    st.info("No critical observations at this time.")


st.markdown("")
st.markdown('<div class="slv-footer">Seven Labs Vision — Powered by Tally Data | Comprehensive Business Analytics</div>', unsafe_allow_html=True)

# ── PERSISTENT CHAT BAR ─────────────────────────────────────────────────────
st.markdown("")
from chat_engine import ask, format_result_as_text

chat_input = st.chat_input("Ask anything — P&L, Balance Sheet, ledger of [party], debtors, creditors...")
if chat_input:
    result = ask(chat_input)
    st.markdown(f"**You:** {chat_input}")
    if result.get("type") == "chat":
        st.markdown(result.get("message", ""))
    else:
        st.markdown(format_result_as_text(result))

conn.close()
