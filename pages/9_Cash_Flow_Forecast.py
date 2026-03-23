"""
Seven Labs Vision -- Cash Flow Forecast Dashboard
Comprehensive forecasting with scenario analysis, alerts, and Excel export.
"""

import streamlit as st
import sys
import os
import json
import tempfile
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from styles import (
    inject_base_styles, page_header, section_header, metric_card,
    badge, footer, info_banner, fmt, fmt_inr,
)
from cashflow_forecaster import (
    analyze_historical, forecast_cashflow, export_forecast_excel,
    DEFAULT_ASSUMPTIONS, DB_PATH,
)

st.set_page_config(
    page_title="Cash Flow Forecast | Seven Labs Vision",
    page_icon="SLV",
    layout="wide",
)
inject_base_styles()

# ---------------------------------------------------------------------------
# SESSION STATE INIT
# ---------------------------------------------------------------------------

if "cf_assumptions" not in st.session_state:
    st.session_state.cf_assumptions = dict(DEFAULT_ASSUMPTIONS)

if "cf_scenarios_selected" not in st.session_state:
    st.session_state.cf_scenarios_selected = ["base"]

if "cf_horizon" not in st.session_state:
    st.session_state.cf_horizon = 6

if "cf_capex_rows" not in st.session_state:
    st.session_state.cf_capex_rows = []

if "cf_receipt_rows" not in st.session_state:
    st.session_state.cf_receipt_rows = []

if "cf_hire_rows" not in st.session_state:
    st.session_state.cf_hire_rows = []


# ---------------------------------------------------------------------------
# DATA LOADING
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_historical():
    return analyze_historical(DB_PATH)


historical = load_historical()
monthly_data = historical.get("monthly_data", [])
patterns = historical.get("patterns", {})
current_pos = historical.get("current_position", {})

# ---------------------------------------------------------------------------
# PAGE HEADER
# ---------------------------------------------------------------------------

page_header(
    "Cash Flow Forecast",
    "Projected cash flows with scenario analysis, runway estimation, and compliance alerts"
)

if not monthly_data:
    info_banner("No historical cash flow data found. Ensure Tally data is loaded.", "warning")
    st.stop()

# ---------------------------------------------------------------------------
# TOP SECTION: CURRENT POSITION
# ---------------------------------------------------------------------------

section_header("CURRENT POSITION")

c1, c2, c3, c4 = st.columns(4)
with c1:
    metric_card(
        "Bank Balance",
        fmt_inr(current_pos.get("bank_balance", 0)),
        f"{len([1 for _ in []])} accounts",  # placeholder
        "blue",
    )
with c2:
    metric_card(
        "Cash Balance",
        fmt_inr(current_pos.get("cash_balance", 0)),
        "Cash-in-Hand",
        "green",
    )
with c3:
    metric_card(
        "Receivables",
        fmt_inr(current_pos.get("receivables", 0)),
        f"DSO: {patterns.get('dso', 0):.0f} days",
        "amber",
    )
with c4:
    metric_card(
        "Payables",
        fmt_inr(current_pos.get("payables", 0)),
        f"DPO: {patterns.get('dpo', 0):.0f} days",
        "red",
    )

# ---------------------------------------------------------------------------
# SECTION 1: HISTORICAL CASH FLOW
# ---------------------------------------------------------------------------

section_header("HISTORICAL CASH FLOW")

import plotly.graph_objects as go

# Chart: Monthly receipts vs payments with net cash line
fig_hist = go.Figure()

months_labels = [d["label"] for d in monthly_data]
receipts_vals = [d["receipts"] for d in monthly_data]
payments_vals = [d["payments"] for d in monthly_data]
net_vals = [d["net_cash"] for d in monthly_data]

fig_hist.add_trace(go.Bar(
    x=months_labels, y=receipts_vals,
    name="Receipts",
    marker_color="#059669",
    opacity=0.85,
))
fig_hist.add_trace(go.Bar(
    x=months_labels, y=payments_vals,
    name="Payments",
    marker_color="#dc2626",
    opacity=0.85,
))
fig_hist.add_trace(go.Scatter(
    x=months_labels, y=net_vals,
    name="Net Cash Flow",
    mode="lines+markers",
    line=dict(color="#2563eb", width=2.5),
    marker=dict(size=6),
))

fig_hist.update_layout(
    barmode="group",
    height=400,
    margin=dict(l=20, r=20, t=30, b=20),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    plot_bgcolor="white",
    yaxis=dict(gridcolor="#f1f5f9", tickformat=",.0f"),
    xaxis=dict(gridcolor="#f1f5f9"),
    font=dict(family="Inter, sans-serif", size=12),
)

st.plotly_chart(fig_hist, use_container_width=True)

# Historical table in expander
with st.expander("View Historical Data Table"):
    table_html = '<table class="slv-table"><thead><tr>'
    table_html += '<th>Month</th><th>Receipts</th><th>Payments</th><th>Net Cash</th>'
    table_html += '<th>Sales</th><th>Purchases</th><th>Salary</th><th>GST</th><th>TDS</th>'
    table_html += '</tr></thead><tbody>'
    for d in monthly_data:
        net_class = "amt-pos" if d["net_cash"] >= 0 else "amt-neg"
        table_html += f'<tr>'
        table_html += f'<td>{d["label"]}</td>'
        table_html += f'<td class="amt">{fmt(d["receipts"])}</td>'
        table_html += f'<td class="amt">{fmt(d["payments"])}</td>'
        table_html += f'<td class="amt {net_class}">{fmt(d["net_cash"])}</td>'
        table_html += f'<td class="amt">{fmt(d.get("sales_receipts", 0))}</td>'
        table_html += f'<td class="amt">{fmt(d.get("purchase_payments", 0))}</td>'
        table_html += f'<td class="amt">{fmt(d.get("salary_payments", 0))}</td>'
        table_html += f'<td class="amt">{fmt(d.get("gst_payments", 0))}</td>'
        table_html += f'<td class="amt">{fmt(d.get("tds_payments", 0))}</td>'
        table_html += '</tr>'

    # Totals row
    table_html += '<tr class="total-row">'
    table_html += '<td>TOTAL</td>'
    table_html += f'<td class="amt">{fmt(sum(d["receipts"] for d in monthly_data))}</td>'
    table_html += f'<td class="amt">{fmt(sum(d["payments"] for d in monthly_data))}</td>'
    table_html += f'<td class="amt">{fmt(sum(d["net_cash"] for d in monthly_data))}</td>'
    table_html += f'<td class="amt">{fmt(sum(d.get("sales_receipts", 0) for d in monthly_data))}</td>'
    table_html += f'<td class="amt">{fmt(sum(d.get("purchase_payments", 0) for d in monthly_data))}</td>'
    table_html += f'<td class="amt">{fmt(sum(d.get("salary_payments", 0) for d in monthly_data))}</td>'
    table_html += f'<td class="amt">{fmt(sum(d.get("gst_payments", 0) for d in monthly_data))}</td>'
    table_html += f'<td class="amt">{fmt(sum(d.get("tds_payments", 0) for d in monthly_data))}</td>'
    table_html += '</tr>'

    table_html += '</tbody></table>'
    st.markdown(table_html, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# SECTION 2: FORECAST SETTINGS (sidebar)
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("### Forecast Settings")
    st.markdown("---")

    scenario_options = ["base", "optimistic", "pessimistic"]
    selected_scenarios = st.multiselect(
        "Scenarios",
        scenario_options,
        default=st.session_state.cf_scenarios_selected,
        help="Select one or more scenarios to compare",
    )
    if selected_scenarios:
        st.session_state.cf_scenarios_selected = selected_scenarios

    forecast_horizon = st.slider(
        "Forecast Horizon (months)",
        min_value=3, max_value=12,
        value=st.session_state.cf_horizon,
    )
    st.session_state.cf_horizon = forecast_horizon

    st.markdown("---")
    st.markdown("#### Growth Assumptions")

    rev_growth = st.number_input(
        "Revenue Growth % (monthly)",
        min_value=-20.0, max_value=50.0,
        value=float(st.session_state.cf_assumptions.get("revenue_growth_pct", 0)),
        step=0.5,
    )
    st.session_state.cf_assumptions["revenue_growth_pct"] = rev_growth

    exp_growth = st.number_input(
        "Expense Growth % (monthly)",
        min_value=-20.0, max_value=50.0,
        value=float(st.session_state.cf_assumptions.get("expense_growth_pct", 0)),
        step=0.5,
    )
    st.session_state.cf_assumptions["expense_growth_pct"] = exp_growth

    st.markdown("---")
    st.markdown("#### Thresholds & Tax")

    min_cash = st.number_input(
        "Min Cash Threshold (Rs)",
        min_value=0,
        value=int(st.session_state.cf_assumptions.get("minimum_cash_threshold", 500000)),
        step=100000,
    )
    st.session_state.cf_assumptions["minimum_cash_threshold"] = min_cash

    adv_tax_rate = st.number_input(
        "Advance Tax Rate %",
        min_value=0.0, max_value=40.0,
        value=float(st.session_state.cf_assumptions.get("advance_tax_rate_pct", 30)),
        step=1.0,
    )
    st.session_state.cf_assumptions["advance_tax_rate_pct"] = adv_tax_rate

    st.markdown("---")
    st.markdown("#### Salary & Rent")

    sal_incr = st.number_input(
        "Salary Increment % (annual)",
        min_value=0.0, max_value=50.0,
        value=float(st.session_state.cf_assumptions.get("salary_increment_pct", 0)),
        step=1.0,
    )
    st.session_state.cf_assumptions["salary_increment_pct"] = sal_incr

    rent_incr = st.number_input(
        "Rent Increase % (annual)",
        min_value=0.0, max_value=30.0,
        value=float(st.session_state.cf_assumptions.get("rent_increase_pct", 0)),
        step=1.0,
    )
    st.session_state.cf_assumptions["rent_increase_pct"] = rent_incr

    emi_val = st.number_input(
        "Loan EMI (monthly, Rs)",
        min_value=0,
        value=int(st.session_state.cf_assumptions.get("loan_repayment_emi", 0)),
        step=5000,
    )
    st.session_state.cf_assumptions["loan_repayment_emi"] = emi_val

# ---------------------------------------------------------------------------
# PLANNED CAPEX / EXPECTED RECEIPTS / NEW HIRES (expanders in main area)
# ---------------------------------------------------------------------------

with st.expander("Planned Capital Expenditure"):
    st.caption("Add one-time large payments (machinery, equipment, etc.)")
    col_a, col_b, col_c, col_d = st.columns([2, 2, 3, 1])
    with col_a:
        capex_month = st.text_input("Month (YYYYMM)", key="capex_month_input", placeholder="202604")
    with col_b:
        capex_amt = st.number_input("Amount (Rs)", min_value=0, step=50000, key="capex_amt_input")
    with col_c:
        capex_desc = st.text_input("Description", key="capex_desc_input", placeholder="New machinery")
    with col_d:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("Add", key="add_capex"):
            if capex_month and capex_amt > 0:
                st.session_state.cf_capex_rows.append({
                    "month": capex_month, "amount": capex_amt, "description": capex_desc
                })
                st.rerun()

    if st.session_state.cf_capex_rows:
        for i, item in enumerate(st.session_state.cf_capex_rows):
            c1, c2, c3, c4 = st.columns([2, 2, 3, 1])
            with c1:
                st.text(item["month"])
            with c2:
                st.text(fmt_inr(item["amount"]))
            with c3:
                st.text(item.get("description", ""))
            with c4:
                if st.button("Remove", key=f"rm_capex_{i}"):
                    st.session_state.cf_capex_rows.pop(i)
                    st.rerun()

st.session_state.cf_assumptions["planned_capex"] = st.session_state.cf_capex_rows

with st.expander("Expected Large Receipts"):
    st.caption("Add expected one-time large inflows (large orders, settlements, etc.)")
    col_a, col_b, col_c, col_d = st.columns([2, 2, 3, 1])
    with col_a:
        rec_month = st.text_input("Month (YYYYMM)", key="rec_month_input", placeholder="202604")
    with col_b:
        rec_amt = st.number_input("Amount (Rs)", min_value=0, step=50000, key="rec_amt_input")
    with col_c:
        rec_desc = st.text_input("Description", key="rec_desc_input", placeholder="Large order payment")
    with col_d:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("Add", key="add_receipt"):
            if rec_month and rec_amt > 0:
                st.session_state.cf_receipt_rows.append({
                    "month": rec_month, "amount": rec_amt, "description": rec_desc
                })
                st.rerun()

    if st.session_state.cf_receipt_rows:
        for i, item in enumerate(st.session_state.cf_receipt_rows):
            c1, c2, c3, c4 = st.columns([2, 2, 3, 1])
            with c1:
                st.text(item["month"])
            with c2:
                st.text(fmt_inr(item["amount"]))
            with c3:
                st.text(item.get("description", ""))
            with c4:
                if st.button("Remove", key=f"rm_rec_{i}"):
                    st.session_state.cf_receipt_rows.pop(i)
                    st.rerun()

st.session_state.cf_assumptions["expected_receipts"] = st.session_state.cf_receipt_rows

with st.expander("New Hires"):
    st.caption("Plan new employee additions")
    col_a, col_b, col_c, col_d = st.columns([2, 2, 2, 1])
    with col_a:
        hire_month = st.text_input("Month (YYYYMM)", key="hire_month_input", placeholder="202605")
    with col_b:
        hire_count = st.number_input("Count", min_value=1, step=1, key="hire_count_input")
    with col_c:
        hire_cost = st.number_input("Monthly Cost (Rs each)", min_value=0, step=5000, key="hire_cost_input")
    with col_d:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("Add", key="add_hire"):
            if hire_month and hire_cost > 0:
                st.session_state.cf_hire_rows.append({
                    "month": hire_month, "count": hire_count, "monthly_cost": hire_cost
                })
                st.rerun()

    if st.session_state.cf_hire_rows:
        for i, item in enumerate(st.session_state.cf_hire_rows):
            c1, c2, c3, c4 = st.columns([2, 2, 2, 1])
            with c1:
                st.text(item["month"])
            with c2:
                st.text(f"{item['count']} people")
            with c3:
                st.text(fmt_inr(item["monthly_cost"]))
            with c4:
                if st.button("Remove", key=f"rm_hire_{i}"):
                    st.session_state.cf_hire_rows.pop(i)
                    st.rerun()

st.session_state.cf_assumptions["new_hires"] = st.session_state.cf_hire_rows


# ---------------------------------------------------------------------------
# GENERATE FORECASTS
# ---------------------------------------------------------------------------

forecasts = {}
for sc in st.session_state.cf_scenarios_selected:
    forecasts[sc] = forecast_cashflow(
        historical,
        assumptions=st.session_state.cf_assumptions,
        months_ahead=st.session_state.cf_horizon,
        scenario=sc,
    )

# ---------------------------------------------------------------------------
# SECTION 3: FORECAST CHART
# ---------------------------------------------------------------------------

section_header("CASH FLOW FORECAST")

fig_fc = go.Figure()

# Historical bars (solid)
fig_fc.add_trace(go.Bar(
    x=[d["label"] for d in monthly_data[-6:]],
    y=[d["receipts"] for d in monthly_data[-6:]],
    name="Historical Receipts",
    marker_color="#059669",
    opacity=0.9,
))
fig_fc.add_trace(go.Bar(
    x=[d["label"] for d in monthly_data[-6:]],
    y=[d["payments"] for d in monthly_data[-6:]],
    name="Historical Payments",
    marker_color="#dc2626",
    opacity=0.9,
))

# Scenario colors
scenario_colors = {
    "base": "#2563eb",
    "optimistic": "#059669",
    "pessimistic": "#dc2626",
}

# Forecast bars (lighter) -- only for primary scenario
primary = st.session_state.cf_scenarios_selected[0] if st.session_state.cf_scenarios_selected else "base"
if primary in forecasts:
    fc_data = forecasts[primary]["forecast_months"]
    fig_fc.add_trace(go.Bar(
        x=[fm["label"] for fm in fc_data],
        y=[fm["projected_receipts"] for fm in fc_data],
        name=f"Forecast Receipts ({primary.title()})",
        marker_color="#059669",
        opacity=0.45,
        marker_pattern_shape="/",
    ))
    fig_fc.add_trace(go.Bar(
        x=[fm["label"] for fm in fc_data],
        y=[fm["projected_payments"] for fm in fc_data],
        name=f"Forecast Payments ({primary.title()})",
        marker_color="#dc2626",
        opacity=0.45,
        marker_pattern_shape="/",
    ))

# Closing bank balance lines for all scenarios
for sc_name, fc_result in forecasts.items():
    fc_months = fc_result["forecast_months"]
    # Combine last historical month closing with forecast
    hist_last_label = monthly_data[-1]["label"] if monthly_data else ""
    x_vals = [hist_last_label] + [fm["label"] for fm in fc_months]
    y_vals = [current_pos.get("total_liquid", 0)] + [fm["projected_closing_bank"] for fm in fc_months]

    sc_color = scenario_colors.get(sc_name, "#6366f1")
    fig_fc.add_trace(go.Scatter(
        x=x_vals, y=y_vals,
        name=f"Closing Bank ({sc_name.title()})",
        mode="lines+markers",
        line=dict(color=sc_color, width=2.5, dash="dot" if sc_name != primary else "solid"),
        marker=dict(size=7),
    ))

# Minimum cash threshold line
min_threshold = st.session_state.cf_assumptions.get("minimum_cash_threshold", 500000)
all_x = [d["label"] for d in monthly_data[-6:]]
if primary in forecasts:
    all_x += [fm["label"] for fm in forecasts[primary]["forecast_months"]]
fig_fc.add_trace(go.Scatter(
    x=all_x,
    y=[min_threshold] * len(all_x),
    name=f"Min Cash Threshold (Rs {fmt(min_threshold)})",
    mode="lines",
    line=dict(color="#dc2626", width=1.5, dash="dash"),
    opacity=0.6,
))

fig_fc.update_layout(
    barmode="group",
    height=450,
    margin=dict(l=20, r=20, t=30, b=20),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    plot_bgcolor="white",
    yaxis=dict(gridcolor="#f1f5f9", tickformat=",.0f"),
    xaxis=dict(gridcolor="#f1f5f9"),
    font=dict(family="Inter, sans-serif", size=12),
)

st.plotly_chart(fig_fc, use_container_width=True)

# ---------------------------------------------------------------------------
# SECTION 4: CASH RUNWAY
# ---------------------------------------------------------------------------

section_header("CASH RUNWAY")

primary_fc = forecasts.get(primary, {})
runway_months = primary_fc.get("runway_months", 0)
min_cash_point = primary_fc.get("min_cash_point", {})

rc1, rc2, rc3 = st.columns(3)

with rc1:
    if runway_months >= 6:
        runway_color = "green"
    elif runway_months >= 3:
        runway_color = "amber"
    else:
        runway_color = "red"

    metric_card(
        "Cash Runway",
        f"{runway_months} months",
        f"Based on {primary} scenario",
        runway_color,
    )

with rc2:
    min_amt = min_cash_point.get("amount", 0)
    min_month_label = min_cash_point.get("month", "N/A")
    min_color = "red" if min_amt < min_threshold else "green"
    metric_card(
        "Minimum Cash Point",
        fmt_inr(min_amt),
        f"In {min_month_label}" if min_month_label else "N/A",
        min_color,
    )

with rc3:
    wc_cycle = patterns.get("working_capital_cycle", 0)
    wc_color = "green" if wc_cycle < 30 else ("amber" if wc_cycle < 60 else "red")
    metric_card(
        "Working Capital Cycle",
        f"{wc_cycle:.0f} days",
        f"DSO {patterns.get('dso', 0):.0f}d - DPO {patterns.get('dpo', 0):.0f}d",
        wc_color,
    )

# Runway progress bar
runway_pct = min(100, runway_months / 12 * 100)
if runway_months >= 6:
    bar_color = "#059669"
elif runway_months >= 3:
    bar_color = "#d97706"
else:
    bar_color = "#dc2626"

st.markdown(f"""
<div style="background: #f1f5f9; border-radius: 8px; height: 12px; margin: 8px 0 24px 0; overflow: hidden;">
    <div style="background: {bar_color}; height: 100%; width: {runway_pct}%; border-radius: 8px;
                transition: width 0.5s ease;"></div>
</div>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# SECTION 5: ALERTS & RECOMMENDATIONS
# ---------------------------------------------------------------------------

section_header("ALERTS & RECOMMENDATIONS")

all_alerts = primary_fc.get("alerts", [])

# Deduplicate and prioritize
critical_alerts = [a for a in all_alerts if a.get("severity") == "critical"]
warning_alerts = [a for a in all_alerts if a.get("severity") == "warning"]
info_alerts = [a for a in all_alerts if a.get("severity") == "info"]

if critical_alerts:
    for a in critical_alerts:
        info_banner(
            f'<strong>[{a["type"]}]</strong> {a["month"]}: {a["message"]}',
            "error",
        )

if warning_alerts:
    # Show unique warning types
    seen = set()
    for a in warning_alerts:
        key = f"{a['type']}_{a['month']}"
        if key not in seen:
            seen.add(key)
            info_banner(
                f'<strong>[{a["type"]}]</strong> {a["month"]}: {a["message"]}',
                "warning",
            )

# Info alerts in expander
if info_alerts:
    with st.expander(f"Upcoming Obligations ({len(info_alerts)} items)"):
        # Group by type
        alert_by_type = {}
        for a in info_alerts:
            t = a["type"]
            if t not in alert_by_type:
                alert_by_type[t] = []
            alert_by_type[t].append(a)

        for atype, items in alert_by_type.items():
            type_labels = {
                "GST_DUE": "GST Payments",
                "TDS_DUE": "TDS Payments",
                "ADVANCE_TAX": "Advance Tax",
                "CAPEX_PLANNED": "Capital Expenditure",
                "EMI_DUE": "Loan EMI",
            }
            st.markdown(f"**{type_labels.get(atype, atype)}**")
            for a in items:
                st.markdown(f"- {a['month']}: {a['message']}")

if not all_alerts:
    info_banner("No alerts for the forecast period. Cash position looks stable.", "success")

# Recommendations
section_header("RECOMMENDATIONS")
recommendations = []

avg_net = patterns.get("avg_net_cash", 0)
if avg_net > 0:
    recommendations.append("Positive average net cash flow indicates healthy operations.")
else:
    recommendations.append("Average net cash flow is negative -- consider cost optimization or revenue acceleration.")

if patterns.get("dso", 0) > 45:
    recommendations.append(
        f"DSO of {patterns.get('dso', 0):.0f} days is high. Consider tightening credit terms or "
        "improving collection follow-up to accelerate receivables."
    )

if patterns.get("receipt_trend") == "decreasing":
    recommendations.append(
        "Receipt trend is declining. Review customer pipeline and consider sales initiatives."
    )

if patterns.get("receipt_trend") == "increasing":
    recommendations.append(
        "Receipt trend is growing -- a positive signal. Ensure capacity can handle increased demand."
    )

if min_cash_point.get("amount", 0) < min_threshold:
    recommendations.append(
        f"Cash is projected to drop below the Rs {fmt(min_threshold)} threshold in "
        f"{min_cash_point.get('month', 'upcoming months')}. Plan for a line of credit or defer non-essential expenses."
    )

seasonal_high = patterns.get("seasonal_months", {}).get("high", [])
seasonal_low = patterns.get("seasonal_months", {}).get("low", [])
if seasonal_low:
    recommendations.append(
        f"Seasonal low months: {', '.join(seasonal_low)}. Build cash reserves before these periods."
    )

for rec in recommendations:
    st.markdown(f"- {rec}")


# ---------------------------------------------------------------------------
# SECTION 6: MONTH-BY-MONTH FORECAST TABLE
# ---------------------------------------------------------------------------

section_header("MONTH-BY-MONTH FORECAST")

# Tabs for each scenario
if len(forecasts) > 1:
    tab_names = [sc.title() for sc in forecasts.keys()]
    tabs = st.tabs(tab_names)
    tab_map = dict(zip(forecasts.keys(), tabs))
else:
    tab_map = {list(forecasts.keys())[0]: st.container()} if forecasts else {}

for sc_name, container in tab_map.items():
    fc_result = forecasts[sc_name]
    fc_months = fc_result["forecast_months"]
    min_thresh = st.session_state.cf_assumptions.get("minimum_cash_threshold", 500000)

    with container:
        table_html = '<table class="slv-table"><thead><tr>'
        table_html += '<th>Month</th><th>Receipts</th><th>Payments</th>'
        table_html += '<th>Net Cash</th><th>Closing Bank</th>'
        table_html += '<th>GST</th><th>TDS</th><th>Adv Tax</th>'
        table_html += '<th>Salary</th><th>Rent</th><th>EMI</th>'
        table_html += '<th>Conf.</th>'
        table_html += '</tr></thead><tbody>'

        for fm in fc_months:
            # Highlight rows below threshold
            row_style = ""
            if fm["projected_closing_bank"] < 0:
                row_style = ' style="background: #fef2f2;"'
            elif fm["projected_closing_bank"] < min_thresh:
                row_style = ' style="background: #fffbeb;"'

            net_class = "amt-pos" if fm["projected_net"] >= 0 else "amt-neg"
            bank_class = "amt-pos" if fm["projected_closing_bank"] >= 0 else "amt-neg"

            # Confidence badge
            conf = fm.get("confidence", 0)
            if conf >= 0.8:
                conf_badge = badge(f"{conf:.0%}", "green")
            elif conf >= 0.6:
                conf_badge = badge(f"{conf:.0%}", "amber")
            else:
                conf_badge = badge(f"{conf:.0%}", "red")

            table_html += f'<tr{row_style}>'
            table_html += f'<td>{fm["label"]}</td>'
            table_html += f'<td class="amt">{fmt(fm["projected_receipts"])}</td>'
            table_html += f'<td class="amt">{fmt(fm["projected_payments"])}</td>'
            table_html += f'<td class="amt {net_class}">{fmt(fm["projected_net"])}</td>'
            table_html += f'<td class="amt {bank_class}">{fmt(fm["projected_closing_bank"])}</td>'
            table_html += f'<td class="amt">{fmt(fm.get("gst_payment", 0))}</td>'
            table_html += f'<td class="amt">{fmt(fm.get("tds_payment", 0))}</td>'
            table_html += f'<td class="amt">{fmt(fm.get("advance_tax", 0))}</td>'
            table_html += f'<td class="amt">{fmt(fm.get("salary", 0))}</td>'
            table_html += f'<td class="amt">{fmt(fm.get("rent", 0))}</td>'
            table_html += f'<td class="amt">{fmt(fm.get("emi", 0))}</td>'
            table_html += f'<td>{conf_badge}</td>'
            table_html += '</tr>'

        table_html += '</tbody></table>'
        st.markdown(table_html, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# DOWNLOAD EXCEL
# ---------------------------------------------------------------------------

section_header("EXPORT")

col_dl1, col_dl2, _ = st.columns([2, 2, 4])

with col_dl1:
    if st.button("Download Forecast (Excel)"):
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            output_path = tmp.name
        try:
            result_path = export_forecast_excel(historical, forecasts, output_path)
            if result_path and os.path.exists(result_path):
                with open(result_path, "rb") as f:
                    st.download_button(
                        label="Save Excel File",
                        data=f.read(),
                        file_name=f"cashflow_forecast_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
            else:
                st.warning("Export completed as CSV (openpyxl not available)")
                csv_path = output_path.replace(".xlsx", ".csv")
                if os.path.exists(csv_path):
                    with open(csv_path, "r") as f:
                        st.download_button(
                            label="Save CSV File",
                            data=f.read(),
                            file_name=f"cashflow_forecast_{datetime.now().strftime('%Y%m%d')}.csv",
                            mime="text/csv",
                        )
        except Exception as e:
            st.error(f"Export failed: {e}")

with col_dl2:
    # Quick summary stats
    st.caption(
        f"Data: {len(monthly_data)} months historical | "
        f"{st.session_state.cf_horizon} months forecast | "
        f"{len(st.session_state.cf_scenarios_selected)} scenario(s)"
    )


# ---------------------------------------------------------------------------
# PATTERN INSIGHTS (small section)
# ---------------------------------------------------------------------------

with st.expander("Pattern Analysis"):
    pc1, pc2, pc3 = st.columns(3)
    with pc1:
        rt = patterns.get("receipt_trend", "stable")
        rt_badge = badge(rt.upper(), "green" if rt == "increasing" else ("red" if rt == "decreasing" else "gray"))
        st.markdown(f"**Receipt Trend:** {rt_badge}", unsafe_allow_html=True)
        st.markdown(f"Avg Monthly Receipts: **{fmt_inr(patterns.get('avg_monthly_receipts', 0))}**")

    with pc2:
        pt = patterns.get("payment_trend", "stable")
        pt_badge = badge(pt.upper(), "red" if pt == "increasing" else ("green" if pt == "decreasing" else "gray"))
        st.markdown(f"**Payment Trend:** {pt_badge}", unsafe_allow_html=True)
        st.markdown(f"Avg Monthly Payments: **{fmt_inr(patterns.get('avg_monthly_payments', 0))}**")

    with pc3:
        st.markdown(f"**Avg Net Cash Flow:** {fmt_inr(patterns.get('avg_net_cash', 0))}")
        if seasonal_high:
            st.markdown(f"High Months: **{', '.join(seasonal_high)}**")
        if seasonal_low:
            st.markdown(f"Low Months: **{', '.join(seasonal_low)}**")


# ---------------------------------------------------------------------------
# FOOTER
# ---------------------------------------------------------------------------

footer()
