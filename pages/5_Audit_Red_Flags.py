"""
Seven Labs Vision -- Audit Red Flags Dashboard
Automated audit checks on Tally company data with risk scoring.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from audit_engine import run_all_checks

st.set_page_config(page_title="Audit Red Flags", page_icon="", layout="wide")

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from styles import inject_base_styles, page_header, section_header, metric_card, fmt, fmt_full, badge, footer, empty_state, info_banner
inject_base_styles()

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tally_data.db")


# -- Helpers ------------------------------------------------------------------

def fmt_inr(amount):
    """Format amount in Indian style."""
    if amount is None:
        return "Rs 0"
    try:
        amount = float(amount)
    except (ValueError, TypeError):
        return "Rs 0"
    abs_amt = abs(amount)
    sign = "-" if amount < 0 else ""
    if abs_amt >= 10000000:
        return f"{sign}Rs {abs_amt / 10000000:.2f} Cr"
    elif abs_amt >= 100000:
        return f"{sign}Rs {abs_amt / 100000:.2f} L"
    elif abs_amt >= 1000:
        return f"{sign}Rs {abs_amt:,.0f}"
    else:
        return f"{sign}Rs {abs_amt:.2f}"


def status_badge_html(status):
    """Return badge() HTML for status."""
    s = (status or "").lower()
    if s == "pass":
        return badge("PASS", "green")
    elif s == "fail":
        return badge("FAIL", "red")
    elif s in ("skipped", "error"):
        return badge("SKIP", "gray")
    return badge(status or "N/A", "gray")


def severity_badge_html(severity):
    """Return badge() HTML for severity."""
    s = (severity or "").lower()
    if s == "high":
        return badge("HIGH", "red")
    elif s == "medium":
        return badge("MED", "amber")
    elif s == "low":
        return badge("LOW", "green")
    return badge(severity or "N/A", "gray")


def status_badge(status):
    """Return colored text for status (for expander headers that don't support HTML)."""
    s = (status or "").lower()
    if s == "pass":
        return ":green[PASS]"
    elif s == "fail":
        return ":red[FAIL]"
    elif s in ("skipped", "error"):
        return ":gray[SKIP]"
    return status or ""


def severity_badge(severity):
    """Return colored text for severity (for expander headers)."""
    s = (severity or "").lower()
    if s == "high":
        return ":red[High]"
    elif s == "medium":
        return ":orange[Medium]"
    elif s == "low":
        return ":green[Low]"
    return severity or ""


def safe_df(records, columns=None):
    """Create a DataFrame safely, returning empty DF if no records."""
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    if columns:
        for c in columns:
            if c not in df.columns:
                df[c] = ""
        df = df[columns]
    return df


# -- Company name -------------------------------------------------------------

try:
    import sqlite3
    _conn = sqlite3.connect(DB_PATH)
    _row = _conn.execute("SELECT value FROM _metadata WHERE key='company_name'").fetchone()
    COMPANY = _row[0] if _row else "Company"
    _conn.close()
except Exception:
    COMPANY = "Company"


# -- Header -------------------------------------------------------------------

page_header("Audit Red Flags", COMPANY)


# -- Run Audit ----------------------------------------------------------------

if "audit_results" not in st.session_state:
    st.session_state.audit_results = None

col_btn, col_status = st.columns([1, 4])
with col_btn:
    run_clicked = st.button("Run Audit", type="primary")
with col_status:
    if st.session_state.audit_results is None:
        st.info("Click Run Audit to begin.")

if run_clicked:
    with st.spinner("Running 11 audit checks..."):
        st.session_state.audit_results = run_all_checks(DB_PATH)

results = st.session_state.audit_results
if results is None:
    st.stop()

summary = results.get("_summary", {})


# -- Risk Score Gauge ---------------------------------------------------------

risk_score = summary.get("risk_score", 0)
if risk_score < 30:
    gauge_color = "#22c55e"
elif risk_score < 60:
    gauge_color = "#eab308"
else:
    gauge_color = "#ef4444"

fig_gauge = go.Figure(go.Indicator(
    mode="gauge+number",
    value=risk_score,
    title={"text": "Risk Score"},
    gauge={
        "axis": {"range": [0, 100], "tickwidth": 1},
        "bar": {"color": gauge_color},
        "steps": [
            {"range": [0, 30], "color": "#dcfce7"},
            {"range": [30, 60], "color": "#fef9c3"},
            {"range": [60, 100], "color": "#fee2e2"},
        ],
        "threshold": {
            "line": {"color": "black", "width": 2},
            "thickness": 0.8,
            "value": risk_score,
        },
    },
))
fig_gauge.update_layout(height=250, margin=dict(l=20, r=20, t=40, b=20))

gc1, gc2 = st.columns([1, 2])
with gc1:
    st.plotly_chart(fig_gauge, use_container_width=True)
with gc2:
    st.markdown("")  # spacer
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        metric_card("Total Checks", summary.get("total_checks", 0), color_class="blue")
    with m2:
        metric_card("Flags Found", summary.get("total_flags", 0), color_class="amber")
    with m3:
        metric_card("High Severity", summary.get("high_severity_checks", 0), color_class="red")
    with m4:
        metric_card("Medium Severity", summary.get("medium_severity_checks", 0), color_class="amber")

st.markdown("---")


# -- Check-by-check results --------------------------------------------------

CHECK_KEYS = [
    "benfords_law",
    "duplicate_invoices",
    "voucher_gaps",
    "holiday_entries",
    "cash_limit_breach",
    "round_amount_entries",
    "negative_cash_balance",
    "debit_creditors",
    "credit_debtors",
    "period_end_journals",
    "large_journal_entries",
]

for key in CHECK_KEYS:
    data = results.get(key)
    if not data:
        continue

    check_name = data.get("check", key)
    status = data.get("status", "")
    severity = data.get("severity", "")
    flag_count = data.get("flag_count", 0)
    description = data.get("description", "")

    header_text = f"{check_name}  --  {status_badge(status)}  |  Severity: {severity_badge(severity)}  |  Flags: {flag_count}"

    with st.expander(header_text, expanded=(status == "fail")):
        # Show badges using the design system
        st.markdown(
            f"{status_badge_html(status)} {severity_badge_html(severity)} Flags: **{flag_count}**",
            unsafe_allow_html=True,
        )
        st.markdown(description)

        if status in ("skipped", "error"):
            reason = data.get("reason", data.get("error", "Check was skipped."))
            st.warning(reason)
            continue

        if status == "pass" and flag_count == 0:
            st.success("No issues found.")
            continue

        # ---- 1. Benford's Law ----
        if key == "benfords_law":
            dist = data.get("digit_distribution", {})
            if dist:
                digits = list(range(1, 10))
                expected_vals = [dist.get(d, {}).get("expected", 0) or dist.get(str(d), {}).get("expected", 0) for d in digits]
                observed_vals = [dist.get(d, {}).get("observed", 0) or dist.get(str(d), {}).get("observed", 0) for d in digits]

                fig_ben = go.Figure()
                fig_ben.add_trace(go.Bar(
                    x=[str(d) for d in digits],
                    y=expected_vals,
                    name="Expected (Benford)",
                    marker_color="#93c5fd",
                ))
                fig_ben.add_trace(go.Bar(
                    x=[str(d) for d in digits],
                    y=observed_vals,
                    name="Observed",
                    marker_color="#f87171",
                ))
                fig_ben.update_layout(
                    barmode="group",
                    title="First Digit Distribution",
                    xaxis_title="Leading Digit",
                    yaxis_title="Percentage (%)",
                    height=350,
                    margin=dict(l=20, r=20, t=40, b=20),
                )
                st.plotly_chart(fig_ben, use_container_width=True)

            chi_sq = data.get("chi_square", 0)
            crit = data.get("critical_value_95", 15.507)
            susp = data.get("is_suspicious", False)
            st.markdown(f"**Chi-square:** {chi_sq}  |  **Critical value (95%):** {crit}  |  **Suspicious:** {'Yes' if susp else 'No'}")
            st.markdown(f"**Total transactions analysed:** {data.get('total_transactions', 0)}")

            devs = data.get("deviations", [])
            if devs:
                st.markdown("**Deviant digits:**")
                st.dataframe(safe_df(devs, ["digit", "expected_pct", "observed_pct", "deviation_pct", "direction"]),
                             use_container_width=True, hide_index=True)

        # ---- 2. Duplicate Invoices ----
        elif key == "duplicate_invoices":
            exact = data.get("exact_duplicates", [])
            amt_dupes = data.get("amount_date_party_duplicates", [])
            if exact:
                st.markdown("**Exact duplicate voucher numbers:**")
                st.dataframe(safe_df(exact, ["voucher_number", "party", "type", "date", "duplicate_count"]),
                             use_container_width=True, hide_index=True)
            if amt_dupes:
                st.markdown("**Same party + date + amount:**")
                df_ad = safe_df(amt_dupes, ["party", "date", "type", "amount", "count"])
                if not df_ad.empty and "amount" in df_ad.columns:
                    df_ad["amount"] = df_ad["amount"].apply(lambda x: fmt_inr(x))
                st.dataframe(df_ad, use_container_width=True, hide_index=True)

        # ---- 3. Voucher Gaps ----
        elif key == "voucher_gaps":
            gaps_by_type = data.get("gaps_by_type", [])
            for gap_info in gaps_by_type:
                vtype = gap_info.get("voucher_type", "")
                total_gaps = gap_info.get("total_gaps", 0)
                total_missing = gap_info.get("total_missing", 0)
                st.markdown(f"**{vtype}** -- {total_gaps} gap(s), {total_missing} missing voucher(s)")
                details = gap_info.get("details", [])
                if details:
                    st.dataframe(safe_df(details, ["from_number", "to_number", "missing_count"]),
                                 use_container_width=True, hide_index=True)

        # ---- 4. Sunday/Holiday Entries ----
        elif key == "holiday_entries":
            sun = data.get("sunday_entries", [])
            hol = data.get("holiday_entries", [])
            st.markdown(f"**Sunday entries:** {data.get('sunday_count', len(sun))}  |  **Holiday entries:** {data.get('holiday_count', len(hol))}")
            if sun:
                df_sun = safe_df(sun, ["date", "day", "voucher_type", "voucher_number", "party"])
                st.dataframe(df_sun, use_container_width=True, hide_index=True)

                # Count by month bar chart
                try:
                    months = [s["date"].split("-")[1] for s in sun if s.get("date")]
                    if months:
                        month_counts = pd.Series(months).value_counts().sort_index()
                        st.bar_chart(month_counts)
                except Exception:
                    pass

            if hol:
                st.markdown("**National holiday entries:**")
                st.dataframe(safe_df(hol, ["date", "holiday", "voucher_type", "voucher_number", "party"]),
                             use_container_width=True, hide_index=True)

        # ---- 5. Cash Limits ----
        elif key == "cash_limit_breach":
            breaches = data.get("breaches", [])
            if breaches:
                df_cash = safe_df(breaches, ["date", "amount", "party", "direction", "voucher_type", "voucher_number"])
                if not df_cash.empty and "amount" in df_cash.columns:
                    df_cash["amount"] = df_cash["amount"].apply(lambda x: fmt_inr(x))
                st.dataframe(df_cash, use_container_width=True, hide_index=True)
                st.caption(f"Threshold: Rs 2,00,000 per Section 269ST. Cash ledgers: {', '.join(data.get('cash_ledgers', []))}")

        # ---- 6. Round Amounts ----
        elif key == "round_amount_entries":
            entries = data.get("entries", [])
            if entries:
                df_round = safe_df(entries, ["date", "ledger", "amount", "roundness", "voucher_number", "party"])
                if not df_round.empty and "amount" in df_round.columns:
                    df_round["amount"] = df_round["amount"].apply(lambda x: fmt_inr(x))
                st.dataframe(df_round, use_container_width=True, hide_index=True)

        # ---- 7. Negative Cash Balance ----
        elif key == "negative_cash_balance":
            neg_dates = data.get("negative_dates", [])
            if neg_dates:
                # Line chart of daily balance
                df_neg = safe_df(neg_dates, ["date", "cash_ledger", "balance"])
                if not df_neg.empty:
                    try:
                        df_neg["balance_num"] = pd.to_numeric(df_neg["balance"], errors="coerce")
                        fig_cash = go.Figure()
                        fig_cash.add_trace(go.Scatter(
                            x=df_neg["date"],
                            y=df_neg["balance_num"],
                            mode="lines+markers",
                            line=dict(color="#ef4444"),
                            name="Cash Balance",
                        ))
                        fig_cash.add_hline(y=0, line_dash="dash", line_color="gray")
                        fig_cash.update_layout(
                            title="Dates with Negative Cash Balance",
                            xaxis_title="Date",
                            yaxis_title="Balance (Rs)",
                            height=300,
                            margin=dict(l=20, r=20, t=40, b=20),
                        )
                        st.plotly_chart(fig_cash, use_container_width=True)
                    except Exception:
                        pass

                    df_display = df_neg[["date", "cash_ledger", "balance"]].copy()
                    df_display["balance"] = df_display["balance"].apply(lambda x: fmt_inr(x))
                    st.dataframe(df_display, use_container_width=True, hide_index=True)

        # ---- 8. Debit Creditors ----
        elif key == "debit_creditors":
            parties = data.get("parties", [])
            if parties:
                df_dc = safe_df(parties, ["party", "debit_balance"])
                if not df_dc.empty and "debit_balance" in df_dc.columns:
                    df_dc["debit_balance"] = df_dc["debit_balance"].apply(lambda x: fmt_inr(x))
                st.dataframe(df_dc, use_container_width=True, hide_index=True)

        # ---- 9. Credit Debtors ----
        elif key == "credit_debtors":
            parties = data.get("parties", [])
            if parties:
                df_cd = safe_df(parties, ["party", "credit_balance"])
                if not df_cd.empty and "credit_balance" in df_cd.columns:
                    df_cd["credit_balance"] = df_cd["credit_balance"].apply(lambda x: fmt_inr(x))
                st.dataframe(df_cd, use_container_width=True, hide_index=True)

        # ---- 10. Period-End Journals ----
        elif key == "period_end_journals":
            entries = data.get("entries", [])
            if entries:
                df_pe = safe_df(entries, ["date", "voucher_number", "ledger", "amount", "party", "narration"])
                if not df_pe.empty and "amount" in df_pe.columns:
                    df_pe["amount"] = df_pe["amount"].apply(lambda x: fmt_inr(x))
                st.dataframe(df_pe, use_container_width=True, hide_index=True)

        # ---- 11. Large Journals ----
        elif key == "large_journal_entries":
            entries = data.get("entries", [])
            if entries:
                df_lj = safe_df(entries, ["amount", "std_devs_from_mean", "date", "voucher_number", "ledger", "narration"])
                if not df_lj.empty and "amount" in df_lj.columns:
                    df_lj["amount"] = df_lj["amount"].apply(lambda x: fmt_inr(x))
                st.dataframe(df_lj, use_container_width=True, hide_index=True)

                threshold = data.get("threshold", 0)
                mean_val = data.get("mean", 0)
                std_val = data.get("std_dev", 0)
                st.caption(f"Threshold: {fmt_inr(threshold)} (mean {fmt_inr(mean_val)} + 3 x std dev {fmt_inr(std_val)})")


# -- Footer & Persistent Chat Bar --------------------------------------------

footer()

from chat_engine import ask, format_result_as_text

chat_input = st.chat_input("Ask anything about audit findings, ledgers, vouchers...")
if chat_input:
    result = ask(chat_input)
    st.markdown(f"**You:** {chat_input}")
    if result.get("type") == "chat":
        st.markdown(result.get("message", ""))
    else:
        st.markdown(format_result_as_text(result))
