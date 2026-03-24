"""
TDS Analysis Dashboard -- SLV
Section-wise, party-wise, monthly, quarterly analysis with compliance checks.
"""

import streamlit as st
import sqlite3
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tds_engine import (
    get_conn, get_tds_available_months, format_indian,
    tds_summary_by_section, tds_party_wise, tds_monthly_trend,
    tds_quarterly_summary, tds_threshold_check, tds_pan_check,
    tds_rate_verification, tds_party_vouchers,
    _detect_tds_ledgers, _get_company_name, _tds_ledger_section_map,
    TDS_SECTIONS, DB_PATH,
)

st.set_page_config(page_title="TDS Analysis -- SLV", page_icon="T", layout="wide")

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from styles import inject_base_styles, page_header, section_header, metric_card, fmt, fmt_full, badge, footer
inject_base_styles()

# ======================================================================
#  SESSION STATE DEFAULTS
# ======================================================================

_defaults = {
    "tds_view": "main",
    "tds_drill_party": None,
    "tds_back_tab": None,
}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ======================================================================
#  HELPERS
# ======================================================================

def fi(amount):
    """Format Indian number."""
    return format_indian(amount)


def _status_badge(status):
    """Return HTML badge for compliance status."""
    s = status.upper() if status else ""
    if s in ("OK", "OK (NO PAN - 20%)", "BELOW_THRESHOLD"):
        return badge(status, "green")
    elif s in ("WARNING", "MINOR_DIFF", "CHECK_PAN_RATE"):
        return badge(status, "amber")
    elif s in ("BREACH", "MISMATCH", "MISSING PAN"):
        return badge(status, "red")
    else:
        return badge(status, "blue")


def _month_label_short(m):
    """YYYYMM -> Apr'25 style label."""
    mn = {"01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
          "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
          "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec"}
    if m and len(m) == 6:
        return f"{mn.get(m[4:6], m[4:6])}'{m[2:4]}"
    return m or ""


# ======================================================================
#  DATA LOAD
# ======================================================================

def _safe_div(a, b, default=0):
    """Safe division."""
    if not b:
        return default
    return a / b


def _safe_cols(conn, table):
    """Return set of column names for a table."""
    try:
        return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        return set()


conn = get_conn()
company_name = _get_company_name(conn)
tds_ledgers = _detect_tds_ledgers(conn)
tds_months = get_tds_available_months(conn)

# ======================================================================
#  HEADER
# ======================================================================

page_header(
    "TDS Analysis Dashboard",
    f"{company_name or 'Company'} | Tax Deducted at Source | {len(tds_ledgers)} TDS ledger(s) detected"
)

# ======================================================================
#  NO TDS CHECK
# ======================================================================

if not tds_ledgers:
    st.info("No TDS transactions found in the loaded data. "
            "TDS ledgers are typically under the 'Duties & Taxes' group with names containing 'TDS'. "
            "Please ensure Tally data includes TDS ledgers and transactions.")
    conn.close()
    st.stop()

# ======================================================================
#  DATE FILTER (SIDEBAR)
# ======================================================================

with st.sidebar:
    import datetime as _dt
    _min_date_row = conn.execute("SELECT MIN(DATE) FROM trn_voucher").fetchone()
    _max_date_row = conn.execute("SELECT MAX(DATE) FROM trn_voucher").fetchone()
    _min_dt = _dt.date(int(_min_date_row[0][:4]), int(_min_date_row[0][4:6]), int(_min_date_row[0][6:8])) if _min_date_row and _min_date_row[0] else _dt.date(2025, 4, 1)
    _max_dt = _dt.date(int(_max_date_row[0][:4]), int(_max_date_row[0][4:6]), int(_max_date_row[0][6:8])) if _max_date_row and _max_date_row[0] else _dt.date.today()
    if "applied_start_date" not in st.session_state:
        st.session_state.applied_start_date = _min_dt
    if "applied_end_date" not in st.session_state:
        st.session_state.applied_end_date = _max_dt
    st.markdown("### Date Range")
    _from = st.date_input("From", value=st.session_state.applied_start_date, min_value=_min_dt, max_value=_max_dt, key="tds_filter_from")
    _to = st.date_input("To", value=st.session_state.applied_end_date, min_value=_min_dt, max_value=_max_dt, key="tds_filter_to")
    _c1, _c2 = st.columns(2)
    with _c1:
        if st.button("Apply", key="tds_apply_dates", use_container_width=True, type="primary"):
            st.session_state.applied_start_date = _from
            st.session_state.applied_end_date = _to
            st.rerun()
    with _c2:
        if st.button("Reset", key="tds_reset_dates", use_container_width=True):
            st.session_state.applied_start_date = _min_dt
            st.session_state.applied_end_date = _max_dt
            st.rerun()
    date_from = st.session_state.applied_start_date.strftime("%Y%m%d")
    date_to = st.session_state.applied_end_date.strftime("%Y%m%d")
    st.caption(f"Showing: {st.session_state.applied_start_date.strftime('%d %b %Y')} — {st.session_state.applied_end_date.strftime('%d %b %Y')}")
    if not tds_months:
        st.info("No TDS transactions found.")

    st.markdown("---")
    st.markdown("### TDS Ledgers Detected")
    for ld in tds_ledgers:
        st.markdown(f"- **{ld['name']}** ({ld['section']})")

# ======================================================================
#  ROUTING
# ======================================================================

view = st.session_state.tds_view

if view == "party_drill":
    # -- PARTY DRILL-DOWN --------------------------------------------------
    party = st.session_state.tds_drill_party
    back_tab = st.session_state.tds_back_tab or "Party-wise Detail"

    if st.button("< Back to TDS Dashboard", key="back_from_drill"):
        st.session_state.tds_view = "main"
        st.rerun()

    if not party:
        st.warning("No party selected.")
    else:
        st.subheader(f"TDS Vouchers: {party}")

        vouchers = tds_party_vouchers(conn, party, date_from=date_from, date_to=date_to)

        if not vouchers:
            st.info("No TDS vouchers found for this party in the selected period.")
        else:
            total_tds = sum(v["tds_amount"] for v in vouchers)
            total_gross = sum(v["gross_amount"] for v in vouchers)

            st.markdown(f"**{len(vouchers)} voucher(s)** | Gross: **{fi(total_gross)}** | TDS: **{fi(total_tds)}**")

            html = """<table class="slv-table">
            <tr><th>Date</th><th>Voucher No</th><th>Type</th><th>Section</th>
                <th>Gross Amount</th><th>TDS Amount</th><th>Narration</th></tr>"""

            for v in vouchers:
                html += f"""<tr>
                    <td>{v['date']}</td>
                    <td>{v['voucher_no'] or '-'}</td>
                    <td>{v['voucher_type'] or '-'}</td>
                    <td>{v['section']}</td>
                    <td>{fi(v['gross_amount'])}</td>
                    <td>{fi(v['tds_amount'])}</td>
                    <td style="text-align:left;max-width:200px;overflow:hidden;text-overflow:ellipsis">{v['narration'][:80]}</td>
                </tr>"""

            html += f"""<tr class="total-row">
                <td colspan="4">TOTAL</td>
                <td>{fi(total_gross)}</td>
                <td>{fi(total_tds)}</td>
                <td></td>
            </tr>"""
            html += "</table>"
            st.markdown(html, unsafe_allow_html=True)

else:
    # -- MAIN DASHBOARD ----------------------------------------------------

    # -- EXECUTIVE SUMMARY CARDS -------------------------------------------
    section_data = tds_summary_by_section(conn, date_from=date_from, date_to=date_to)
    pan_data = tds_pan_check(conn)

    total_tds = sum(s["tds_amount"] for s in section_data)
    total_parties = sum(s["parties"] for s in section_data)
    sections_covered = len(section_data)
    pan_with = sum(1 for p in pan_data if p["has_pan"])
    pan_total = len(pan_data)
    pan_pct = round(_safe_div(pan_with, pan_total) * 100, 1)

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        metric_card("Total TDS Deducted", fi(total_tds), color_class="blue")
    with m2:
        metric_card("Parties with TDS", str(total_parties))
    with m3:
        metric_card("Sections Covered", str(sections_covered))
    with m4:
        pan_color = "green" if pan_pct >= 90 else "amber" if pan_pct >= 70 else "red"
        metric_card("PAN Compliance", f"{pan_pct}%", color_class=pan_color)

    st.markdown("")

    # -- TABS --------------------------------------------------------------
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Section-wise Summary",
        "Party-wise Detail",
        "Monthly Trend",
        "Quarterly Returns",
        "Compliance Checks",
    ])

    # ==================================================================
    #  TAB 1: SECTION-WISE SUMMARY
    # ==================================================================
    with tab1:
        section_header("TDS by Section")

        if not section_data:
            st.info("No TDS data found for the selected period.")
        else:
            html = """<table class="slv-table">
            <tr><th>Section</th><th>Description</th><th>Rate (%)</th>
                <th>Threshold</th><th>Parties</th><th>TDS Amount</th></tr>"""

            for s in section_data:
                rate_str = f"{s['rate']}%" if s['rate'] is not None else "Various"
                thr_str = fi(s['threshold']) if s['threshold'] else "-"
                html += f"""<tr>
                    <td><strong>{s['section']}</strong></td>
                    <td>{s['description']}</td>
                    <td>{rate_str}</td>
                    <td>{thr_str}</td>
                    <td>{s['parties']}</td>
                    <td>{fi(s['tds_amount'])}</td>
                </tr>"""

            html += f"""<tr class="total-row">
                <td colspan="4">TOTAL</td>
                <td>{total_parties}</td>
                <td>{fi(total_tds)}</td>
            </tr>"""
            html += "</table>"
            st.markdown(html, unsafe_allow_html=True)

            # Bar chart
            if len(section_data) > 1:
                st.markdown("")
                section_header("TDS Distribution by Section")
                import pandas as pd
                chart_df = pd.DataFrame([
                    {"Section": s["section"], "TDS Amount": s["tds_amount"]}
                    for s in section_data
                ])
                st.bar_chart(chart_df.set_index("Section"))

    # ==================================================================
    #  TAB 2: PARTY-WISE DETAIL
    # ==================================================================
    with tab2:
        section_header("Party-wise TDS Detail")

        # Section filter
        all_sections = sorted(set(s["section"] for s in section_data)) if section_data else []
        section_filter = st.selectbox(
            "Filter by Section",
            ["All Sections"] + all_sections,
            key="tds_section_filter"
        )

        sel_section = None if section_filter == "All Sections" else section_filter
        party_data = tds_party_wise(conn, section=sel_section, date_from=date_from, date_to=date_to)

        if not party_data:
            st.info("No party-wise TDS data found for the selected period/section.")
        else:
            st.markdown(f"**{len(party_data)} parties**")

            html = """<table class="slv-table">
            <tr><th>Party</th><th>PAN</th><th>Section(s)</th>
                <th>Gross Payment</th><th>TDS Deducted</th><th>Eff. Rate %</th><th>Vouchers</th></tr>"""

            for p in party_data:
                pan_badge = p['pan'] if p['has_pan'] else badge("No PAN", "red")
                html += f"""<tr>
                    <td>{p['party']}</td>
                    <td>{pan_badge}</td>
                    <td>{p['sections']}</td>
                    <td>{fi(p['gross_payment'])}</td>
                    <td>{fi(p['tds_amount'])}</td>
                    <td>{p['effective_rate']}</td>
                    <td>{p['voucher_count']}</td>
                </tr>"""

            total_gross_party = sum(p["gross_payment"] for p in party_data)
            total_tds_party = sum(p["tds_amount"] for p in party_data)
            html += f"""<tr class="total-row">
                <td colspan="3">TOTAL</td>
                <td>{fi(total_gross_party)}</td>
                <td>{fi(total_tds_party)}</td>
                <td></td><td></td>
            </tr>"""
            html += "</table>"
            st.markdown(html, unsafe_allow_html=True)

            # Party drill-down buttons
            st.markdown("")
            st.markdown("**Click a party to view voucher details:**")
            # Show in rows of 4
            for i in range(0, min(len(party_data), 20), 4):
                cols = st.columns(4)
                for j in range(4):
                    idx = i + j
                    if idx < len(party_data):
                        with cols[j]:
                            p = party_data[idx]
                            label = f"{p['party'][:25]} ({fi(p['tds_amount'])})"
                            if st.button(label, key=f"drill_party_{idx}"):
                                st.session_state.tds_view = "party_drill"
                                st.session_state.tds_drill_party = p["party"]
                                st.session_state.tds_back_tab = "Party-wise Detail"
                                st.rerun()

    # ==================================================================
    #  TAB 3: MONTHLY TREND
    # ==================================================================
    with tab3:
        section_header("Monthly TDS Trend")

        monthly_data = tds_monthly_trend(conn, date_from=date_from, date_to=date_to)

        if not monthly_data:
            st.info("No monthly TDS data found for the selected period.")
        else:
            # Collect all sections across months
            all_sec = set()
            for m in monthly_data:
                all_sec.update(m["sections"].keys())
            all_sec = sorted(all_sec)

            # Table
            html = '<table class="slv-table"><tr><th>Month</th>'
            for sec in all_sec:
                html += f"<th>{sec}</th>"
            html += "<th>Total TDS</th></tr>"

            for m in monthly_data:
                html += f"<tr><td><strong>{m['month_label']}</strong></td>"
                for sec in all_sec:
                    val = m["sections"].get(sec, 0)
                    html += f"<td>{fi(val) if val else '-'}</td>"
                html += f"<td><strong>{fi(m['total_tds'])}</strong></td></tr>"

            # Totals
            grand_total = sum(m["total_tds"] for m in monthly_data)
            html += '<tr class="total-row"><td>TOTAL</td>'
            for sec in all_sec:
                sec_total = sum(m["sections"].get(sec, 0) for m in monthly_data)
                html += f"<td>{fi(sec_total)}</td>"
            html += f"<td>{fi(grand_total)}</td></tr>"
            html += "</table>"
            st.markdown(html, unsafe_allow_html=True)

            # Chart
            if len(monthly_data) > 1:
                st.markdown("")
                section_header("TDS Trend Chart")
                import pandas as pd
                chart_data = pd.DataFrame([
                    {"Month": m["month_label"], "TDS Amount": m["total_tds"]}
                    for m in monthly_data
                ])
                st.bar_chart(chart_data.set_index("Month"))

    # ==================================================================
    #  TAB 4: QUARTERLY RETURNS
    # ==================================================================
    with tab4:
        section_header("Quarterly TDS Summary (for Return Filing)")
        st.markdown("*24Q - Salary | 26Q - Non-salary | 27Q - Non-resident*")

        quarterly_data = tds_quarterly_summary(conn, date_from=date_from, date_to=date_to)

        if not quarterly_data:
            st.info("No quarterly TDS data found for the selected period.")
        else:
            # Collect all sections
            all_sec_q = set()
            for q in quarterly_data:
                all_sec_q.update(q["sections"].keys())
            all_sec_q = sorted(all_sec_q)

            html = '<table class="slv-table"><tr><th>Quarter</th><th>Parties</th>'
            for sec in all_sec_q:
                html += f"<th>{sec}</th>"
            html += "<th>Total TDS</th></tr>"

            for q in quarterly_data:
                html += f"""<tr>
                    <td><strong>{q['quarter']}</strong></td>
                    <td>{q['party_count']}</td>"""
                for sec in all_sec_q:
                    val = q["sections"].get(sec, 0)
                    html += f"<td>{fi(val) if val else '-'}</td>"
                html += f"<td><strong>{fi(q['total'])}</strong></td></tr>"

            # Grand total
            grand_q = sum(q["total"] for q in quarterly_data)
            total_parties_q = max(q["party_count"] for q in quarterly_data) if quarterly_data else 0
            html += '<tr class="total-row"><td>TOTAL</td>'
            html += f"<td>{total_parties_q}</td>"
            for sec in all_sec_q:
                sec_total = sum(q["sections"].get(sec, 0) for q in quarterly_data)
                html += f"<td>{fi(sec_total)}</td>"
            html += f"<td>{fi(grand_q)}</td></tr>"
            html += "</table>"
            st.markdown(html, unsafe_allow_html=True)

            # Return type guidance
            st.markdown("")
            has_salary = any("192" in q.get("sections", {}) for q in quarterly_data)
            has_non_salary = any(
                any(s != "192" and s != "206C" for s in q.get("sections", {}).keys())
                for q in quarterly_data
            )
            has_tcs = any("206C" in q.get("sections", {}) for q in quarterly_data)

            guidance_parts = []
            if has_salary:
                guidance_parts.append("**24Q** (Salary TDS)")
            if has_non_salary:
                guidance_parts.append("**26Q** (Non-salary TDS)")
            if has_tcs:
                guidance_parts.append("**27EQ** (TCS)")
            if guidance_parts:
                st.info("Returns to be filed: " + " | ".join(guidance_parts))

    # ==================================================================
    #  TAB 5: COMPLIANCE CHECKS
    # ==================================================================
    with tab5:
        section_header("TDS Compliance Checks")

        check1, check2, check3 = st.tabs([
            "Threshold Breaches",
            "Missing PAN",
            "Rate Mismatches",
        ])

        # -- Threshold Breaches ----------------------------------------
        with check1:
            st.markdown("##### Parties where payments may exceed TDS thresholds")
            st.markdown("*Checks payments under Indirect Expenses for threshold breaches without TDS deduction.*")

            threshold_data = tds_threshold_check(conn, date_from=date_from, date_to=date_to)

            if not threshold_data:
                st.success("No threshold breach issues detected. All payments appear to be within limits or have TDS deducted.")
            else:
                breaches = [t for t in threshold_data if t["status"] == "BREACH"]
                warnings = [t for t in threshold_data if t["status"] == "WARNING"]

                if breaches:
                    st.error(f"{len(breaches)} potential threshold breach(es) detected!")
                if warnings:
                    st.warning(f"{len(warnings)} payment(s) approaching threshold limits.")

                html = """<table class="slv-table">
                <tr><th>Party</th><th>Section</th><th>Total Payment</th>
                    <th>Threshold</th><th>TDS Deducted?</th><th>Status</th></tr>"""

                for t in threshold_data:
                    html += f"""<tr>
                        <td>{t['party']}</td>
                        <td>{t['applicable_section']}</td>
                        <td>{fi(t['total_payment'])}</td>
                        <td>{fi(t['threshold'])}</td>
                        <td>{'Yes' if t['tds_deducted'] else 'No'}</td>
                        <td>{_status_badge(t['status'])}</td>
                    </tr>"""

                html += "</table>"
                st.markdown(html, unsafe_allow_html=True)

        # -- Missing PAN -----------------------------------------------
        with check2:
            st.markdown("##### PAN Availability Check")
            st.markdown("*Without PAN, TDS must be deducted at 20% (higher rate under Section 206AA).*")

            if not pan_data:
                st.info("No party TDS data available for PAN check.")
            else:
                missing_pan = [p for p in pan_data if not p["has_pan"]]
                with_pan = [p for p in pan_data if p["has_pan"]]

                c1, c2 = st.columns(2)
                with c1:
                    st.metric("Parties with PAN", len(with_pan))
                with c2:
                    st.metric("Parties without PAN", len(missing_pan))

                if missing_pan:
                    st.error(f"{len(missing_pan)} party/parties without PAN -- 20% TDS rate applies!")

                    html = """<table class="slv-table">
                    <tr><th>Party</th><th>TDS Amount</th><th>Status</th></tr>"""
                    for p in missing_pan:
                        html += f"""<tr>
                            <td>{p['party']}</td>
                            <td>{fi(p['tds_amount'])}</td>
                            <td>{_status_badge(p['status'])}</td>
                        </tr>"""
                    html += "</table>"
                    st.markdown(html, unsafe_allow_html=True)
                else:
                    st.success("All parties with TDS deductions have PAN recorded.")

                # Show full list
                with st.expander("View all parties with PAN status"):
                    html = """<table class="slv-table">
                    <tr><th>Party</th><th>PAN</th><th>TDS Amount</th><th>Status</th></tr>"""
                    for p in pan_data:
                        pan_display = p['pan'] if p['has_pan'] else '-'
                        html += f"""<tr>
                            <td>{p['party']}</td>
                            <td>{pan_display}</td>
                            <td>{fi(p['tds_amount'])}</td>
                            <td>{_status_badge(p['status'])}</td>
                        </tr>"""
                    html += "</table>"
                    st.markdown(html, unsafe_allow_html=True)

        # -- Rate Mismatches -------------------------------------------
        with check3:
            st.markdown("##### TDS Rate Verification")
            st.markdown("*Compares effective TDS rate with standard rate for each section.*")

            rate_data = tds_rate_verification(conn, date_from=date_from, date_to=date_to)

            if not rate_data:
                st.info("No rate data available for verification.")
            else:
                mismatches = [r for r in rate_data if r["status"] in ("MISMATCH", "CHECK_PAN_RATE")]
                minor = [r for r in rate_data if r["status"] == "MINOR_DIFF"]

                if mismatches:
                    st.error(f"{len(mismatches)} significant rate mismatch(es) detected!")
                if minor:
                    st.warning(f"{len(minor)} minor rate difference(s) noted.")
                if not mismatches and not minor:
                    st.success("All TDS rates appear to be within expected ranges.")

                html = """<table class="slv-table">
                <tr><th>Party</th><th>PAN</th><th>Section(s)</th>
                    <th>Gross Payment</th><th>TDS Amount</th>
                    <th>Eff. Rate %</th><th>Expected %</th><th>Status</th></tr>"""

                for r in rate_data:
                    pan_display = r['pan'] if r['pan'] else '-'
                    exp_str = f"{r['expected_rate']}%" if r['expected_rate'] is not None else "-"
                    html += f"""<tr>
                        <td>{r['party']}</td>
                        <td>{pan_display}</td>
                        <td>{r['sections']}</td>
                        <td>{fi(r['gross_payment'])}</td>
                        <td>{fi(r['tds_amount'])}</td>
                        <td>{r['effective_rate']}</td>
                        <td>{exp_str}</td>
                        <td>{_status_badge(r['status'])}</td>
                    </tr>"""

                html += "</table>"
                st.markdown(html, unsafe_allow_html=True)

                # Reference table
                with st.expander("TDS Rate Reference"):
                    ref_html = """<table class="slv-table">
                    <tr><th>Section</th><th>Payment Type</th><th>Rate (%)</th><th>Threshold (per year)</th></tr>"""
                    for sec, info in sorted(TDS_SECTIONS.items()):
                        if sec == "Other":
                            continue
                        rate_str = f"{info['rate']}%" if info['rate'] is not None else "Various/Slab"
                        thr_str = fi(info['threshold']) if info['threshold'] else "-"
                        ref_html += f"""<tr>
                            <td><strong>{sec}</strong></td>
                            <td>{info['description']}</td>
                            <td>{rate_str}</td>
                            <td>{thr_str}</td>
                        </tr>"""
                    ref_html += "</table>"
                    st.markdown(ref_html, unsafe_allow_html=True)

# ======================================================================
#  CLOSE CONNECTION
# ======================================================================

conn.close()

# ======================================================================
#  CHAT BAR
# ======================================================================

st.markdown("---")
try:
    from chat_engine import ask, format_result_as_text
    chat_input = st.chat_input("Ask anything -- TDS queries, ledger details, compliance checks...")
    if chat_input:
        chat_conn = get_conn()
        result = ask(chat_input)
        st.markdown(f"**You:** {chat_input}")
        if result.get("type") == "chat":
            st.markdown(result.get("message", ""))
        else:
            st.markdown(format_result_as_text(result))
        chat_conn.close()
except ImportError:
    pass

footer(company_name)
