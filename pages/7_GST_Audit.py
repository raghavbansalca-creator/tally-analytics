"""
GST Audit & Reconciliation — SLV
Compare Tally books with GST portal data (GSTR-1, GSTR-2B, GSTR-3B).
Upload portal files, run reconciliation, view mismatches, download report.
"""

import streamlit as st
import sqlite3
import sys
import os
import tempfile
import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from gst_reconciliation import (
    full_gst_audit, generate_excel_report, detect_file_type,
    get_books_purchases, get_books_sales, DB_PATH,
)

st.set_page_config(page_title="GST Audit -- SLV", layout="wide")

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from styles import (
    inject_base_styles, page_header, section_header, metric_card,
    fmt, fmt_full, badge, footer, info_banner,
)

inject_base_styles()

# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _fi(amount):
    """Format amount for display."""
    if amount is None:
        return "0.00"
    return f"{amount:,.2f}"


def _severity_badge(severity):
    """Return badge HTML for severity level."""
    colors = {"HIGH": "red", "MEDIUM": "amber", "LOW": "blue"}
    return badge(severity, colors.get(severity, "gray"))


def _status_badge(status):
    """Return badge HTML for status."""
    if status == "Match":
        return badge("MATCH", "green")
    elif "MISMATCH" in status:
        return badge("MISMATCH", "red")
    elif "EXCESS" in status:
        return badge("EXCESS CLAIM", "red")
    elif "UNDER" in status:
        return badge("UNDER CLAIM", "amber")
    return badge(status, "gray")


def _get_fy_months():
    """Return list of (YYYYMM, label) for current and previous FY."""
    months = []
    month_names = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
        7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
    }
    # Current date: use 2026 as per project
    for year in [2025, 2026]:
        start_m = 4 if year == 2025 else 1
        end_m = 12 if year == 2025 else 3
        for m in range(start_m, end_m + 1):
            ym = f"{year}{m:02d}"
            label = f"{month_names[m]} {year}"
            months.append((ym, label))
    return months


def _month_date_range(ym):
    """Convert YYYYMM to (from_date, to_date) in YYYYMMDD."""
    import calendar
    year = int(ym[:4])
    month = int(ym[4:6])
    last_day = calendar.monthrange(year, month)[1]
    return f"{year}{month:02d}01", f"{year}{month:02d}{last_day:02d}"


def _fy_date_range(fy_start_year):
    """Get full FY date range. FY 2025-26 -> 20250401 to 20260331."""
    return f"{fy_start_year}0401", f"{fy_start_year + 1}0331"


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE HEADER
# ══════════════════════════════════════════════════════════════════════════════

page_header("GST Audit & Reconciliation", "Compare Tally books with GST portal data")

# ══════════════════════════════════════════════════════════════════════════════
#  SIDEBAR — PERIOD & FILE UPLOADS
# ══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("### GST Audit Settings")
    st.markdown("---")

    # Period selector
    period_type = st.radio(
        "Period",
        ["Monthly", "Quarterly", "Full Year"],
        horizontal=True,
        key="gst_audit_period_type",
    )

    if period_type == "Monthly":
        fy_months = _get_fy_months()
        month_labels = [label for _, label in fy_months]
        month_values = [ym for ym, _ in fy_months]
        selected_idx = st.selectbox(
            "Select Month",
            range(len(fy_months)),
            format_func=lambda i: month_labels[i],
            index=min(len(fy_months) - 1, 9),
            key="gst_audit_month",
        )
        from_date, to_date = _month_date_range(month_values[selected_idx])

    elif period_type == "Quarterly":
        quarters = [
            ("Q1 (Apr-Jun 2025)", "20250401", "20250630"),
            ("Q2 (Jul-Sep 2025)", "20250701", "20250930"),
            ("Q3 (Oct-Dec 2025)", "20251001", "20251231"),
            ("Q4 (Jan-Mar 2026)", "20260101", "20260331"),
        ]
        q_idx = st.selectbox(
            "Select Quarter",
            range(len(quarters)),
            format_func=lambda i: quarters[i][0],
            key="gst_audit_quarter",
        )
        from_date, to_date = quarters[q_idx][1], quarters[q_idx][2]

    else:
        from_date, to_date = _fy_date_range(2025)

    st.markdown("---")
    st.markdown("### Upload Portal Files")
    st.caption("Upload JSON, Excel (.xlsx), or PDF files from the GST portal")

    gstr2b_file = st.file_uploader(
        "GSTR-2B (ITC Data)",
        type=["json", "xlsx", "xls", "csv", "pdf"],
        key="gstr2b_upload",
    )
    gstr1_file = st.file_uploader(
        "GSTR-1 (Output Data)",
        type=["json", "xlsx", "xls", "csv", "pdf"],
        key="gstr1_upload",
    )
    gstr3b_file = st.file_uploader(
        "GSTR-3B (Summary Return)",
        type=["json", "xlsx", "xls", "csv", "pdf"],
        key="gstr3b_upload",
    )

# ══════════════════════════════════════════════════════════════════════════════
#  RUN RECONCILIATION
# ══════════════════════════════════════════════════════════════════════════════

has_any_upload = gstr2b_file or gstr1_file or gstr3b_file

if not has_any_upload:
    info_banner(
        "Upload at least one GST portal file (GSTR-2B, GSTR-1, or GSTR-3B) from the sidebar "
        "to start reconciliation. You can upload JSON, Excel, or PDF files downloaded from the GST portal.",
        "info",
    )

    # Show books data summary even without portal files
    section_header("Books Data Summary")
    try:
        purchases = get_books_purchases(DB_PATH, from_date, to_date)
        sales = get_books_sales(DB_PATH, from_date, to_date)

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            metric_card("Purchase Invoices", str(len(purchases)), "In selected period", "blue")
        with c2:
            metric_card("Sales Invoices", str(len(sales)), "In selected period", "green")
        with c3:
            total_itc = sum(p["total_tax"] for p in purchases)
            metric_card("Total ITC (Books)", f"Rs {_fi(total_itc)}", "CGST + SGST + IGST", "purple")
        with c4:
            total_output = sum(s["total_tax"] for s in sales)
            metric_card("Total Output Tax", f"Rs {_fi(total_output)}", "CGST + SGST + IGST", "amber")
    except Exception as e:
        st.warning(f"Could not load books data: {e}")

    footer()
    st.stop()

# Run button
run_clicked = st.button("Run Reconciliation", type="primary", use_container_width=True)

if "gst_audit_result" not in st.session_state:
    st.session_state.gst_audit_result = None

if run_clicked:
    with st.spinner("Running GST Audit Reconciliation..."):
        # Detect file types
        gstr2b_type = detect_file_type(gstr2b_file.name) if gstr2b_file else "json"
        gstr1_type = detect_file_type(gstr1_file.name) if gstr1_file else "json"
        gstr3b_type = detect_file_type(gstr3b_file.name) if gstr3b_file else "json"

        # Read file contents
        gstr2b_data = gstr2b_file.read() if gstr2b_file else None
        gstr1_data = gstr1_file.read() if gstr1_file else None
        gstr3b_data = gstr3b_file.read() if gstr3b_file else None

        # Reset file pointers
        if gstr2b_file:
            gstr2b_file.seek(0)
        if gstr1_file:
            gstr1_file.seek(0)
        if gstr3b_file:
            gstr3b_file.seek(0)

        result = full_gst_audit(
            db_path=DB_PATH,
            gstr2b_path=gstr2b_data,
            gstr1_path=gstr1_data,
            gstr3b_path=gstr3b_data,
            from_date=from_date,
            to_date=to_date,
            gstr2b_type=gstr2b_type,
            gstr1_type=gstr1_type,
            gstr3b_type=gstr3b_type,
        )
        st.session_state.gst_audit_result = result

result = st.session_state.gst_audit_result

if result is None:
    info_banner("Click 'Run Reconciliation' to compare portal data with Tally books.", "info")
    footer()
    st.stop()

# ══════════════════════════════════════════════════════════════════════════════
#  DISPLAY RESULTS
# ══════════════════════════════════════════════════════════════════════════════

# Show any parsing errors
for err_key in ["gstr2b_error", "gstr1_error", "gstr3b_error", "books_error",
                "itc_error", "output_error", "summary_error"]:
    if err_key in result and result[err_key]:
        st.error(f"Error: {result[err_key]}")

# ── TOP METRICS ──────────────────────────────────────────────────────────────

section_header(f"Audit Summary  |  {result.get('period', '')}")

m1, m2, m3, m4 = st.columns(4)

itc = result.get("itc_reconciliation")
output = result.get("output_reconciliation")
flags = result.get("risk_flags", [])

with m1:
    if itc:
        diff = itc["difference"]["total"]
        color = "green" if abs(diff) <= 1 else "red"
        metric_card("ITC Difference", f"Rs {_fi(diff)}", "Portal vs Books", color)
    else:
        metric_card("ITC Difference", "N/A", "No GSTR-2B uploaded", "gray")

with m2:
    if output:
        diff = output["difference"]["total"]
        color = "green" if abs(diff) <= 1 else "red"
        metric_card("Output Tax Diff", f"Rs {_fi(diff)}", "GSTR-1 vs Books", color)
    else:
        metric_card("Output Tax Diff", "N/A", "No GSTR-1 uploaded", "gray")

with m3:
    total_matched = 0
    total_invoices = 0
    if itc:
        total_matched += itc["matched_count"]
        total_invoices += max(itc["total_portal_invoices"], itc["total_books_invoices"])
    if output:
        total_matched += output["matched_count"]
        total_invoices += max(output["total_portal_invoices"], output["total_books_invoices"])
    pct = round(total_matched / total_invoices * 100, 1) if total_invoices > 0 else 0
    color = "green" if pct >= 90 else ("amber" if pct >= 70 else "red")
    metric_card("Match Rate", f"{pct}%", f"{total_matched} / {total_invoices} invoices", color)

with m4:
    high_flags = len([f for f in flags if f["severity"] == "HIGH"])
    color = "red" if high_flags > 0 else ("amber" if flags else "green")
    metric_card("Risk Flags", str(len(flags)), f"{high_flags} high severity", color)

# ── TABS ─────────────────────────────────────────────────────────────────────

tab_names = []
if itc:
    tab_names.append("ITC Reconciliation")
if output:
    tab_names.append("Output Reconciliation")
if result.get("summary_reconciliation"):
    tab_names.append("3B Summary")
if result.get("cross_checks"):
    tab_names.append("Cross-Checks")
if flags:
    tab_names.append("Risk Flags")

if not tab_names:
    st.warning("No reconciliation data to display. Check uploaded files and period selection.")
    footer()
    st.stop()

tabs = st.tabs(tab_names)
tab_idx = 0

# ── TAB: ITC RECONCILIATION ─────────────────────────────────────────────────

if itc:
    with tabs[tab_idx]:
        tab_idx += 1

        # Summary row
        sc1, sc2, sc3 = st.columns(3)
        with sc1:
            st.markdown(f"""
            <div class="card">
                <div class="card-header">Portal (GSTR-2B)</div>
                <table class="slv-table">
                    <tr><td>CGST</td><td>{_fi(itc['portal_total']['cgst'])}</td></tr>
                    <tr><td>SGST</td><td>{_fi(itc['portal_total']['sgst'])}</td></tr>
                    <tr><td>IGST</td><td>{_fi(itc['portal_total']['igst'])}</td></tr>
                    <tr class="total-row"><td>Total</td><td>{_fi(itc['portal_total']['total'])}</td></tr>
                </table>
            </div>
            """, unsafe_allow_html=True)
        with sc2:
            st.markdown(f"""
            <div class="card">
                <div class="card-header">Books (Tally)</div>
                <table class="slv-table">
                    <tr><td>CGST</td><td>{_fi(itc['books_total']['cgst'])}</td></tr>
                    <tr><td>SGST</td><td>{_fi(itc['books_total']['sgst'])}</td></tr>
                    <tr><td>IGST</td><td>{_fi(itc['books_total']['igst'])}</td></tr>
                    <tr class="total-row"><td>Total</td><td>{_fi(itc['books_total']['total'])}</td></tr>
                </table>
            </div>
            """, unsafe_allow_html=True)
        with sc3:
            st.markdown(f"""
            <div class="card">
                <div class="card-header">Difference</div>
                <table class="slv-table">
                    <tr><td>CGST</td><td>{_fi(itc['difference']['cgst'])}</td></tr>
                    <tr><td>SGST</td><td>{_fi(itc['difference']['sgst'])}</td></tr>
                    <tr><td>IGST</td><td>{_fi(itc['difference']['igst'])}</td></tr>
                    <tr class="total-row"><td>Total</td><td>{_fi(itc['difference']['total'])}</td></tr>
                </table>
            </div>
            """, unsafe_allow_html=True)

        # Sub-tabs for invoice details
        itc_tabs = st.tabs([
            f"Matched ({itc['matched_count']})",
            f"Only in Portal ({len(itc['only_in_portal'])})",
            f"Only in Books ({len(itc['only_in_books'])})",
            f"Amount Mismatches ({len(itc['amount_mismatches'])})",
        ])

        with itc_tabs[0]:
            if itc["matched_invoices"]:
                import pandas as pd
                df = pd.DataFrame(itc["matched_invoices"])
                display_cols = [c for c in ["gstin", "invoice_no", "invoice_date", "party",
                                            "portal_taxable", "books_taxable", "portal_tax", "books_tax"]
                                if c in df.columns]
                st.dataframe(df[display_cols], use_container_width=True, hide_index=True)
            else:
                st.info("No matched invoices.")

        with itc_tabs[1]:
            if itc["only_in_portal"]:
                import pandas as pd
                df = pd.DataFrame(itc["only_in_portal"])
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.success("No invoices found only in portal. All portal ITC is booked.")

        with itc_tabs[2]:
            if itc["only_in_books"]:
                import pandas as pd
                df = pd.DataFrame(itc["only_in_books"])
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.success("No invoices found only in books. All booked ITC is in portal.")

        with itc_tabs[3]:
            if itc["amount_mismatches"]:
                import pandas as pd
                df = pd.DataFrame(itc["amount_mismatches"])
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.success("No amount mismatches found.")

# ── TAB: OUTPUT RECONCILIATION ───────────────────────────────────────────────

if output:
    with tabs[tab_idx]:
        tab_idx += 1

        sc1, sc2, sc3 = st.columns(3)
        with sc1:
            st.markdown(f"""
            <div class="card">
                <div class="card-header">GSTR-1 (Portal)</div>
                <table class="slv-table">
                    <tr><td>CGST</td><td>{_fi(output['portal_total']['cgst'])}</td></tr>
                    <tr><td>SGST</td><td>{_fi(output['portal_total']['sgst'])}</td></tr>
                    <tr><td>IGST</td><td>{_fi(output['portal_total']['igst'])}</td></tr>
                    <tr class="total-row"><td>Total</td><td>{_fi(output['portal_total']['total'])}</td></tr>
                </table>
            </div>
            """, unsafe_allow_html=True)
        with sc2:
            st.markdown(f"""
            <div class="card">
                <div class="card-header">Books (Tally)</div>
                <table class="slv-table">
                    <tr><td>CGST</td><td>{_fi(output['books_total']['cgst'])}</td></tr>
                    <tr><td>SGST</td><td>{_fi(output['books_total']['sgst'])}</td></tr>
                    <tr><td>IGST</td><td>{_fi(output['books_total']['igst'])}</td></tr>
                    <tr class="total-row"><td>Total</td><td>{_fi(output['books_total']['total'])}</td></tr>
                </table>
            </div>
            """, unsafe_allow_html=True)
        with sc3:
            st.markdown(f"""
            <div class="card">
                <div class="card-header">Difference</div>
                <table class="slv-table">
                    <tr><td>CGST</td><td>{_fi(output['difference']['cgst'])}</td></tr>
                    <tr><td>SGST</td><td>{_fi(output['difference']['sgst'])}</td></tr>
                    <tr><td>IGST</td><td>{_fi(output['difference']['igst'])}</td></tr>
                    <tr class="total-row"><td>Total</td><td>{_fi(output['difference']['total'])}</td></tr>
                </table>
            </div>
            """, unsafe_allow_html=True)

        out_tabs = st.tabs([
            f"Matched ({output['matched_count']})",
            f"Only in GSTR-1 ({len(output['only_in_portal'])})",
            f"Only in Books ({len(output['only_in_books'])})",
            f"Amount Mismatches ({len(output['amount_mismatches'])})",
        ])

        with out_tabs[0]:
            if output["matched_invoices"]:
                import pandas as pd
                df = pd.DataFrame(output["matched_invoices"])
                display_cols = [c for c in ["gstin", "invoice_no", "invoice_date", "party",
                                            "portal_taxable", "books_taxable", "portal_tax", "books_tax"]
                                if c in df.columns]
                st.dataframe(df[display_cols], use_container_width=True, hide_index=True)
            else:
                st.info("No matched invoices.")

        with out_tabs[1]:
            if output["only_in_portal"]:
                import pandas as pd
                df = pd.DataFrame(output["only_in_portal"])
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.success("No invoices found only in GSTR-1.")

        with out_tabs[2]:
            if output["only_in_books"]:
                import pandas as pd
                df = pd.DataFrame(output["only_in_books"])
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.success("All sales invoices are filed in GSTR-1.")

        with out_tabs[3]:
            if output["amount_mismatches"]:
                import pandas as pd
                df = pd.DataFrame(output["amount_mismatches"])
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.success("No amount mismatches found.")

# ── TAB: 3B SUMMARY RECONCILIATION ──────────────────────────────────────────

if result.get("summary_reconciliation"):
    with tabs[tab_idx]:
        tab_idx += 1
        summary_recon = result["summary_reconciliation"]

        sm1, sm2 = st.columns(2)
        with sm1:
            metric_card(
                "Checks Passed",
                f"{summary_recon['passed']} / {summary_recon['total_checks']}",
                "", "green" if summary_recon["failed"] == 0 else "amber",
            )
        with sm2:
            metric_card(
                "Mismatches Found",
                str(summary_recon["failed"]),
                "", "red" if summary_recon["failed"] > 0 else "green",
            )

        if summary_recon.get("checks"):
            # Build HTML table
            rows_html = ""
            for check in summary_recon["checks"]:
                status_html = _status_badge(check["status"])
                rows_html += f"""
                <tr>
                    <td style="text-align:left">{check['section']}</td>
                    <td>{check['component']}</td>
                    <td>{_fi(check['portal_value'])}</td>
                    <td>{_fi(check['books_value'])}</td>
                    <td>{_fi(check['difference'])}</td>
                    <td>{status_html}</td>
                </tr>"""

            st.markdown(f"""
            <table class="slv-table">
                <thead>
                    <tr>
                        <th style="text-align:left">Section</th>
                        <th>Component</th>
                        <th>Portal (3B)</th>
                        <th>Books</th>
                        <th>Difference</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>{rows_html}</tbody>
            </table>
            """, unsafe_allow_html=True)

# ── TAB: CROSS-CHECKS ───────────────────────────────────────────────────────

if result.get("cross_checks"):
    with tabs[tab_idx]:
        tab_idx += 1
        cc = result["cross_checks"]

        if cc:
            rows_html = ""
            for check in cc:
                status_html = _status_badge(check["status"])
                # Get the two comparison values
                val1 = check.get("gstr1_value", check.get("gstr3b_value", check.get("books_value", 0)))
                val2 = check.get("gstr3b_value", check.get("gstr2b_value", check.get("portal_value", 0)))
                rows_html += f"""
                <tr>
                    <td style="text-align:left">{check['check']}</td>
                    <td>{_fi(val1)}</td>
                    <td>{_fi(val2)}</td>
                    <td>{_fi(check.get('difference', 0))}</td>
                    <td>{status_html}</td>
                    <td style="text-align:left">{check.get('remark', '')}</td>
                </tr>"""

            st.markdown(f"""
            <table class="slv-table">
                <thead>
                    <tr>
                        <th style="text-align:left">Cross-Check</th>
                        <th>Value 1</th>
                        <th>Value 2</th>
                        <th>Difference</th>
                        <th>Status</th>
                        <th style="text-align:left">Remark</th>
                    </tr>
                </thead>
                <tbody>{rows_html}</tbody>
            </table>
            """, unsafe_allow_html=True)
        else:
            st.info("No cross-checks available. Upload multiple portal files for cross-verification.")

# ── TAB: RISK FLAGS ──────────────────────────────────────────────────────────

if flags:
    with tabs[tab_idx]:
        tab_idx += 1

        high = [f for f in flags if f["severity"] == "HIGH"]
        medium = [f for f in flags if f["severity"] == "MEDIUM"]
        low = [f for f in flags if f["severity"] == "LOW"]

        if high:
            section_header("High Severity")
            for f in high:
                st.markdown(f"""
                <div class="info-banner info-banner--error">
                    {_severity_badge('HIGH')} &nbsp;
                    <strong>[{f['category']}]</strong> {f['description']}
                </div>
                """, unsafe_allow_html=True)

        if medium:
            section_header("Medium Severity")
            for f in medium:
                st.markdown(f"""
                <div class="info-banner info-banner--warning">
                    {_severity_badge('MEDIUM')} &nbsp;
                    <strong>[{f['category']}]</strong> {f['description']}
                </div>
                """, unsafe_allow_html=True)

        if low:
            section_header("Low Severity")
            for f in low:
                st.markdown(f"""
                <div class="info-banner info-banner--info">
                    {_severity_badge('LOW')} &nbsp;
                    <strong>[{f['category']}]</strong> {f['description']}
                </div>
                """, unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
#  DOWNLOAD EXCEL REPORT
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("---")
section_header("Export Report")

try:
    import pandas as pd

    tmp_path = os.path.join(tempfile.gettempdir(), "gst_audit_report.xlsx")
    generate_excel_report(result, tmp_path)

    with open(tmp_path, "rb") as f:
        excel_data = f.read()

    st.download_button(
        label="Download Excel Report",
        data=excel_data,
        file_name=f"GST_Audit_{result.get('period', '').replace(' ', '_')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
except ImportError:
    st.warning("Install pandas and openpyxl to enable Excel export: pip install pandas openpyxl")
except Exception as e:
    st.error(f"Could not generate Excel report: {e}")

footer()
