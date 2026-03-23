"""
GST Audit & Reconciliation -- SLV
Compare Tally books with GST portal data (GSTR-1, GSTR-2B, GSTR-3B).
Upload portal files, run reconciliation, view mismatches, download report.

Defensive: try/except around each section, safe column access,
safe division, safe list operations, malformed file handling.
"""

import streamlit as st
import sqlite3
import sys
import os
import tempfile
import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    from gst_reconciliation import (
        full_gst_audit, generate_excel_report, detect_file_type,
        get_books_purchases, get_books_sales, DB_PATH,
    )
    GST_ENGINE_AVAILABLE = True
except ImportError as e:
    GST_ENGINE_AVAILABLE = False
    _import_error = str(e)

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
    """Format amount for display. Safe for None/empty/non-numeric."""
    if amount is None:
        return "0.00"
    try:
        return f"{float(amount):,.2f}"
    except (ValueError, TypeError):
        return "0.00"


def _safe_float(val):
    """Convert to float safely."""
    if val is None or val == "":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _severity_badge(severity):
    """Return badge HTML for severity level."""
    colors = {"HIGH": "red", "MEDIUM": "amber", "LOW": "blue"}
    return badge(str(severity or ""), colors.get(str(severity or ""), "gray"))


def _status_badge(status):
    """Return badge HTML for status."""
    status = str(status or "")
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

if not GST_ENGINE_AVAILABLE:
    st.error(f"GST reconciliation engine not available: {_import_error}")
    footer()
    st.stop()

# ══════════════════════════════════════════════════════════════════════════════
#  SIDEBAR -- PERIOD & FILE UPLOADS
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
        if not fy_months:
            from_date, to_date = "20250401", "20260331"
        else:
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

        purchases = purchases if isinstance(purchases, list) else []
        sales = sales if isinstance(sales, list) else []

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            metric_card("Purchase Invoices", str(len(purchases)), "In selected period", "blue")
        with c2:
            metric_card("Sales Invoices", str(len(sales)), "In selected period", "green")
        with c3:
            total_itc = sum(_safe_float(p.get("total_tax", 0)) for p in purchases)
            metric_card("Total ITC (Books)", f"Rs {_fi(total_itc)}", "CGST + SGST + IGST", "purple")
        with c4:
            total_output = sum(_safe_float(s.get("total_tax", 0)) for s in sales)
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
        try:
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
        except Exception as e:
            st.error(f"Reconciliation failed: {e}")
            st.session_state.gst_audit_result = None

result = st.session_state.gst_audit_result

if result is None:
    info_banner("Click 'Run Reconciliation' to compare portal data with Tally books.", "info")
    footer()
    st.stop()

if not isinstance(result, dict):
    st.error("Unexpected result format from GST audit.")
    footer()
    st.stop()

# ══════════════════════════════════════════════════════════════════════════════
#  DISPLAY RESULTS
# ══════════════════════════════════════════════════════════════════════════════

# Show any parsing errors
for err_key in ["gstr2b_error", "gstr1_error", "gstr3b_error", "books_error",
                "itc_error", "output_error", "summary_error"]:
    err_val = result.get(err_key)
    if err_val:
        st.error(f"Error: {err_val}")

# ── TOP METRICS ──────────────────────────────────────────────────────────────

try:
    section_header(f"Audit Summary  |  {result.get('period', '')}")

    m1, m2, m3, m4 = st.columns(4)

    itc = result.get("itc_reconciliation")
    output = result.get("output_reconciliation")
    flags = result.get("risk_flags", [])
    if not isinstance(flags, list):
        flags = []

    with m1:
        if itc and isinstance(itc, dict):
            diff_dict = itc.get("difference", {})
            diff = _safe_float(diff_dict.get("total", 0) if isinstance(diff_dict, dict) else 0)
            color = "green" if abs(diff) <= 1 else "red"
            metric_card("ITC Difference", f"Rs {_fi(diff)}", "Portal vs Books", color)
        else:
            metric_card("ITC Difference", "N/A", "No GSTR-2B uploaded", "gray")

    with m2:
        if output and isinstance(output, dict):
            diff_dict = output.get("difference", {})
            diff = _safe_float(diff_dict.get("total", 0) if isinstance(diff_dict, dict) else 0)
            color = "green" if abs(diff) <= 1 else "red"
            metric_card("Output Tax Diff", f"Rs {_fi(diff)}", "GSTR-1 vs Books", color)
        else:
            metric_card("Output Tax Diff", "N/A", "No GSTR-1 uploaded", "gray")

    with m3:
        total_matched = 0
        total_invoices = 0
        if itc and isinstance(itc, dict):
            total_matched += itc.get("matched_count", 0) or 0
            total_invoices += max(itc.get("total_portal_invoices", 0) or 0, itc.get("total_books_invoices", 0) or 0)
        if output and isinstance(output, dict):
            total_matched += output.get("matched_count", 0) or 0
            total_invoices += max(output.get("total_portal_invoices", 0) or 0, output.get("total_books_invoices", 0) or 0)
        # Safe division
        pct = round(total_matched / total_invoices * 100, 1) if total_invoices > 0 else 0
        color = "green" if pct >= 90 else ("amber" if pct >= 70 else "red")
        metric_card("Match Rate", f"{pct}%", f"{total_matched} / {total_invoices} invoices", color)

    with m4:
        high_flags = len([f for f in flags if isinstance(f, dict) and f.get("severity") == "HIGH"])
        color = "red" if high_flags > 0 else ("amber" if flags else "green")
        metric_card("Risk Flags", str(len(flags)), f"{high_flags} high severity", color)
except Exception as e:
    st.warning(f"Could not render summary metrics: {e}")

# ── TABS ─────────────────────────────────────────────────────────────────────

tab_names = []
if itc and isinstance(itc, dict):
    tab_names.append("ITC Reconciliation")
if output and isinstance(output, dict):
    tab_names.append("Output Reconciliation")
if result.get("summary_reconciliation") and isinstance(result.get("summary_reconciliation"), dict):
    tab_names.append("3B Summary")
if result.get("cross_checks") and isinstance(result.get("cross_checks"), list):
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

if itc and isinstance(itc, dict):
    with tabs[tab_idx]:
        tab_idx += 1
        try:
            # Summary row
            portal_total = itc.get("portal_total", {})
            books_total = itc.get("books_total", {})
            difference = itc.get("difference", {})

            sc1, sc2, sc3 = st.columns(3)
            with sc1:
                st.markdown(f"""
                <div class="card">
                    <div class="card-header">Portal (GSTR-2B)</div>
                    <table class="slv-table">
                        <tr><td>CGST</td><td>{_fi(portal_total.get('cgst', 0))}</td></tr>
                        <tr><td>SGST</td><td>{_fi(portal_total.get('sgst', 0))}</td></tr>
                        <tr><td>IGST</td><td>{_fi(portal_total.get('igst', 0))}</td></tr>
                        <tr class="total-row"><td>Total</td><td>{_fi(portal_total.get('total', 0))}</td></tr>
                    </table>
                </div>
                """, unsafe_allow_html=True)
            with sc2:
                st.markdown(f"""
                <div class="card">
                    <div class="card-header">Books (Tally)</div>
                    <table class="slv-table">
                        <tr><td>CGST</td><td>{_fi(books_total.get('cgst', 0))}</td></tr>
                        <tr><td>SGST</td><td>{_fi(books_total.get('sgst', 0))}</td></tr>
                        <tr><td>IGST</td><td>{_fi(books_total.get('igst', 0))}</td></tr>
                        <tr class="total-row"><td>Total</td><td>{_fi(books_total.get('total', 0))}</td></tr>
                    </table>
                </div>
                """, unsafe_allow_html=True)
            with sc3:
                st.markdown(f"""
                <div class="card">
                    <div class="card-header">Difference</div>
                    <table class="slv-table">
                        <tr><td>CGST</td><td>{_fi(difference.get('cgst', 0))}</td></tr>
                        <tr><td>SGST</td><td>{_fi(difference.get('sgst', 0))}</td></tr>
                        <tr><td>IGST</td><td>{_fi(difference.get('igst', 0))}</td></tr>
                        <tr class="total-row"><td>Total</td><td>{_fi(difference.get('total', 0))}</td></tr>
                    </table>
                </div>
                """, unsafe_allow_html=True)

            # Sub-tabs for invoice details
            matched_invs = itc.get("matched_invoices", [])
            only_portal = itc.get("only_in_portal", [])
            only_books = itc.get("only_in_books", [])
            amt_mis = itc.get("amount_mismatches", [])

            itc_tabs = st.tabs([
                f"Matched ({itc.get('matched_count', len(matched_invs))})",
                f"Only in Portal ({len(only_portal)})",
                f"Only in Books ({len(only_books)})",
                f"Amount Mismatches ({len(amt_mis)})",
            ])

            with itc_tabs[0]:
                if matched_invs and isinstance(matched_invs, list):
                    import pandas as pd
                    df = pd.DataFrame(matched_invs)
                    display_cols = [c for c in ["gstin", "invoice_no", "invoice_date", "party",
                                                "portal_taxable", "books_taxable", "portal_tax", "books_tax"]
                                    if c in df.columns]
                    if display_cols:
                        st.dataframe(df[display_cols], use_container_width=True, hide_index=True)
                    else:
                        st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.info("No matched invoices.")

            with itc_tabs[1]:
                if only_portal and isinstance(only_portal, list):
                    import pandas as pd
                    st.dataframe(pd.DataFrame(only_portal), use_container_width=True, hide_index=True)
                else:
                    st.success("No invoices found only in portal. All portal ITC is booked.")

            with itc_tabs[2]:
                if only_books and isinstance(only_books, list):
                    import pandas as pd
                    st.dataframe(pd.DataFrame(only_books), use_container_width=True, hide_index=True)
                else:
                    st.success("No invoices found only in books. All booked ITC is in portal.")

            with itc_tabs[3]:
                if amt_mis and isinstance(amt_mis, list):
                    import pandas as pd
                    st.dataframe(pd.DataFrame(amt_mis), use_container_width=True, hide_index=True)
                else:
                    st.success("No amount mismatches found.")
        except Exception as e:
            st.error(f"Error rendering ITC reconciliation: {e}")

# ── TAB: OUTPUT RECONCILIATION ───────────────────────────────────────────────

if output and isinstance(output, dict):
    with tabs[tab_idx]:
        tab_idx += 1
        try:
            portal_total = output.get("portal_total", {})
            books_total = output.get("books_total", {})
            difference = output.get("difference", {})

            sc1, sc2, sc3 = st.columns(3)
            with sc1:
                st.markdown(f"""
                <div class="card">
                    <div class="card-header">GSTR-1 (Portal)</div>
                    <table class="slv-table">
                        <tr><td>CGST</td><td>{_fi(portal_total.get('cgst', 0))}</td></tr>
                        <tr><td>SGST</td><td>{_fi(portal_total.get('sgst', 0))}</td></tr>
                        <tr><td>IGST</td><td>{_fi(portal_total.get('igst', 0))}</td></tr>
                        <tr class="total-row"><td>Total</td><td>{_fi(portal_total.get('total', 0))}</td></tr>
                    </table>
                </div>
                """, unsafe_allow_html=True)
            with sc2:
                st.markdown(f"""
                <div class="card">
                    <div class="card-header">Books (Tally)</div>
                    <table class="slv-table">
                        <tr><td>CGST</td><td>{_fi(books_total.get('cgst', 0))}</td></tr>
                        <tr><td>SGST</td><td>{_fi(books_total.get('sgst', 0))}</td></tr>
                        <tr><td>IGST</td><td>{_fi(books_total.get('igst', 0))}</td></tr>
                        <tr class="total-row"><td>Total</td><td>{_fi(books_total.get('total', 0))}</td></tr>
                    </table>
                </div>
                """, unsafe_allow_html=True)
            with sc3:
                st.markdown(f"""
                <div class="card">
                    <div class="card-header">Difference</div>
                    <table class="slv-table">
                        <tr><td>CGST</td><td>{_fi(difference.get('cgst', 0))}</td></tr>
                        <tr><td>SGST</td><td>{_fi(difference.get('sgst', 0))}</td></tr>
                        <tr><td>IGST</td><td>{_fi(difference.get('igst', 0))}</td></tr>
                        <tr class="total-row"><td>Total</td><td>{_fi(difference.get('total', 0))}</td></tr>
                    </table>
                </div>
                """, unsafe_allow_html=True)

            matched_invs = output.get("matched_invoices", [])
            only_portal = output.get("only_in_portal", [])
            only_books = output.get("only_in_books", [])
            amt_mis = output.get("amount_mismatches", [])

            out_tabs = st.tabs([
                f"Matched ({output.get('matched_count', len(matched_invs))})",
                f"Only in GSTR-1 ({len(only_portal)})",
                f"Only in Books ({len(only_books)})",
                f"Amount Mismatches ({len(amt_mis)})",
            ])

            with out_tabs[0]:
                if matched_invs and isinstance(matched_invs, list):
                    import pandas as pd
                    df = pd.DataFrame(matched_invs)
                    display_cols = [c for c in ["gstin", "invoice_no", "invoice_date", "party",
                                                "portal_taxable", "books_taxable", "portal_tax", "books_tax"]
                                    if c in df.columns]
                    if display_cols:
                        st.dataframe(df[display_cols], use_container_width=True, hide_index=True)
                    else:
                        st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.info("No matched invoices.")

            with out_tabs[1]:
                if only_portal and isinstance(only_portal, list):
                    import pandas as pd
                    st.dataframe(pd.DataFrame(only_portal), use_container_width=True, hide_index=True)
                else:
                    st.success("No invoices found only in GSTR-1.")

            with out_tabs[2]:
                if only_books and isinstance(only_books, list):
                    import pandas as pd
                    st.dataframe(pd.DataFrame(only_books), use_container_width=True, hide_index=True)
                else:
                    st.success("All sales invoices are filed in GSTR-1.")

            with out_tabs[3]:
                if amt_mis and isinstance(amt_mis, list):
                    import pandas as pd
                    st.dataframe(pd.DataFrame(amt_mis), use_container_width=True, hide_index=True)
                else:
                    st.success("No amount mismatches found.")
        except Exception as e:
            st.error(f"Error rendering output reconciliation: {e}")

# ── TAB: 3B SUMMARY RECONCILIATION ──────────────────────────────────────────

summary_recon = result.get("summary_reconciliation")
if summary_recon and isinstance(summary_recon, dict):
    with tabs[tab_idx]:
        tab_idx += 1
        try:
            sm1, sm2 = st.columns(2)
            with sm1:
                passed = summary_recon.get("passed", 0) or 0
                total_checks = summary_recon.get("total_checks", 0) or 0
                failed = summary_recon.get("failed", 0) or 0
                metric_card(
                    "Checks Passed",
                    f"{passed} / {total_checks}",
                    "", "green" if failed == 0 else "amber",
                )
            with sm2:
                metric_card(
                    "Mismatches Found",
                    str(failed),
                    "", "red" if failed > 0 else "green",
                )

            checks = summary_recon.get("checks", [])
            if checks and isinstance(checks, list):
                rows_html = ""
                for check in checks:
                    if not isinstance(check, dict):
                        continue
                    status_html = _status_badge(check.get("status", ""))
                    rows_html += f"""
                    <tr>
                        <td style="text-align:left">{check.get('section', '')}</td>
                        <td>{check.get('component', '')}</td>
                        <td>{_fi(check.get('portal_value', 0))}</td>
                        <td>{_fi(check.get('books_value', 0))}</td>
                        <td>{_fi(check.get('difference', 0))}</td>
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
        except Exception as e:
            st.error(f"Error rendering 3B summary: {e}")

# ── TAB: CROSS-CHECKS ───────────────────────────────────────────────────────

cross_checks = result.get("cross_checks")
if cross_checks and isinstance(cross_checks, list):
    with tabs[tab_idx]:
        tab_idx += 1
        try:
            rows_html = ""
            for check in cross_checks:
                if not isinstance(check, dict):
                    continue
                status_html = _status_badge(check.get("status", ""))
                val1 = check.get("gstr1_value", check.get("gstr3b_value", check.get("books_value", 0)))
                val2 = check.get("gstr3b_value", check.get("gstr2b_value", check.get("portal_value", 0)))
                rows_html += f"""
                <tr>
                    <td style="text-align:left">{check.get('check', '')}</td>
                    <td>{_fi(val1)}</td>
                    <td>{_fi(val2)}</td>
                    <td>{_fi(check.get('difference', 0))}</td>
                    <td>{status_html}</td>
                    <td style="text-align:left">{check.get('remark', '')}</td>
                </tr>"""

            if rows_html:
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
        except Exception as e:
            st.error(f"Error rendering cross-checks: {e}")

# ── TAB: RISK FLAGS ──────────────────────────────────────────────────────────

if flags:
    with tabs[tab_idx]:
        tab_idx += 1
        try:
            high = [f for f in flags if isinstance(f, dict) and f.get("severity") == "HIGH"]
            medium = [f for f in flags if isinstance(f, dict) and f.get("severity") == "MEDIUM"]
            low = [f for f in flags if isinstance(f, dict) and f.get("severity") == "LOW"]

            if high:
                section_header("High Severity")
                for f in high:
                    st.markdown(f"""
                    <div class="info-banner info-banner--error">
                        {_severity_badge('HIGH')} &nbsp;
                        <strong>[{f.get('category', '')}]</strong> {f.get('description', '')}
                    </div>
                    """, unsafe_allow_html=True)

            if medium:
                section_header("Medium Severity")
                for f in medium:
                    st.markdown(f"""
                    <div class="info-banner info-banner--warning">
                        {_severity_badge('MEDIUM')} &nbsp;
                        <strong>[{f.get('category', '')}]</strong> {f.get('description', '')}
                    </div>
                    """, unsafe_allow_html=True)

            if low:
                section_header("Low Severity")
                for f in low:
                    st.markdown(f"""
                    <div class="info-banner info-banner--info">
                        {_severity_badge('LOW')} &nbsp;
                        <strong>[{f.get('category', '')}]</strong> {f.get('description', '')}
                    </div>
                    """, unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Error rendering risk flags: {e}")

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

    period_str = result.get('period', '') or ''
    st.download_button(
        label="Download Excel Report",
        data=excel_data,
        file_name=f"GST_Audit_{period_str.replace(' ', '_')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
except ImportError:
    st.warning("Install pandas and openpyxl to enable Excel export: pip install pandas openpyxl")
except Exception as e:
    st.error(f"Could not generate Excel report: {e}")

footer()
