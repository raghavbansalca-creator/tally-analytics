"""
Seven Labs Vision -- Narration Audit Dashboard
Analyze voucher narrations, classify transactions, flag suspicious entries.
"""

import streamlit as st
import pandas as pd
import sys
import os
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from narration_engine import analyze_all_narrations, export_narration_report

# New multi-layer pipeline (group-context + bank parser + regex)
try:
    from narration_classifier import classify_all as pipeline_classify_all
    PIPELINE_AVAILABLE = True
except ImportError:
    PIPELINE_AVAILABLE = False
from styles import (
    inject_base_styles, page_header, section_header, metric_card,
    badge, footer, fmt, fmt_full,
)

st.set_page_config(page_title="Narration Audit", page_icon="", layout="wide")
inject_base_styles()

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tally_data.db")


# -- Helpers ------------------------------------------------------------------

def fmt_inr(amount):
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


def severity_badge_html(severity):
    s = (severity or "").upper()
    if s == "HIGH":
        return badge("HIGH", "red")
    elif s == "MEDIUM":
        return badge("MED", "amber")
    elif s == "LOW":
        return badge("LOW", "green")
    return badge(severity or "N/A", "gray")


def severity_color(severity):
    s = (severity or "").upper()
    if s == "HIGH":
        return "red"
    elif s == "MEDIUM":
        return "amber"
    return "green"


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

page_header("Narration Audit", f"Auto-classify voucher narrations and flag audit observations | {COMPANY}")


# -- Session state ------------------------------------------------------------

if "narration_results" not in st.session_state:
    st.session_state.narration_results = None


# -- Pipeline info banner -----------------------------------------------------

if PIPELINE_AVAILABLE:
    st.info(
        "**New: Multi-Layer Engine active.** This page uses the original regex-only analysis. "
        "For the enhanced Group vs Narration cross-check (with misclassification detection), "
        "go to **Narration Cross-Check** page in the sidebar."
    )


# -- Run button ---------------------------------------------------------------

col_btn, col_status = st.columns([1, 4])
with col_btn:
    run_clicked = st.button("Run Narration Analysis", type="primary")
with col_status:
    if st.session_state.narration_results is None:
        st.info("Click to analyze all voucher narrations.")

if run_clicked:
    with st.spinner("Analyzing narrations across all vouchers..."):
        st.session_state.narration_results = analyze_all_narrations(DB_PATH)

results = st.session_state.narration_results
if results is None:
    st.stop()


# -- Summary Metrics ----------------------------------------------------------

section_header("OVERVIEW")

risk = results.get("risk_summary", {})
total_flagged = risk.get("high", 0) + risk.get("medium", 0) + risk.get("low", 0)

m1, m2, m3, m4, m5 = st.columns(5)
with m1:
    metric_card("Total Vouchers", f"{results['total_vouchers']:,}", color_class="blue")
with m2:
    metric_card("Flagged", f"{total_flagged:,}",
                sub=f"of {results['narrations_analyzed']:,} analyzed", color_class="amber")
with m3:
    metric_card("HIGH Severity", f"{risk.get('high', 0):,}", color_class="red")
with m4:
    metric_card("MEDIUM Severity", f"{risk.get('medium', 0):,}", color_class="amber")
with m5:
    metric_card("LOW Severity", f"{risk.get('low', 0):,}", color_class="green")

st.markdown("")

# No-narration callout
if results.get("no_narration_count", 0) > 0:
    st.warning(f"**{results['no_narration_count']}** vouchers have NO narration at all. "
               "These need immediate attention -- transaction purpose is unclear.")


# -- Filters ------------------------------------------------------------------

section_header("FLAGGED VOUCHERS")

fc1, fc2, fc3 = st.columns([1, 1, 2])

with fc1:
    severity_filter = st.selectbox("Filter by Severity", ["All", "HIGH", "MEDIUM", "LOW"])

# Build category list from results
all_categories = sorted(results.get("category_summary", {}).keys())
with fc2:
    category_filter = st.selectbox("Filter by Category", ["All"] + all_categories)

with fc3:
    st.markdown("")  # spacer

flagged = results.get("flagged_vouchers", [])

# Apply filters
if severity_filter != "All":
    flagged = [v for v in flagged if v["severity"] == severity_filter]
if category_filter != "All":
    flagged = [v for v in flagged if category_filter in v.get("categories", [])]


# -- Flagged Vouchers Table ---------------------------------------------------

if not flagged:
    st.info("No vouchers match the selected filters.")
else:
    st.markdown(f"Showing **{len(flagged):,}** flagged vouchers")

    # Build display dataframe
    display_rows = []
    for v in flagged:
        display_rows.append({
            "Date": v.get("date", ""),
            "Type": v.get("voucher_type", ""),
            "Number": v.get("voucher_number", ""),
            "Party": v.get("party", ""),
            "Amount": v.get("amount", 0),
            "Narration": v.get("narration", "")[:120],
            "Categories": ", ".join(v.get("categories", [])),
            "Comments": "; ".join(v.get("comments", [])),
            "Severity": v.get("severity", ""),
        })

    df = pd.DataFrame(display_rows)

    # Format amount column
    df["Amount"] = df["Amount"].apply(lambda x: fmt_inr(x))

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        height=min(600, 40 + len(df) * 35),
        column_config={
            "Narration": st.column_config.TextColumn(width="large"),
            "Categories": st.column_config.TextColumn(width="medium"),
            "Comments": st.column_config.TextColumn(width="large"),
        },
    )


# -- Category Breakdown -------------------------------------------------------

section_header("CATEGORY-WISE BREAKDOWN")

cat_summary = results.get("category_summary", {})
if cat_summary:
    # Sort by count descending
    sorted_cats = sorted(cat_summary.items(), key=lambda x: -x[1]["count"])

    for cname, cdata in sorted_cats:
        count = cdata["count"]
        total_amt = cdata["total_amount"]
        vouchers = cdata.get("vouchers", [])

        with st.expander(f"{cname} -- {count} voucher(s) | Total: {fmt_inr(total_amt)}"):
            ec1, ec2 = st.columns(2)
            with ec1:
                st.markdown(f"**Voucher count:** {count}")
            with ec2:
                st.markdown(f"**Total amount:** {fmt_inr(total_amt)}")

            if vouchers:
                cat_rows = []
                for v in vouchers[:200]:
                    cat_rows.append({
                        "Date": v.get("date", ""),
                        "Type": v.get("voucher_type", ""),
                        "Number": v.get("voucher_number", ""),
                        "Party": v.get("party", ""),
                        "Amount": fmt_inr(v.get("amount", 0)),
                        "Narration": v.get("narration", "")[:100],
                    })
                st.dataframe(
                    pd.DataFrame(cat_rows),
                    use_container_width=True,
                    hide_index=True,
                    height=min(400, 40 + len(cat_rows) * 35),
                )
                if len(vouchers) > 200:
                    st.caption(f"Showing 200 of {len(vouchers)} vouchers. Download Excel for full list.")
else:
    st.info("No categories detected.")


# -- Download Excel -----------------------------------------------------------

section_header("EXPORT")

if st.button("Download Excel Report"):
    with st.spinner("Generating Excel report..."):
        tmp_path = os.path.join(tempfile.gettempdir(), "narration_audit_report.xlsx")
        export_narration_report(results, tmp_path)

        with open(tmp_path, "rb") as f:
            excel_bytes = f.read()

        st.download_button(
            label="Save Narration Audit Report (.xlsx)",
            data=excel_bytes,
            file_name="narration_audit_report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        st.success("Report generated. Click above to save.")


# -- Footer -------------------------------------------------------------------

footer(COMPANY)
