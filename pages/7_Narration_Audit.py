"""
Seven Labs Vision -- Narration Audit Dashboard
Analyze voucher narrations, classify transactions, flag suspicious entries.
"""

import streamlit as st
import pandas as pd
import sqlite3
import sys
import os
import datetime
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from narration_engine import analyze_all_narrations, export_narration_report
from narration_trainer import (
    sync_training_table, generate_training_batch, save_review,
    save_batch_reviews, skip_narration, get_training_stats,
    get_count_by_filter, export_training_data, export_training_excel,
    CATEGORIES as TRAINER_CATEGORIES,
)
from sidebar_filters import render_sidebar_filters

st.set_page_config(page_title="Narration Audit -- SLV", page_icon="N", layout="wide")

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tally_data.db")


# ── CSS ──────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
    .na-header {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
        color: white;
        padding: 1.2rem 1.8rem;
        border-radius: 12px;
        margin-bottom: 1rem;
    }
    .na-header h1 { color: white; margin: 0; font-size: 1.6rem; }
    .na-header p { color: #94a3b8; margin: 0.2rem 0 0 0; font-size: 0.9rem; }

    .metric-card {
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 1rem 1.2rem;
        text-align: center;
    }
    .metric-card .label { color: #64748b; font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.03em; }
    .metric-card .value { color: #1e293b; font-size: 1.3rem; font-weight: 700; }
    .metric-card .value.green { color: #16a34a; }
    .metric-card .value.red { color: #dc2626; }
    .metric-card .value.blue { color: #2563eb; }
    .metric-card .value.amber { color: #d97706; }

    .sev-high { color: #dc2626; font-weight: 700; }
    .sev-medium { color: #d97706; font-weight: 700; }
    .sev-low { color: #16a34a; font-weight: 700; }
</style>
""", unsafe_allow_html=True)


# ── HELPERS ──────────────────────────────────────────────────────────────────

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


def metric_card_html(label, value, color_class=""):
    vc = f' {color_class}' if color_class else ''
    return f'''<div class="metric-card">
        <div class="label">{label}</div>
        <div class="value{vc}">{value}</div>
    </div>'''


def severity_text(severity):
    s = (severity or "").upper()
    if s == "HIGH":
        return '<span class="sev-high">HIGH</span>'
    elif s == "MEDIUM":
        return '<span class="sev-medium">MEDIUM</span>'
    return '<span class="sev-low">LOW</span>'


# ── COMPANY NAME ─────────────────────────────────────────────────────────────

try:
    _conn = sqlite3.connect(DB_PATH)
    _row = _conn.execute("SELECT value FROM _metadata WHERE key='company_name'").fetchone()
    COMPANY = _row[0] if _row else "Company"
    _conn.close()
except Exception:
    COMPANY = "Company"


# ── HEADER ───────────────────────────────────────────────────────────────────

st.markdown(
    f'<div class="na-header">'
    f'<h1>Narration Audit</h1>'
    f'<p>Auto-classify voucher narrations and flag audit observations | {COMPANY}</p>'
    f'</div>',
    unsafe_allow_html=True,
)


# ── DATE FILTER (sidebar) ───────────────────────────────────────────────────

_conn = sqlite3.connect(DB_PATH)
_min_date_row = _conn.execute("SELECT MIN(DATE) FROM trn_voucher").fetchone()
_max_date_row = _conn.execute("SELECT MAX(DATE) FROM trn_voucher").fetchone()
_min_dt = (
    datetime.date(int(_min_date_row[0][:4]), int(_min_date_row[0][4:6]), int(_min_date_row[0][6:8]))
    if _min_date_row and _min_date_row[0]
    else datetime.date(2025, 4, 1)
)
_max_dt = (
    datetime.date(int(_max_date_row[0][:4]), int(_max_date_row[0][4:6]), int(_max_date_row[0][6:8]))
    if _max_date_row and _max_date_row[0]
    else datetime.date.today()
)

if "applied_start_date" not in st.session_state:
    st.session_state.applied_start_date = _min_dt
if "applied_end_date" not in st.session_state:
    st.session_state.applied_end_date = _max_dt

st.sidebar.markdown("### Date Range")
_from = st.sidebar.date_input(
    "From", value=st.session_state.applied_start_date,
    min_value=_min_dt, max_value=_max_dt, key="na_filter_from",
)
_to = st.sidebar.date_input(
    "To", value=st.session_state.applied_end_date,
    min_value=_min_dt, max_value=_max_dt, key="na_filter_to",
)
_c1, _c2 = st.sidebar.columns(2)
with _c1:
    if st.button("Apply", key="na_apply_dates", use_container_width=True, type="primary"):
        st.session_state.applied_start_date = _from
        st.session_state.applied_end_date = _to
        st.session_state.narration_results = None  # force re-analysis
        st.rerun()
with _c2:
    if st.button("Reset", key="na_reset_dates", use_container_width=True):
        st.session_state.applied_start_date = _min_dt
        st.session_state.applied_end_date = _max_dt
        st.session_state.narration_results = None
        st.rerun()

DATE_FROM = st.session_state.applied_start_date.strftime("%Y%m%d")
DATE_TO = st.session_state.applied_end_date.strftime("%Y%m%d")
st.sidebar.caption(
    f"Showing: {st.session_state.applied_start_date.strftime('%d %b %Y')} "
    f"-- {st.session_state.applied_end_date.strftime('%d %b %Y')}"
)

# ── DYNAMIC SIDEBAR FILTERS ─────────────────────────────────────────────────
_filters = render_sidebar_filters(_conn, page_key="narration_audit")
_conn.close()


# ── SESSION STATE ────────────────────────────────────────────────────────────

if "narration_results" not in st.session_state:
    st.session_state.narration_results = None


# ── RUN ANALYSIS ─────────────────────────────────────────────────────────────

col_btn, col_status = st.columns([1, 4])
with col_btn:
    run_clicked = st.button("Run Narration Analysis", type="primary")
with col_status:
    if st.session_state.narration_results is None:
        st.info("Click to analyze all voucher narrations in the selected date range.")

if run_clicked:
    with st.spinner("Analyzing narrations across all vouchers..."):
        st.session_state.narration_results = analyze_all_narrations(
            DB_PATH, from_date=DATE_FROM, to_date=DATE_TO
        )

results = st.session_state.narration_results
if results is None:
    st.stop()


# ══════════════════════════════════════════════════════════════════════════════
#  TABS
# ══════════════════════════════════════════════════════════════════════════════

tab_overview, tab_flagged, tab_training, tab_no_narr = st.tabs([
    "Overview", "Flagged Vouchers", "Training Data", "No Narration",
])


# ── TAB 1: OVERVIEW ─────────────────────────────────────────────────────────

with tab_overview:
    risk = results.get("risk_summary", {})
    total_flagged = risk.get("high", 0) + risk.get("medium", 0) + risk.get("low", 0)

    m1, m2, m3, m4, m5 = st.columns(5)
    with m1:
        st.markdown(metric_card_html("Total Vouchers", f"{results['total_vouchers']:,}", "blue"), unsafe_allow_html=True)
    with m2:
        st.markdown(metric_card_html("Flagged", f"{total_flagged:,}", "amber"), unsafe_allow_html=True)
    with m3:
        st.markdown(metric_card_html("No Narration", f"{results.get('no_narration_count', 0):,}", "red"), unsafe_allow_html=True)
    with m4:
        st.markdown(metric_card_html("HIGH Risk", f"{risk.get('high', 0):,}", "red"), unsafe_allow_html=True)
    with m5:
        st.markdown(metric_card_html("MEDIUM Risk", f"{risk.get('medium', 0):,}", "amber"), unsafe_allow_html=True)

    st.markdown("")

    # No-narration callout
    if results.get("no_narration_count", 0) > 0:
        st.warning(
            f"**{results['no_narration_count']}** vouchers have NO narration at all. "
            "These need immediate attention -- transaction purpose is unclear."
        )

    # ── Category Summary Table ───────────────────────────────────────────
    st.subheader("Category Summary")
    cat_summary = results.get("category_summary", {})
    if cat_summary:
        # Determine severity per category from the engine definitions
        from narration_engine import CATEGORIES as ENGINE_CATS
        cat_severity_map = {}
        for ec in ENGINE_CATS:
            cat_severity_map[ec["name"]] = ec["severity"]
        # Meta categories
        cat_severity_map["No Narration"] = "HIGH"
        cat_severity_map["Unusually Short Narration"] = "MEDIUM"
        cat_severity_map["Unusually Long Narration"] = "LOW"

        sorted_cats = sorted(cat_summary.items(), key=lambda x: -x[1]["count"])
        cat_rows = []
        for cname, cdata in sorted_cats:
            cat_rows.append({
                "Category": cname,
                "Count": cdata["count"],
                "Total Amount": fmt_inr(cdata["total_amount"]),
                "Severity": cat_severity_map.get(cname, "LOW"),
            })

        df_cat = pd.DataFrame(cat_rows)
        st.dataframe(df_cat, use_container_width=True, hide_index=True, height=min(500, 40 + len(df_cat) * 35))

        # ── Bar Chart ────────────────────────────────────────────────────
        st.subheader("Categories by Count")
        chart_df = pd.DataFrame({
            "Category": [c[0] for c in sorted_cats],
            "Count": [c[1]["count"] for c in sorted_cats],
        }).set_index("Category")
        st.bar_chart(chart_df)
    else:
        st.info("No categories detected.")

    # ── Category Drill-down ──────────────────────────────────────────────
    st.subheader("Category Details")
    if cat_summary:
        for cname, cdata in sorted(cat_summary.items(), key=lambda x: -x[1]["count"]):
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
                    vrows = []
                    for v in vouchers[:200]:
                        vrows.append({
                            "Date": v.get("date", ""),
                            "Type": v.get("voucher_type", ""),
                            "Number": v.get("voucher_number", ""),
                            "Party": v.get("party", ""),
                            "Amount": fmt_inr(v.get("amount", 0)),
                            "Narration": v.get("narration", "")[:100],
                        })
                    st.dataframe(
                        pd.DataFrame(vrows),
                        use_container_width=True,
                        hide_index=True,
                        height=min(400, 40 + len(vrows) * 35),
                    )
                    if len(vouchers) > 200:
                        st.caption(f"Showing 200 of {len(vouchers)} vouchers. Download Excel for full list.")


# ── TAB 2: FLAGGED VOUCHERS ─────────────────────────────────────────────────

with tab_flagged:
    st.subheader("Flagged Vouchers")

    fc1, fc2 = st.columns([1, 1])
    with fc1:
        severity_filter = st.selectbox("Filter by Severity", ["All", "HIGH", "MEDIUM", "LOW"], key="na_sev_filter")
    all_categories = sorted(results.get("category_summary", {}).keys())
    with fc2:
        category_filter = st.selectbox("Filter by Category", ["All"] + all_categories, key="na_cat_filter")

    flagged = results.get("flagged_vouchers", [])

    # Apply filters
    if severity_filter != "All":
        flagged = [v for v in flagged if v["severity"] == severity_filter]
    if category_filter != "All":
        flagged = [v for v in flagged if category_filter in v.get("categories", [])]

    if not flagged:
        st.info("No vouchers match the selected filters.")
    else:
        st.markdown(f"Showing **{len(flagged):,}** flagged vouchers")

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
                "Severity": v.get("severity", ""),
            })

        df = pd.DataFrame(display_rows)
        df["Amount"] = df["Amount"].apply(lambda x: fmt_inr(x))

        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            height=min(600, 40 + len(df) * 35),
            column_config={
                "Narration": st.column_config.TextColumn(width="large"),
                "Categories": st.column_config.TextColumn(width="medium"),
            },
        )

        # ── Expandable audit comments ────────────────────────────────────
        st.subheader("Audit Comments Detail")
        # Show top 50 for performance
        for i, v in enumerate(flagged[:50]):
            sev = v.get("severity", "LOW")
            sev_label = {"HIGH": "HIGH", "MEDIUM": "MED", "LOW": "LOW"}.get(sev, sev)
            sev_color = {"HIGH": "red", "MEDIUM": "orange", "LOW": "green"}.get(sev, "gray")
            with st.expander(
                f":{sev_color}[{sev_label}] {v.get('date', '')} | "
                f"{v.get('voucher_type', '')} {v.get('voucher_number', '')} | "
                f"{v.get('party', '')[:30]} | {fmt_inr(v.get('amount', 0))}"
            ):
                st.markdown(f"**Narration:** {v.get('narration', '')}")
                st.markdown(f"**Categories:** {', '.join(v.get('categories', []))}")
                for comment in v.get("comments", []):
                    st.markdown(f"- {comment}")

        if len(flagged) > 50:
            st.caption(f"Showing detail for top 50 of {len(flagged)} flagged vouchers.")

    # ── Export ────────────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Export")
    if st.button("Generate Excel Report", key="na_export"):
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


# ── TAB 3: TRAINING DATA ────────────────────────────────────────────────────

with tab_training:
    st.subheader("Training Data Pipeline")
    st.caption("Sync narrations, review regex classifications, export for LLM fine-tuning.")

    # ── Sync button ──────────────────────────────────────────────────────
    tc1, tc2 = st.columns([1, 3])
    with tc1:
        if st.button("Sync Training Table", type="primary", key="na_sync"):
            with st.spinner("Syncing narrations to training table..."):
                inserted = sync_training_table(DB_PATH)
            st.success(f"Synced! {inserted} new narrations added.")
            st.rerun()

    # ── Stats ────────────────────────────────────────────────────────────
    stats = get_training_stats(DB_PATH)
    sc1, sc2, sc3, sc4, sc5 = st.columns(5)
    with sc1:
        st.metric("Total", stats["total"])
    with sc2:
        st.metric("Reviewed", stats["reviewed"])
    with sc3:
        st.metric("Verified", stats["verified"])
    with sc4:
        st.metric("Corrected", stats["corrected"])
    with sc5:
        st.metric("Accuracy", f"{stats['accuracy']}%")

    if stats["total"] == 0:
        st.info("No training data yet. Click 'Sync Training Table' to populate from vouchers.")
        st.stop()

    st.markdown("---")

    # ── Batch Review Interface ───────────────────────────────────────────
    st.subheader("Batch Review")

    rc1, rc2, rc3 = st.columns(3)
    with rc1:
        review_status = st.selectbox(
            "Status filter", ["unreviewed", "all", "verified", "corrected", "skipped"],
            key="na_review_status",
        )
    with rc2:
        review_category = st.selectbox(
            "Category filter", ["All"] + TRAINER_CATEGORIES, key="na_review_cat",
        )
    with rc3:
        batch_size = st.number_input("Batch size", min_value=5, max_value=100, value=20, key="na_batch_size")

    filter_cat = review_category if review_category != "All" else None
    total_matching = get_count_by_filter(DB_PATH, review_status, filter_cat)
    st.caption(f"{total_matching} narrations matching filters")

    if "na_batch_offset" not in st.session_state:
        st.session_state.na_batch_offset = 0

    batch = generate_training_batch(
        DB_PATH,
        batch_size=batch_size,
        offset=st.session_state.na_batch_offset,
        filter_status=review_status,
        filter_category=filter_cat,
    )

    if not batch:
        st.info("No narrations match the current filters.")
    else:
        # Navigation
        nav1, nav2, nav3 = st.columns([1, 2, 1])
        with nav1:
            if st.button("Previous", key="na_prev", disabled=st.session_state.na_batch_offset == 0):
                st.session_state.na_batch_offset = max(0, st.session_state.na_batch_offset - batch_size)
                st.rerun()
        with nav2:
            page_num = st.session_state.na_batch_offset // batch_size + 1
            total_pages = max(1, (total_matching + batch_size - 1) // batch_size)
            st.markdown(f"Page **{page_num}** of **{total_pages}**")
        with nav3:
            if st.button("Next", key="na_next", disabled=len(batch) < batch_size):
                st.session_state.na_batch_offset += batch_size
                st.rerun()

        # Display batch for review
        reviews_to_save = []
        for idx, item in enumerate(batch):
            with st.expander(
                f"{item['narration'][:80]}... | {item['voucher_type']} | "
                f"{fmt_inr(item['amount'])} | [{item['status']}]",
                expanded=False,
            ):
                st.markdown(f"**Narration:** {item['narration']}")
                st.markdown(f"**Party:** {item['party']} | **Type:** {item['voucher_type']} | **Amount:** {fmt_inr(item['amount'])}")
                st.markdown(f"**Regex Category:** {item['regex_category']} (confidence: {item['regex_confidence']:.0%})")

                ic1, ic2 = st.columns(2)
                with ic1:
                    current_cat = item["human_category"] if item["human_category"] else item["regex_category"]
                    try:
                        cat_idx = TRAINER_CATEGORIES.index(current_cat)
                    except ValueError:
                        cat_idx = len(TRAINER_CATEGORIES) - 1  # Uncategorized
                    human_cat = st.selectbox(
                        "Category", TRAINER_CATEGORIES, index=cat_idx,
                        key=f"na_cat_{item['guid']}",
                    )
                with ic2:
                    notes = st.text_input("Notes", value=item["notes"], key=f"na_notes_{item['guid']}")

                bc1, bc2 = st.columns(2)
                with bc1:
                    if st.button("Save", key=f"na_save_{item['guid']}"):
                        save_review(DB_PATH, item["guid"], human_cat, notes)
                        st.success("Saved!")
                        st.rerun()
                with bc2:
                    if st.button("Skip", key=f"na_skip_{item['guid']}"):
                        skip_narration(DB_PATH, item["guid"])
                        st.rerun()

    # ── Export buttons ───────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Export Training Data")

    ex1, ex2 = st.columns(2)
    with ex1:
        if st.button("Export JSONL (fine-tuning)", key="na_export_jsonl"):
            tmp_path = os.path.join(tempfile.gettempdir(), "narration_training.jsonl")
            count = export_training_data(DB_PATH, tmp_path)
            if count > 0:
                with open(tmp_path, "rb") as f:
                    st.download_button(
                        "Download JSONL", data=f.read(),
                        file_name="narration_training.jsonl",
                        mime="application/jsonl",
                    )
                st.success(f"Exported {count} examples.")
            else:
                st.warning("No reviewed narrations to export. Review some first.")

    with ex2:
        if st.button("Export Excel (bulk review)", key="na_export_xlsx"):
            tmp_path = os.path.join(tempfile.gettempdir(), "narration_training.xlsx")
            count = export_training_excel(DB_PATH, tmp_path)
            if count > 0:
                with open(tmp_path, "rb") as f:
                    st.download_button(
                        "Download Excel", data=f.read(),
                        file_name="narration_training.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
                st.success(f"Exported {count} rows.")
            else:
                st.warning("No training data to export. Sync first.")


# ── TAB 4: NO NARRATION ─────────────────────────────────────────────────────

with tab_no_narr:
    st.subheader("Vouchers with No Narration")

    # Query vouchers with empty/null narration
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    no_narr_sql = """
        SELECT
            v.DATE,
            v.VOUCHERTYPENAME,
            v.VOUCHERNUMBER,
            v.PARTYLEDGERNAME,
            COALESCE(
                (SELECT SUM(ABS(CAST(a.AMOUNT AS REAL)))
                 FROM trn_accounting a
                 WHERE a.VOUCHER_GUID = v.GUID AND CAST(a.AMOUNT AS REAL) > 0),
                0
            ) as amount
        FROM trn_voucher v
        WHERE (v.NARRATION IS NULL OR v.NARRATION = '' OR TRIM(v.NARRATION) = '')
          AND v.DATE >= ? AND v.DATE <= ?
        ORDER BY v.DATE
    """
    no_narr_rows = conn.execute(no_narr_sql, (DATE_FROM, DATE_TO)).fetchall()
    conn.close()

    if not no_narr_rows:
        st.success("All vouchers in the selected date range have narrations.")
    else:
        st.warning(f"**{len(no_narr_rows)}** vouchers have no narration.")

        # Group by voucher type
        type_groups = {}
        for row in no_narr_rows:
            vtype = row["VOUCHERTYPENAME"] or "Unknown"
            if vtype not in type_groups:
                type_groups[vtype] = []
            type_groups[vtype].append(row)

        # Summary by type
        st.markdown("**Breakdown by Voucher Type:**")
        type_summary = []
        for vtype, rows in sorted(type_groups.items(), key=lambda x: -len(x[1])):
            total_amt = sum(float(r["amount"] or 0) for r in rows)
            type_summary.append({
                "Voucher Type": vtype,
                "Count": len(rows),
                "Total Amount": fmt_inr(total_amt),
            })
        st.dataframe(pd.DataFrame(type_summary), use_container_width=True, hide_index=True)

        # Detail per type
        for vtype, rows in sorted(type_groups.items(), key=lambda x: -len(x[1])):
            total_amt = sum(float(r["amount"] or 0) for r in rows)
            with st.expander(f"{vtype} -- {len(rows)} voucher(s) | {fmt_inr(total_amt)}"):
                detail_rows = []
                for r in rows:
                    date_raw = r["DATE"] or ""
                    if date_raw and len(date_raw) >= 8:
                        display_date = f"{date_raw[6:8]}/{date_raw[4:6]}/{date_raw[:4]}"
                    else:
                        display_date = date_raw
                    detail_rows.append({
                        "Date": display_date,
                        "Number": r["VOUCHERNUMBER"] or "",
                        "Party": r["PARTYLEDGERNAME"] or "",
                        "Amount": fmt_inr(float(r["amount"] or 0)),
                    })
                st.dataframe(
                    pd.DataFrame(detail_rows),
                    use_container_width=True,
                    hide_index=True,
                    height=min(400, 40 + len(detail_rows) * 35),
                )
