"""
Narration Training Data Pipeline -- Review & Labeling Interface
Build a verified dataset of narration classifications for ML/LLM fine-tuning.
"""

import sys
import os
import io
import tempfile
import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from styles import (
    inject_base_styles, page_header, section_header, metric_card,
    badge, footer, info_banner, fmt, fmt_inr,
)
from narration_trainer import (
    CATEGORIES, sync_training_table, generate_training_batch,
    save_review, save_batch_reviews, skip_narration,
    get_training_stats, get_count_by_filter,
    export_training_data, export_training_excel,
    import_reviewed_excel, compute_accuracy,
)

# ── PAGE CONFIG ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Narration Training | Seven Labs Vision",
    page_icon="SLV",
    layout="wide",
)
inject_base_styles()

# ── DATABASE ─────────────────────────────────────────────────────────────────

DB_PATH = st.session_state.get("db_path", "")
if not DB_PATH or not os.path.exists(DB_PATH):
    page_header("Narration Training", "Build labeled datasets for ML fine-tuning")
    info_banner("No database loaded. Please load a Tally database from the Setup page.", "warning")
    st.stop()

# ── SYNC ON FIRST LOAD ──────────────────────────────────────────────────────

if "nt_synced" not in st.session_state:
    with st.spinner("Syncing narrations to training table..."):
        new_count = sync_training_table(DB_PATH)
    st.session_state["nt_synced"] = True
    if new_count > 0:
        st.toast(f"Added {new_count:,} new narrations to training table")

# ── SIDEBAR FILTERS ──────────────────────────────────────────────────────────

st.sidebar.markdown("### Training Filters")
st.sidebar.markdown("---")

filter_status = st.sidebar.selectbox(
    "Show status",
    ["unreviewed", "verified", "corrected", "skipped", "all"],
    index=0,
    key="nt_filter_status",
)

filter_category = st.sidebar.selectbox(
    "Category filter",
    ["All Categories"] + CATEGORIES,
    index=0,
    key="nt_filter_cat",
)
if filter_category == "All Categories":
    filter_category = None

min_amount = st.sidebar.number_input("Min amount", value=0.0, step=1000.0, key="nt_min_amt")
max_amount = st.sidebar.number_input("Max amount", value=0.0, step=1000.0, key="nt_max_amt",
                                      help="0 = no upper limit")
if max_amount == 0:
    max_amount = None
if min_amount == 0:
    min_amount = None

# Sidebar stats
st.sidebar.markdown("---")
stats = get_training_stats(DB_PATH)
st.sidebar.markdown("### Quick Stats")
st.sidebar.metric("Total Narrations", f"{stats['total']:,}")
st.sidebar.metric("Reviewed", f"{stats['reviewed']:,}")
st.sidebar.metric("Regex Accuracy", f"{stats['accuracy']}%")
st.sidebar.metric("Unreviewed", f"{stats['unreviewed']:,}")

# ── HEADER ───────────────────────────────────────────────────────────────────

page_header(
    "Narration Training Pipeline",
    "Review regex classifications, correct mistakes, and export training data for LLM fine-tuning",
)

# ── TOP METRICS ──────────────────────────────────────────────────────────────

c1, c2, c3, c4 = st.columns(4)
with c1:
    metric_card("Total Narrations", f"{stats['total']:,}",
                sub=f"{stats['unreviewed']:,} pending", color_class="blue")
with c2:
    metric_card("Reviewed", f"{stats['reviewed']:,}",
                sub=f"{stats['verified']:,} verified, {stats['corrected']:,} corrected",
                color_class="green")
with c3:
    acc_color = "green" if stats["accuracy"] >= 80 else ("amber" if stats["accuracy"] >= 60 else "red")
    metric_card("Regex Accuracy", f"{stats['accuracy']}%",
                sub=f"Based on {stats['reviewed']:,} reviews", color_class=acc_color)
with c4:
    metric_card("Categories Covered", f"{stats['regex_categories']}",
                sub=f"{stats['human_categories']} human-labeled", color_class="purple")

# Progress bar
if stats["total"] > 0:
    progress = stats["reviewed"] / stats["total"]
    st.progress(progress, text=f"{progress * 100:.1f}% reviewed ({stats['reviewed']:,} of {stats['total']:,})")

st.markdown("")

# ── TABS ─────────────────────────────────────────────────────────────────────

tab_quick, tab_bulk, tab_export, tab_accuracy = st.tabs([
    "Quick Review", "Bulk Review", "Export / Import", "Accuracy Report",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1: QUICK REVIEW
# ══════════════════════════════════════════════════════════════════════════════

with tab_quick:
    section_header("Quick Review Mode")

    # Initialize review index
    if "nt_review_idx" not in st.session_state:
        st.session_state["nt_review_idx"] = 0

    total_matching = get_count_by_filter(DB_PATH, filter_status, filter_category)

    if total_matching == 0:
        info_banner(
            f"No narrations matching filter: status={filter_status or 'all'}"
            + (f", category={filter_category}" if filter_category else ""),
            "info",
        )
    else:
        # Always fetch from offset 0 since reviewed items disappear from unreviewed
        batch = generate_training_batch(
            DB_PATH,
            batch_size=1,
            offset=0,
            filter_status=filter_status,
            filter_category=filter_category,
            min_amount=min_amount,
            max_amount=max_amount,
        )

        if not batch:
            info_banner("No more narrations to review with current filters.", "success")
        else:
            item = batch[0]
            reviewed_so_far = stats["reviewed"]

            st.markdown(
                f'<p style="font-size:0.85rem;color:#64748b;">'
                f'{reviewed_so_far:,} of {stats["total"]:,} reviewed '
                f'&nbsp;|&nbsp; {total_matching:,} matching current filter</p>',
                unsafe_allow_html=True,
            )

            # Narration card
            conf_badge = (
                badge("High confidence", "green") if item["regex_confidence"] >= 0.9
                else badge("Medium confidence", "amber") if item["regex_confidence"] >= 0.6
                else badge("Low confidence", "red")
            )
            amount_display = fmt_inr(item["amount"])

            st.markdown(f'''
            <div class="card" style="border-left: 4px solid #2563eb;">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">
                    <div>
                        <span style="font-size:0.8rem;color:#64748b;">
                            {item["date"]} &nbsp;|&nbsp; {item["voucher_type"]}
                        </span>
                    </div>
                    <div>
                        <span style="font-family:'JetBrains Mono',monospace;font-weight:700;font-size:1.1rem;color:#0f172a;">
                            {amount_display}
                        </span>
                    </div>
                </div>
                <div style="margin-bottom:8px;">
                    <span style="font-size:0.75rem;color:#94a3b8;text-transform:uppercase;letter-spacing:0.04em;">Party</span>
                    <p style="margin:2px 0 12px 0;font-weight:600;color:#1e293b;">{item["party"] or "—"}</p>
                </div>
                <div style="margin-bottom:12px;">
                    <span style="font-size:0.75rem;color:#94a3b8;text-transform:uppercase;letter-spacing:0.04em;">Narration</span>
                    <p style="margin:2px 0;font-size:1.05rem;font-weight:500;color:#0f172a;background:#f8fafc;padding:12px 16px;border-radius:8px;border:1px solid #e2e8f0;">
                        {item["narration"] or "<em>Empty</em>"}
                    </p>
                </div>
                <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;">
                    <span style="font-size:0.8rem;color:#475569;font-weight:600;">Regex says:</span>
                    {badge(item["regex_category"], "blue")}
                    {conf_badge}
                </div>
            </div>
            ''', unsafe_allow_html=True)

            # Classification controls
            col_cat, col_notes = st.columns([2, 1])

            with col_cat:
                # Pre-select the regex suggestion
                default_idx = 0
                if item["regex_category"] in CATEGORIES:
                    default_idx = CATEGORIES.index(item["regex_category"])
                user_category = st.selectbox(
                    "Your classification",
                    CATEGORIES,
                    index=default_idx,
                    key=f"nt_cat_{item['guid'][:12]}",
                )

            with col_notes:
                user_notes = st.text_input(
                    "Notes (optional)",
                    value="",
                    key=f"nt_notes_{item['guid'][:12]}",
                    placeholder="e.g. ambiguous, check later",
                )

            # Action buttons
            b1, b2, b3, b4 = st.columns([1, 1.5, 1, 2])
            with b1:
                if st.button("Correct", key=f"nt_ok_{item['guid'][:12]}", type="primary",
                             use_container_width=True):
                    save_review(DB_PATH, item["guid"], item["regex_category"],
                                notes=user_notes, status="verified")
                    st.session_state["nt_synced"] = False  # Force re-sync stats
                    st.rerun()

            with b2:
                if st.button("Wrong -- Save My Choice", key=f"nt_wrong_{item['guid'][:12]}",
                             use_container_width=True):
                    save_review(DB_PATH, item["guid"], user_category,
                                notes=user_notes)
                    st.session_state["nt_synced"] = False
                    st.rerun()

            with b3:
                if st.button("Skip", key=f"nt_skip_{item['guid'][:12]}",
                             use_container_width=True):
                    skip_narration(DB_PATH, item["guid"])
                    st.session_state["nt_synced"] = False
                    st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2: BULK REVIEW
# ══════════════════════════════════════════════════════════════════════════════

with tab_bulk:
    section_header("Bulk Review Mode")

    BULK_SIZE = 50

    # Pagination
    total_bulk = get_count_by_filter(DB_PATH, filter_status, filter_category)
    total_pages = max(1, (total_bulk + BULK_SIZE - 1) // BULK_SIZE)

    col_pg1, col_pg2, col_pg3 = st.columns([1, 2, 1])
    with col_pg2:
        page_num = st.number_input(
            "Page", min_value=1, max_value=total_pages, value=1,
            key="nt_bulk_page",
        )
    st.caption(f"Showing page {page_num} of {total_pages} ({total_bulk:,} narrations)")

    offset = (page_num - 1) * BULK_SIZE
    batch = generate_training_batch(
        DB_PATH,
        batch_size=BULK_SIZE,
        offset=offset,
        filter_status=filter_status,
        filter_category=filter_category,
        min_amount=min_amount,
        max_amount=max_amount,
    )

    if not batch:
        info_banner("No narrations matching current filters.", "info")
    else:
        # Build editable dataframe
        df_data = []
        for item in batch:
            df_data.append({
                "GUID": item["guid"][:12] + "...",
                "_guid_full": item["guid"],
                "Date": item["date"],
                "Type": item["voucher_type"],
                "Party": item["party"][:30] if item["party"] else "",
                "Amount": item["amount"],
                "Narration": item["narration"][:80] if item["narration"] else "",
                "Regex Category": item["regex_category"],
                "Human Category": item["human_category"] or item["regex_category"],
                "Status": item["status"],
            })

        df = pd.DataFrame(df_data)

        # Show as editable table
        edited_df = st.data_editor(
            df.drop(columns=["_guid_full"]),
            column_config={
                "Human Category": st.column_config.SelectboxColumn(
                    "Human Category",
                    options=CATEGORIES,
                    required=True,
                ),
                "Amount": st.column_config.NumberColumn("Amount", format="%.2f"),
                "Status": st.column_config.TextColumn("Status", disabled=True),
            },
            disabled=["GUID", "Date", "Type", "Party", "Amount", "Narration",
                       "Regex Category", "Status"],
            hide_index=True,
            use_container_width=True,
            key="nt_bulk_editor",
        )

        if st.button("Save All Changes", type="primary", key="nt_bulk_save"):
            reviews = []
            for i, row in edited_df.iterrows():
                human_cat = row["Human Category"]
                if human_cat and human_cat != df_data[i].get("Human Category"):
                    reviews.append({
                        "guid": df_data[i]["_guid_full"],
                        "human_category": human_cat,
                    })
                elif human_cat:
                    # Save even unchanged as verified
                    reviews.append({
                        "guid": df_data[i]["_guid_full"],
                        "human_category": human_cat,
                    })

            if reviews:
                save_batch_reviews(DB_PATH, reviews)
                st.session_state["nt_synced"] = False
                st.toast(f"Saved {len(reviews)} reviews")
                st.rerun()
            else:
                st.info("No changes to save.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3: EXPORT / IMPORT
# ══════════════════════════════════════════════════════════════════════════════

with tab_export:
    section_header("Export & Import")

    col_e1, col_e2, col_e3 = st.columns(3)

    # ── Export to Excel
    with col_e1:
        st.markdown("#### Export to Excel")
        st.caption("Download all narrations with regex and human classifications for offline review.")
        if st.button("Export to Excel", key="nt_export_xl", use_container_width=True):
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
                tmp_path = tmp.name
            count = export_training_excel(DB_PATH, tmp_path)
            with open(tmp_path, "rb") as f:
                data = f.read()
            os.unlink(tmp_path)
            st.download_button(
                label=f"Download ({count:,} rows)",
                data=data,
                file_name="narration_training_data.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="nt_dl_xl",
            )

    # ── Import reviewed Excel
    with col_e2:
        st.markdown("#### Import Reviewed Excel")
        st.caption("Upload corrected Excel back. Fill the 'Human Category' column with correct labels.")
        uploaded_file = st.file_uploader(
            "Upload reviewed Excel",
            type=["xlsx", "xls"],
            key="nt_import_xl",
        )
        if uploaded_file is not None:
            if st.button("Process Import", key="nt_process_import"):
                with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
                    tmp.write(uploaded_file.getvalue())
                    tmp_path = tmp.name
                result = import_reviewed_excel(DB_PATH, tmp_path)
                os.unlink(tmp_path)
                st.session_state["nt_synced"] = False
                st.success(f"Updated: {result['updated']:,}  |  Skipped: {result['skipped']:,}")
                if result["errors"]:
                    for err in result["errors"][:10]:
                        st.warning(err)
                st.rerun()

    # ── Export JSONL for fine-tuning
    with col_e3:
        st.markdown("#### Export Training JSONL")
        st.caption("OpenAI fine-tuning format. Only verified/corrected narrations are included.")
        reviewed_count = stats["reviewed"]
        st.markdown(f"**{reviewed_count:,}** labeled examples available")
        if reviewed_count == 0:
            st.info("Review some narrations first to generate training data.")
        else:
            if st.button("Generate JSONL", key="nt_export_jsonl", use_container_width=True):
                with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False, mode="w") as tmp:
                    tmp_path = tmp.name
                count = export_training_data(DB_PATH, tmp_path)
                with open(tmp_path, "rb") as f:
                    data = f.read()
                os.unlink(tmp_path)
                st.download_button(
                    label=f"Download JSONL ({count:,} examples)",
                    data=data,
                    file_name="narration_training.jsonl",
                    mime="application/jsonl",
                    key="nt_dl_jsonl",
                )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4: ACCURACY REPORT
# ══════════════════════════════════════════════════════════════════════════════

with tab_accuracy:
    section_header("Regex Accuracy Report")

    if stats["reviewed"] < 10:
        info_banner(
            f"Need at least 10 reviewed narrations for accuracy stats. "
            f"Currently have {stats['reviewed']}. Go to Quick Review to start labeling.",
            "warning",
        )
    else:
        acc = compute_accuracy(DB_PATH)

        # Overall accuracy
        acc_pct = acc["overall_accuracy"]
        acc_clr = "green" if acc_pct >= 80 else ("amber" if acc_pct >= 60 else "red")

        ca1, ca2, ca3 = st.columns(3)
        with ca1:
            metric_card("Overall Accuracy", f"{acc_pct}%",
                        sub=f"Based on {acc['total_reviewed']:,} reviews", color_class=acc_clr)
        with ca2:
            metric_card("Categories Evaluated",
                        f"{len(acc['per_category'])}",
                        color_class="blue")
        with ca3:
            misclass_count = sum(c for _, _, c in acc["top_misclassifications"])
            metric_card("Total Misclassifications",
                        f"{misclass_count:,}",
                        sub=f"{len(acc['top_misclassifications'])} patterns",
                        color_class="red")

        st.markdown("")

        # Per-category precision/recall table
        st.markdown("#### Per-Category Performance")
        if acc["per_category"]:
            rows_data = []
            for cat, m in sorted(acc["per_category"].items(),
                                  key=lambda x: -x[1]["f1"]):
                rows_data.append({
                    "Category": cat,
                    "True Positives": m["tp"],
                    "False Positives": m["fp"],
                    "False Negatives": m["fn"],
                    "Precision %": m["precision"],
                    "Recall %": m["recall"],
                    "F1 Score": m["f1"],
                })
            df_perf = pd.DataFrame(rows_data)
            st.dataframe(
                df_perf,
                column_config={
                    "Precision %": st.column_config.ProgressColumn(
                        "Precision %", min_value=0, max_value=100, format="%.1f%%",
                    ),
                    "Recall %": st.column_config.ProgressColumn(
                        "Recall %", min_value=0, max_value=100, format="%.1f%%",
                    ),
                    "F1 Score": st.column_config.ProgressColumn(
                        "F1 Score", min_value=0, max_value=100, format="%.1f",
                    ),
                },
                hide_index=True,
                use_container_width=True,
            )

        st.markdown("")

        # Top misclassifications
        if acc["top_misclassifications"]:
            st.markdown("#### Regex Got These Wrong Most Often")
            mis_data = []
            for regex_cat, human_cat, cnt in acc["top_misclassifications"][:10]:
                mis_data.append({
                    "Regex Said": regex_cat,
                    "Should Be": human_cat,
                    "Count": cnt,
                })
            st.dataframe(
                pd.DataFrame(mis_data),
                hide_index=True,
                use_container_width=True,
            )

        # Confusion matrix
        if acc["total_reviewed"] >= 50:
            st.markdown("#### Confusion Matrix")
            st.caption("Rows = Regex prediction, Columns = Human label")

            # Build pivot
            confusion_rows = []
            for (rc, hc), cnt in acc["confusion"].items():
                confusion_rows.append({
                    "Regex Category": rc,
                    "Human Category": hc,
                    "Count": cnt,
                })
            if confusion_rows:
                df_conf = pd.DataFrame(confusion_rows)
                pivot = df_conf.pivot_table(
                    index="Regex Category",
                    columns="Human Category",
                    values="Count",
                    aggfunc="sum",
                    fill_value=0,
                )
                st.dataframe(pivot, use_container_width=True)


# ── FOOTER ───────────────────────────────────────────────────────────────────

footer()
