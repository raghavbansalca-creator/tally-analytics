"""
Seven Labs Vision -- Company Intelligence Profile
Displays the auto-detected company profile: entity type, business nature, industry,
complexity score, detection signals, and recommended analyses.
"""

import streamlit as st
import sys, os, json

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tally_data.db")

st.set_page_config(page_title="Company Profile", page_icon="\U0001f3e2", layout="wide")


# ── STYLING ──────────────────────────────────────────────────────────────────

CARD_CSS = """
<style>
.profile-card {
    border: 2px solid #e0e0e0;
    border-radius: 12px;
    padding: 20px 16px;
    text-align: center;
    min-height: 140px;
    background: #fafafa;
    margin-bottom: 8px;
}
.profile-card .card-icon {
    font-size: 28px;
    margin-bottom: 6px;
}
.profile-card .card-label {
    color: #888;
    font-size: 13px;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-bottom: 4px;
}
.profile-card .card-value {
    font-size: 22px;
    font-weight: 700;
    margin-bottom: 0;
}
.card-border-blue { border-left: 5px solid #2196F3; }
.card-border-green { border-left: 5px solid #4CAF50; }
.card-border-orange { border-left: 5px solid #FF9800; }
.card-border-purple { border-left: 5px solid #9C27B0; }

.detail-grid {
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    gap: 8px 24px;
}
.detail-item {
    padding: 6px 0;
}
.detail-item .detail-label {
    color: #888;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}
.detail-item .detail-value {
    font-size: 15px;
    font-weight: 600;
}

.feature-item {
    padding: 4px 8px;
    margin: 3px 0;
    border-radius: 6px;
    background: #e8f5e9;
    display: inline-block;
    font-size: 14px;
}
.feature-check {
    color: #4CAF50;
    font-weight: bold;
    margin-right: 6px;
}

.priority-high {
    background: #ffebee;
    color: #c62828;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 12px;
    font-weight: 600;
}
.priority-medium {
    background: #fff3e0;
    color: #e65100;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 12px;
    font-weight: 600;
}
.priority-low {
    background: #e8f5e9;
    color: #2e7d32;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 12px;
    font-weight: 600;
}

.confidence-bar-container {
    display: flex;
    align-items: center;
    margin: 4px 0;
}
.confidence-label {
    width: 140px;
    font-size: 13px;
    text-align: right;
    padding-right: 10px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.confidence-bar {
    height: 20px;
    border-radius: 4px;
    min-width: 4px;
}
.confidence-score {
    font-size: 12px;
    padding-left: 6px;
    color: #666;
    min-width: 30px;
}

.stat-box {
    text-align: center;
    padding: 10px;
    border: 1px solid #eee;
    border-radius: 8px;
    background: #fafafa;
}
.stat-box .stat-num {
    font-size: 20px;
    font-weight: 700;
    color: #333;
}
.stat-box .stat-label {
    font-size: 11px;
    color: #999;
    text-transform: uppercase;
}
</style>
"""

st.markdown(CARD_CSS, unsafe_allow_html=True)


# ── HELPERS ──────────────────────────────────────────────────────────────────

def safe_get(d, key, default=""):
    if d is None:
        return default
    v = d.get(key)
    return v if v is not None else default


def fmt_number(n):
    """Format a number with commas."""
    if n is None:
        return "0"
    try:
        return f"{int(n):,}"
    except (ValueError, TypeError):
        return str(n)


def render_confidence_bars(scores, color="#2196F3"):
    """Render horizontal bars for confidence scores."""
    if not scores:
        st.caption("No confidence data available")
        return
    max_score = max(scores.values()) if scores else 1
    html_parts = []
    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    for label, score in sorted_scores:
        width_pct = int((score / max(max_score, 1)) * 100)
        width_pct = max(width_pct, 5)
        html_parts.append(
            f'<div class="confidence-bar-container">'
            f'<div class="confidence-label">{label}</div>'
            f'<div class="confidence-bar" style="width:{width_pct}%;background:{color};"></div>'
            f'<div class="confidence-score">{score}</div>'
            f'</div>'
        )
    st.markdown("".join(html_parts), unsafe_allow_html=True)


def render_signal_list(signals_dict, keys_to_show):
    """Render a set of signal key-value pairs nicely."""
    for key in keys_to_show:
        if key in signals_dict:
            val = signals_dict[key]
            display_key = key.replace("_", " ").title()
            if isinstance(val, list):
                if val:
                    st.markdown(f"**{display_key}:** {', '.join(str(v) for v in val[:10])}")
            elif isinstance(val, dict):
                if val:
                    items = [f"{k}: {v}" for k, v in list(val.items())[:10]]
                    st.markdown(f"**{display_key}:** {', '.join(items)}")
            elif isinstance(val, bool):
                st.markdown(f"**{display_key}:** {'Yes' if val else 'No'}")
            else:
                st.markdown(f"**{display_key}:** {val}")


# ── LOAD PROFILE ─────────────────────────────────────────────────────────────

def load_existing_profile():
    """Load profile from database, return None if not available."""
    try:
        from company_profiler import load_profile
        return load_profile(DB_PATH)
    except Exception:
        return None


def run_profiler():
    """Run the profiler and return the new profile."""
    try:
        from company_profiler import profile_company
        return profile_company(DB_PATH)
    except Exception as e:
        st.error(f"Error running profiler: {e}")
        return None


# ── MAIN PAGE ────────────────────────────────────────────────────────────────

profile = load_existing_profile()

# Re-profile button in sidebar
st.sidebar.markdown("### Actions")
if st.sidebar.button("Re-Profile Company", key="btn_reprofile", type="primary"):
    with st.spinner("Analyzing company data..."):
        profile = run_profiler()
    if profile:
        st.sidebar.success("Profile updated!")
        st.rerun()

if profile is None:
    st.markdown("## Company Intelligence Profile")
    st.info("No company profile found. Click **Re-Profile Company** in the sidebar to analyze the loaded Tally data.")
    st.stop()


# ── HEADER ───────────────────────────────────────────────────────────────────

company_name = safe_get(profile, "company_name", "Company")
st.markdown(f"## Company Intelligence Profile")
st.markdown(f"### {company_name}")
st.markdown("---")


# ── PROFILE CARDS ROW ────────────────────────────────────────────────────────

c1, c2, c3, c4 = st.columns(4)

entity_type = safe_get(profile, "entity_type", "Unknown")
business_nature = safe_get(profile, "business_nature", "Unknown")
industry = safe_get(profile, "industry", "General")
complexity = safe_get(profile, "complexity", "Unknown")
complexity_score = int(safe_get(profile, "complexity_score", 0))

with c1:
    st.markdown(
        f'<div class="profile-card card-border-blue">'
        f'<div class="card-icon">[E]</div>'
        f'<div class="card-label">Entity Type</div>'
        f'<div class="card-value">{entity_type}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

with c2:
    st.markdown(
        f'<div class="profile-card card-border-green">'
        f'<div class="card-icon">[B]</div>'
        f'<div class="card-label">Business Nature</div>'
        f'<div class="card-value">{business_nature}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

with c3:
    st.markdown(
        f'<div class="profile-card card-border-orange">'
        f'<div class="card-icon">[I]</div>'
        f'<div class="card-label">Industry</div>'
        f'<div class="card-value">{industry}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

with c4:
    st.markdown(
        f'<div class="profile-card card-border-purple">'
        f'<div class="card-icon">[C]</div>'
        f'<div class="card-label">Complexity</div>'
        f'<div class="card-value">{complexity} {complexity_score}/10</div>'
        f'</div>',
        unsafe_allow_html=True,
    )
    st.progress(complexity_score / 10)

st.markdown("")


# ── COMPANY DETAILS ──────────────────────────────────────────────────────────

st.markdown("### Company Details")

d1, d2, d3 = st.columns(3)

with d1:
    gstin = safe_get(profile, "gstin", "N/A")
    st.markdown(f"**GSTIN:** {gstin if gstin else 'N/A'}")
    state = safe_get(profile, "state", "N/A")
    st.markdown(f"**State:** {state if state else 'N/A'}")

with d2:
    gst_reg = safe_get(profile, "gst_registration_type", "N/A")
    st.markdown(f"**GST Registration:** {gst_reg if gst_reg else 'N/A'}")

with d3:
    pass

# Data volume stats
stats = safe_get(profile, "stats", {})
if stats:
    st.markdown("#### Data Volume")
    stat_cols = st.columns(7)
    stat_labels = [
        ("mst_group", "Groups"),
        ("mst_ledger", "Ledgers"),
        ("mst_stock_item", "Stock Items"),
        ("mst_godown", "Godowns"),
        ("mst_voucher_type", "Voucher Types"),
        ("trn_voucher", "Vouchers"),
        ("trn_accounting", "Entries"),
    ]
    for i, (key, label) in enumerate(stat_labels):
        with stat_cols[i]:
            val = stats.get(key, 0)
            st.markdown(
                f'<div class="stat-box">'
                f'<div class="stat-num">{fmt_number(val)}</div>'
                f'<div class="stat-label">{label}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

st.markdown("---")


# ── COMPLEXITY FEATURES ──────────────────────────────────────────────────────

st.markdown("### Complexity Features")
features = safe_get(profile, "features", {})
if features:
    cols = st.columns(3)
    feature_list = list(features.items())
    for i, (key, desc) in enumerate(feature_list):
        with cols[i % 3]:
            st.markdown(
                f'<div class="feature-item">'
                f'<span class="feature-check">&#10003;</span>'
                f'{desc}'
                f'</div>',
                unsafe_allow_html=True,
            )
    st.markdown("")
else:
    st.caption("No complexity features detected.")

st.markdown("---")


# ── DETECTION CONFIDENCE ─────────────────────────────────────────────────────

st.markdown("### Detection Confidence")

signals = safe_get(profile, "signals", {})

conf1, conf2, conf3 = st.columns(3)

with conf1:
    st.markdown("**Entity Type Scores**")
    entity_signals = signals.get("entity_type", {})
    entity_scores = entity_signals.get("scores", {})
    render_confidence_bars(entity_scores, "#2196F3")

with conf2:
    st.markdown("**Business Nature Scores**")
    nature_signals = signals.get("business_nature", {})
    nature_scores = nature_signals.get("scores", {})
    render_confidence_bars(nature_scores, "#4CAF50")

with conf3:
    st.markdown("**Industry Scores**")
    industry_signals = signals.get("industry", {})
    industry_scores = industry_signals.get("scores", {})
    render_confidence_bars(industry_scores, "#FF9800")

st.markdown("---")


# ── DETECTION SIGNALS ────────────────────────────────────────────────────────

st.markdown("### Detection Signals")

with st.expander("Entity Type Signals"):
    es = signals.get("entity_type", {})
    if es:
        render_signal_list(es, [
            "pan_4th_char", "pan_entity", "capital_ledgers",
            "has_share_capital", "partner_capital_count",
            "name_has_pvt", "name_has_ltd", "name_has_llp",
            "company_indicator_ledgers", "gst_composition", "gst_isd",
        ])
        # Show scores summary
        scores = es.get("scores", {})
        if scores:
            st.markdown("**Score Summary:**")
            parts = [f"{k}: {v}" for k, v in sorted(scores.items(), key=lambda x: -x[1])]
            st.code(", ".join(parts))
    else:
        st.caption("No entity type signals available.")

with st.expander("Business Nature Signals"):
    ns = signals.get("business_nature", {})
    if ns:
        render_signal_list(ns, [
            "manufacturing_groups", "defined_but_unused_mfg_types",
            "used_manufacturing_types", "manufacturing_voucher_count",
            "has_stock_items", "stock_item_count",
            "service_income_ledgers", "trade_account_count",
            "cost_centre_vouchers", "mixed_primary", "mixed_secondary",
        ])
        scores = ns.get("scores", {})
        if scores:
            st.markdown("**Score Summary:**")
            parts = [f"{k}: {v}" for k, v in sorted(scores.items(), key=lambda x: -x[1])]
            st.code(", ".join(parts))
    else:
        st.caption("No business nature signals available.")

with st.expander("Industry Signals"):
    isig = signals.get("industry", {})
    if isig:
        render_signal_list(isig, [
            "batch_items", "perishable_items", "pharma_item_match_pct",
            "pharma_ledgers", "ecommerce_ledgers", "realestate_ledgers",
            "weight_based_items", "jewellery_items",
            "tracking_number_vouchers", "transport_ledgers",
            "handicraft_ledgers", "company_name_handicraft",
            "hospital_ledgers", "education_ledgers", "restaurant_ledgers",
            "forex_ledgers", "import_export_ledgers",
        ])
        scores = isig.get("scores", {})
        if scores:
            st.markdown("**Score Summary:**")
            parts = [f"{k}: {v}" for k, v in sorted(scores.items(), key=lambda x: -x[1])]
            st.code(", ".join(parts))
    else:
        st.caption("No industry signals available.")

st.markdown("---")


# ── RECOMMENDED ANALYSES ─────────────────────────────────────────────────────

st.markdown("### Recommended Analyses")

recommendations = safe_get(profile, "recommendations", [])
if recommendations:
    # Group by category
    categories = {}
    for rec in recommendations:
        cat = rec.get("category", "Other")
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(rec)

    # Category display order
    cat_order = ["Audit", "Analysis", "Compliance", "Industry"]
    all_cats = cat_order + [c for c in categories if c not in cat_order]

    for cat in all_cats:
        if cat not in categories:
            continue
        recs = categories[cat]
        st.markdown(f"#### {cat}")

        table_html = '<table style="width:100%;border-collapse:collapse;margin-bottom:16px;">'
        table_html += (
            '<tr style="background:#f5f5f5;border-bottom:2px solid #ddd;">'
            '<th style="text-align:left;padding:8px;">Name</th>'
            '<th style="text-align:left;padding:8px;">Description</th>'
            '<th style="text-align:center;padding:8px;width:90px;">Priority</th>'
            '</tr>'
        )
        for rec in recs:
            name = rec.get("name", "")
            desc = rec.get("description", "")
            priority = rec.get("priority", "Medium")
            p_lower = priority.lower()
            if p_lower == "high":
                badge = '<span class="priority-high">HIGH</span>'
            elif p_lower == "medium" or p_lower == "med":
                badge = '<span class="priority-medium">MED</span>'
            else:
                badge = '<span class="priority-low">LOW</span>'

            table_html += (
                f'<tr style="border-bottom:1px solid #eee;">'
                f'<td style="padding:8px;font-weight:600;">{name}</td>'
                f'<td style="padding:8px;color:#555;">{desc}</td>'
                f'<td style="padding:8px;text-align:center;">{badge}</td>'
                f'</tr>'
            )
        table_html += '</table>'
        st.markdown(table_html, unsafe_allow_html=True)
else:
    st.caption("No recommendations available. Run the profiler first.")

st.markdown("---")


# ── RE-PROFILE BUTTON (MAIN AREA) ───────────────────────────────────────────

st.markdown("")
bc1, bc2, bc3 = st.columns([1, 1, 1])
with bc2:
    if st.button("Re-Profile Company", key="btn_reprofile_main"):
        with st.spinner("Analyzing company data..."):
            profile = run_profiler()
        if profile:
            st.success("Profile updated successfully!")
            st.rerun()


# ── PERSISTENT CHAT BAR ─────────────────────────────────────────────────────

try:
    from chat_engine import ask, format_result_as_text
    chat_input = st.chat_input("Ask anything -- P&L, Balance Sheet, ledger of [party], debtors, creditors...")
    if chat_input:
        result = ask(chat_input)
        st.markdown(f"**You:** {chat_input}")
        st.markdown(format_result_as_text(result))
except ImportError:
    pass
except Exception:
    pass
