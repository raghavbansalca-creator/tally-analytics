"""
Seven Labs Vision -- Company Intelligence Profile
Displays the auto-detected company profile: entity type, business nature, industry,
complexity score, detection signals, and recommended analyses.

Defensive: try/except around each section, safe column access, safe division,
safe list operations.
"""

import streamlit as st
import sys, os, json

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tally_data.db")

st.set_page_config(page_title="Company Profile", page_icon="", layout="wide")

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from styles import inject_base_styles, page_header, section_header, metric_card, fmt, fmt_full, badge, footer, empty_state, info_banner
inject_base_styles()


# -- HELPERS ------------------------------------------------------------------

def safe_get(d, key, default=""):
    if d is None or not isinstance(d, dict):
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
    if not scores or not isinstance(scores, dict):
        st.caption("No confidence data available")
        return
    max_score = max(scores.values()) if scores else 1
    if max_score == 0:
        max_score = 1  # safe division
    html_parts = []
    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    for label, score in sorted_scores:
        width_pct = int((score / max(max_score, 1)) * 100)
        width_pct = max(width_pct, 5)
        html_parts.append(
            f'<div style="display:flex;align-items:center;margin:4px 0;">'
            f'<div style="width:140px;font-size:13px;text-align:right;padding-right:10px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{label}</div>'
            f'<div style="height:20px;border-radius:4px;min-width:4px;width:{width_pct}%;background:{color};"></div>'
            f'<div style="font-size:12px;padding-left:6px;color:#666;min-width:30px;">{score}</div>'
            f'</div>'
        )
    st.markdown("".join(html_parts), unsafe_allow_html=True)


def render_signal_list(signals_dict, keys_to_show):
    """Render a set of signal key-value pairs nicely."""
    if not isinstance(signals_dict, dict):
        return
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


# -- LOAD PROFILE -------------------------------------------------------------

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


# -- MAIN PAGE ----------------------------------------------------------------

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
    page_header("Company Intelligence Profile", "Analyze your Tally data to detect entity type, business nature, and industry")
    st.info("No company profile found. Click **Re-Profile Company** in the sidebar to analyze the loaded Tally data.")
    st.stop()

if not isinstance(profile, dict):
    st.error("Profile has unexpected format.")
    st.stop()


# -- HEADER -------------------------------------------------------------------

company_name = safe_get(profile, "company_name", "Company")
page_header("Company Intelligence Profile", company_name)


# -- PROFILE CARDS ROW -------------------------------------------------------

try:
    c1, c2, c3, c4 = st.columns(4)

    entity_type = safe_get(profile, "entity_type", "Unknown")
    business_nature = safe_get(profile, "business_nature", "Unknown")
    industry = safe_get(profile, "industry", "General")
    complexity = safe_get(profile, "complexity", "Unknown")
    try:
        complexity_score = int(safe_get(profile, "complexity_score", 0) or 0)
    except (ValueError, TypeError):
        complexity_score = 0

    with c1:
        metric_card("Entity Type", entity_type, color_class="blue")

    with c2:
        metric_card("Business Nature", business_nature, color_class="green")

    with c3:
        metric_card("Industry", industry, color_class="amber")

    with c4:
        metric_card("Complexity", f"{complexity} {complexity_score}/10", color_class="purple")
        # Safe division: complexity_score is already int, /10 always safe
        st.progress(min(max(complexity_score, 0), 10) / 10)
except Exception as e:
    st.warning(f"Could not render profile cards: {e}")

st.markdown("")


# -- COMPANY DETAILS ----------------------------------------------------------

try:
    section_header("Company Details")

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
    if stats and isinstance(stats, dict):
        section_header("Data Volume")
        stat_labels = [
            ("mst_group", "Groups"),
            ("mst_ledger", "Ledgers"),
            ("mst_stock_item", "Stock Items"),
            ("mst_godown", "Godowns"),
            ("mst_voucher_type", "Voucher Types"),
            ("trn_voucher", "Vouchers"),
            ("trn_accounting", "Entries"),
        ]
        # Safe: only create as many columns as we have items (max 7)
        num_cols = min(len(stat_labels), 7)
        if num_cols > 0:
            stat_cols = st.columns(num_cols)
            for i, (key, label) in enumerate(stat_labels[:num_cols]):
                with stat_cols[i]:
                    val = stats.get(key, 0)
                    metric_card(label, fmt_number(val))
except Exception as e:
    st.warning(f"Could not render company details: {e}")

st.markdown("---")


# -- COMPLEXITY FEATURES ------------------------------------------------------

try:
    section_header("Complexity Features")
    features = safe_get(profile, "features", {})
    if features and isinstance(features, dict):
        feature_list = list(features.items())
        num_cols = min(3, max(1, len(feature_list)))
        cols = st.columns(num_cols)
        for i, (key, desc) in enumerate(feature_list):
            with cols[i % num_cols]:
                st.markdown(f"* {desc}")
        st.markdown("")
    else:
        st.caption("No complexity features detected.")
except Exception as e:
    st.warning(f"Could not render complexity features: {e}")

st.markdown("---")


# -- DETECTION CONFIDENCE -----------------------------------------------------

try:
    section_header("Detection Confidence")

    signals = safe_get(profile, "signals", {})
    if not isinstance(signals, dict):
        signals = {}

    conf1, conf2, conf3 = st.columns(3)

    with conf1:
        st.markdown("**Entity Type Scores**")
        entity_signals = signals.get("entity_type", {})
        entity_scores = entity_signals.get("scores", {}) if isinstance(entity_signals, dict) else {}
        render_confidence_bars(entity_scores, "#2196F3")

    with conf2:
        st.markdown("**Business Nature Scores**")
        nature_signals = signals.get("business_nature", {})
        nature_scores = nature_signals.get("scores", {}) if isinstance(nature_signals, dict) else {}
        render_confidence_bars(nature_scores, "#4CAF50")

    with conf3:
        st.markdown("**Industry Scores**")
        industry_signals = signals.get("industry", {})
        industry_scores = industry_signals.get("scores", {}) if isinstance(industry_signals, dict) else {}
        render_confidence_bars(industry_scores, "#FF9800")
except Exception as e:
    st.warning(f"Could not render detection confidence: {e}")

st.markdown("---")


# -- DETECTION SIGNALS --------------------------------------------------------

try:
    section_header("Detection Signals")

    with st.expander("Entity Type Signals"):
        es = signals.get("entity_type", {}) if isinstance(signals, dict) else {}
        if es and isinstance(es, dict):
            render_signal_list(es, [
                "pan_4th_char", "pan_entity", "capital_ledgers",
                "has_share_capital", "partner_capital_count",
                "name_has_pvt", "name_has_ltd", "name_has_llp",
                "company_indicator_ledgers", "gst_composition", "gst_isd",
            ])
            scores = es.get("scores", {})
            if scores and isinstance(scores, dict):
                st.markdown("**Score Summary:**")
                parts = [f"{k}: {v}" for k, v in sorted(scores.items(), key=lambda x: -x[1])]
                st.code(", ".join(parts))
        else:
            st.caption("No entity type signals available.")

    with st.expander("Business Nature Signals"):
        ns = signals.get("business_nature", {}) if isinstance(signals, dict) else {}
        if ns and isinstance(ns, dict):
            render_signal_list(ns, [
                "manufacturing_groups", "defined_but_unused_mfg_types",
                "used_manufacturing_types", "manufacturing_voucher_count",
                "has_stock_items", "stock_item_count",
                "service_income_ledgers", "trade_account_count",
                "cost_centre_vouchers", "mixed_primary", "mixed_secondary",
            ])
            scores = ns.get("scores", {})
            if scores and isinstance(scores, dict):
                st.markdown("**Score Summary:**")
                parts = [f"{k}: {v}" for k, v in sorted(scores.items(), key=lambda x: -x[1])]
                st.code(", ".join(parts))
        else:
            st.caption("No business nature signals available.")

    with st.expander("Industry Signals"):
        isig = signals.get("industry", {}) if isinstance(signals, dict) else {}
        if isig and isinstance(isig, dict):
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
            if scores and isinstance(scores, dict):
                st.markdown("**Score Summary:**")
                parts = [f"{k}: {v}" for k, v in sorted(scores.items(), key=lambda x: -x[1])]
                st.code(", ".join(parts))
        else:
            st.caption("No industry signals available.")
except Exception as e:
    st.warning(f"Could not render detection signals: {e}")

st.markdown("---")


# -- RECOMMENDED ANALYSES -----------------------------------------------------

try:
    section_header("Recommended Analyses")

    recommendations = safe_get(profile, "recommendations", [])
    if recommendations and isinstance(recommendations, list):
        # Group by category
        categories = {}
        for rec in recommendations:
            if not isinstance(rec, dict):
                continue
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

            for rec in recs:
                name = rec.get("name", "")
                desc = rec.get("description", "")
                priority = rec.get("priority", "Medium")
                p_lower = str(priority).lower()
                if p_lower == "high":
                    badge_html = badge("HIGH", "red")
                elif p_lower in ("medium", "med"):
                    badge_html = badge("MED", "amber")
                else:
                    badge_html = badge("LOW", "green")

                st.markdown(f"**{name}** {badge_html} -- {desc}", unsafe_allow_html=True)
    else:
        st.caption("No recommendations available. Run the profiler first.")
except Exception as e:
    st.warning(f"Could not render recommendations: {e}")

st.markdown("---")


# -- RE-PROFILE BUTTON (MAIN AREA) -------------------------------------------

st.markdown("")
bc1, bc2, bc3 = st.columns([1, 1, 1])
with bc2:
    if st.button("Re-Profile Company", key="btn_reprofile_main"):
        with st.spinner("Analyzing company data..."):
            profile = run_profiler()
        if profile:
            st.success("Profile updated successfully!")
            st.rerun()


# -- FOOTER & PERSISTENT CHAT BAR --------------------------------------------

footer()

try:
    from chat_engine import ask, format_result_as_text
    chat_input = st.chat_input("Ask anything -- P&L, Balance Sheet, ledger of [party], debtors, creditors...")
    if chat_input:
        result = ask(chat_input)
        st.markdown(f"**You:** {chat_input}")
        if isinstance(result, dict):
            st.markdown(format_result_as_text(result))
        else:
            st.markdown(str(result))
except ImportError:
    pass
except Exception:
    pass
