"""
Seven Labs Vision -- Design System v2
Clean, minimal, professional UI inspired by Claude/Linear/Vercel.
"""

import streamlit as st


# -- FORMATTERS ---------------------------------------------------------------

def fmt(amount):
    """Format amount in Indian numbering: Cr / L / thousands."""
    if amount is None:
        return "0"
    abs_amt = abs(amount)
    sign = "-" if amount < 0 else ""
    if abs_amt >= 10000000:
        return f"{sign}{abs_amt/10000000:,.2f} Cr"
    elif abs_amt >= 100000:
        return f"{sign}{abs_amt/100000:,.2f} L"
    elif abs_amt >= 1000:
        return f"{sign}{abs_amt:,.0f}"
    else:
        return f"{sign}{abs_amt:,.2f}"


def fmt_full(amount):
    """Full formatted amount with decimals."""
    if amount is None:
        return "0.00"
    sign = "-" if amount < 0 else ""
    return f"{sign}{abs(amount):,.2f}"


def fmt_inr(amount):
    """Format with rupee symbol."""
    return f"Rs {fmt(amount)}"


# -- CSS INJECTION ------------------------------------------------------------

def inject_base_styles():
    """Inject the unified design system CSS. Call once per page."""
    css = """
/* ================================================================
   SEVEN LABS VISION -- DESIGN SYSTEM v2
   Clean, minimal, professional
   ================================================================ */

/* -- FONTS ---------------------------------------------------- */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');

/* -- BASE ----------------------------------------------------- */
html, body, [class*="st-"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
}
.block-container {
    padding: 2rem 2rem 4rem 2rem !important;
    max-width: 1280px !important;
}

/* -- SIDEBAR -------------------------------------------------- */
section[data-testid="stSidebar"] {
    background: #f8fafc !important;
    border-right: 1px solid #e2e8f0 !important;
}
section[data-testid="stSidebar"] > div {
    padding-top: 1rem !important;
}
section[data-testid="stSidebar"] * {
    color: #334155 !important;
}
section[data-testid="stSidebar"] .stMarkdown h3,
section[data-testid="stSidebar"] .stMarkdown h4 {
    color: #0f172a !important;
    font-family: 'Inter', sans-serif !important;
}
section[data-testid="stSidebar"] hr {
    border-color: #e2e8f0 !important;
    margin: 0.5rem 0 !important;
}
section[data-testid="stSidebar"] .stButton > button {
    background: transparent !important;
    color: #475569 !important;
    border: none !important;
    text-align: left !important;
    font-size: 0.875rem !important;
    font-weight: 500 !important;
    padding: 0.5rem 0.75rem !important;
    border-radius: 8px !important;
    transition: all 0.15s ease !important;
    width: 100% !important;
}
section[data-testid="stSidebar"] .stButton > button:hover {
    background: #e2e8f0 !important;
    color: #0f172a !important;
}
section[data-testid="stSidebar"] input,
section[data-testid="stSidebar"] [data-baseweb="input"] input {
    background: #ffffff !important;
    border-color: #e2e8f0 !important;
    color: #0f172a !important;
    font-size: 0.875rem !important;
}
section[data-testid="stSidebar"] .stCaption p {
    color: #94a3b8 !important;
    font-size: 0.75rem !important;
}

/* Sidebar section labels */
.sidebar-section-label {
    font-size: 0.65rem !important;
    font-weight: 700 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
    color: #94a3b8 !important;
    padding: 0.75rem 0.75rem 0.25rem !important;
    margin: 0 !important;
}

/* Company card in sidebar */
.sidebar-company-card {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    padding: 12px 14px;
    margin: 0 0 12px 0;
}
.sidebar-company-card .company-name {
    font-size: 0.82rem;
    font-weight: 700;
    color: #0f172a;
    margin: 0 0 4px 0;
    line-height: 1.3;
}
.sidebar-company-card .company-tags {
    display: flex;
    gap: 4px;
    flex-wrap: wrap;
}
.sidebar-company-card .tag {
    font-size: 0.6rem;
    font-weight: 600;
    padding: 2px 6px;
    border-radius: 4px;
    text-transform: uppercase;
    letter-spacing: 0.03em;
}
.sidebar-company-card .tag-blue { background: #dbeafe; color: #1e40af; }
.sidebar-company-card .tag-green { background: #dcfce7; color: #166534; }
.sidebar-company-card .tag-amber { background: #fef3c7; color: #92400e; }
.sidebar-company-card .tag-purple { background: #ede9fe; color: #5b21b6; }

/* -- PAGE HEADER ---------------------------------------------- */
.page-header {
    margin-bottom: 1.5rem;
    padding-bottom: 1rem;
    border-bottom: 1px solid #f1f5f9;
}
.page-header h1 {
    font-size: 1.75rem;
    font-weight: 700;
    color: #0f172a;
    margin: 0 0 0.25rem 0;
    font-family: 'Inter', sans-serif;
    line-height: 1.2;
}
.page-header .subtitle {
    font-size: 0.875rem;
    color: #64748b;
    margin: 0;
    font-weight: 400;
}

/* -- CARDS ---------------------------------------------------- */
.card {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 20px 24px;
    margin-bottom: 16px;
    transition: box-shadow 0.2s ease;
}
.card:hover {
    box-shadow: 0 4px 12px rgba(0,0,0,0.05);
}
.card-header {
    font-size: 0.875rem;
    font-weight: 600;
    color: #475569;
    margin: 0 0 16px 0;
    padding: 0 0 12px 0;
    border-bottom: 1px solid #f1f5f9;
    text-transform: uppercase;
    letter-spacing: 0.03em;
}
.card-title {
    font-size: 1rem;
    font-weight: 700;
    color: #0f172a;
    margin: 0 0 12px 0;
}

/* -- METRIC CARDS --------------------------------------------- */
.metric-card {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 20px 24px;
    position: relative;
    transition: all 0.2s ease;
}
.metric-card:hover {
    border-color: #cbd5e1;
    box-shadow: 0 2px 8px rgba(0,0,0,0.04);
}
.metric-card .metric-label {
    font-size: 0.7rem;
    font-weight: 600;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin: 0 0 6px 0;
}
.metric-card .metric-value {
    font-size: 1.5rem;
    font-weight: 700;
    color: #0f172a;
    font-family: 'JetBrains Mono', monospace;
    margin: 0 0 4px 0;
    line-height: 1.2;
}
.metric-card .metric-sub {
    font-size: 0.75rem;
    color: #64748b;
    margin: 0;
}
.metric-card .metric-value.green { color: #059669; }
.metric-card .metric-value.red { color: #dc2626; }
.metric-card .metric-value.blue { color: #2563eb; }
.metric-card .metric-value.amber { color: #d97706; }
.metric-card .metric-value.purple { color: #7c3aed; }

/* Accent stripe variants */
.metric-card--green { border-top: 3px solid #059669; }
.metric-card--red { border-top: 3px solid #dc2626; }
.metric-card--blue { border-top: 3px solid #2563eb; }
.metric-card--amber { border-top: 3px solid #d97706; }
.metric-card--purple { border-top: 3px solid #7c3aed; }

/* -- TABLES --------------------------------------------------- */
table.slv-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.825rem;
    font-family: 'Inter', sans-serif;
}
table.slv-table th {
    background: #f8fafc;
    color: #475569;
    padding: 10px 14px;
    font-weight: 600;
    text-align: right;
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    border-bottom: 2px solid #e2e8f0;
}
table.slv-table th:first-child { text-align: left; }
table.slv-table td {
    padding: 10px 14px;
    text-align: right;
    border-bottom: 1px solid #f1f5f9;
    color: #334155;
    font-size: 0.825rem;
}
table.slv-table td:first-child { text-align: left; font-weight: 500; }
table.slv-table tr:hover { background: #f8fafc; }
table.slv-table tr.total-row td {
    font-weight: 700;
    border-top: 2px solid #e2e8f0;
    background: #f8fafc;
    color: #0f172a;
}

/* -- AMOUNTS -------------------------------------------------- */
.amt { font-family: 'JetBrains Mono', monospace; font-weight: 600; }
.amt-pos { color: #059669; }
.amt-neg { color: #dc2626; }
.amt-muted { color: #94a3b8; }

/* -- BADGES --------------------------------------------------- */
.badge {
    display: inline-block;
    font-size: 0.65rem;
    font-weight: 700;
    padding: 2px 8px;
    border-radius: 100px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    line-height: 1.6;
}
.badge-green { background: #dcfce7; color: #166534; }
.badge-red { background: #fee2e2; color: #991b1b; }
.badge-amber { background: #fef3c7; color: #92400e; }
.badge-blue { background: #dbeafe; color: #1e40af; }
.badge-gray { background: #f1f5f9; color: #475569; }
.badge-purple { background: #ede9fe; color: #5b21b6; }

/* -- BREADCRUMBS ---------------------------------------------- */
.breadcrumb {
    font-size: 0.8rem;
    color: #94a3b8;
    padding: 8px 0;
    margin-bottom: 1rem;
}
.breadcrumb .sep { margin: 0 6px; }
.breadcrumb .current { color: #0f172a; font-weight: 600; }

/* -- GROUP/LEDGER ROWS ---------------------------------------- */
.row-item {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 10px 16px;
    border-radius: 8px;
    margin: 2px 0;
    transition: all 0.15s ease;
    cursor: pointer;
    border: 1px solid transparent;
}
.row-item:hover {
    background: #f8fafc;
    border-color: #e2e8f0;
}
.row-item .name {
    font-weight: 500;
    color: #1e293b;
    font-size: 0.875rem;
}
.row-item .value {
    font-family: 'JetBrains Mono', monospace;
    font-weight: 600;
    color: #475569;
    font-size: 0.875rem;
}

/* -- SECTION HEADER ------------------------------------------- */
.section-header {
    font-size: 0.875rem;
    font-weight: 700;
    color: #0f172a;
    padding: 0 0 8px 0;
    margin: 24px 0 12px 0;
    border-bottom: 1px solid #e2e8f0;
}

/* -- EMPTY STATE ---------------------------------------------- */
.empty-state {
    text-align: center;
    padding: 48px 24px;
    color: #94a3b8;
}
.empty-state .icon { font-size: 2rem; margin-bottom: 12px; }
.empty-state .title { font-size: 1rem; font-weight: 600; color: #475569; margin-bottom: 4px; }
.empty-state .desc { font-size: 0.875rem; color: #94a3b8; }

/* -- INFO BANNER ---------------------------------------------- */
.info-banner {
    padding: 12px 16px;
    border-radius: 8px;
    font-size: 0.85rem;
    margin-bottom: 16px;
    display: flex;
    align-items: flex-start;
    gap: 10px;
}
.info-banner--info { background: #eff6ff; color: #1e40af; border: 1px solid #bfdbfe; }
.info-banner--warning { background: #fffbeb; color: #92400e; border: 1px solid #fde68a; }
.info-banner--error { background: #fef2f2; color: #991b1b; border: 1px solid #fecaca; }
.info-banner--success { background: #f0fdf4; color: #166534; border: 1px solid #bbf7d0; }

/* -- STREAMLIT OVERRIDES -------------------------------------- */
[data-testid="stMetric"] {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 16px 20px;
}
[data-testid="stMetric"] label {
    font-family: 'Inter', sans-serif !important;
    font-size: 0.7rem !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: #94a3b8 !important;
}
[data-testid="stMetric"] [data-testid="stMetricValue"] {
    font-family: 'JetBrains Mono', monospace !important;
    font-weight: 700;
    font-size: 1.4rem !important;
}
[data-testid="stDataFrame"] {
    border-radius: 10px;
    border: 1px solid #e2e8f0;
    overflow: hidden;
}
[data-testid="stExpander"] {
    border: 1px solid #e2e8f0 !important;
    border-radius: 12px !important;
    margin-bottom: 8px;
}
[data-testid="stExpander"] summary {
    font-weight: 600 !important;
    font-size: 0.875rem !important;
}

/* Main area buttons */
.main div.stButton > button {
    border-radius: 8px;
    font-weight: 500;
    font-family: 'Inter', sans-serif;
    font-size: 0.85rem;
    transition: all 0.15s ease;
    border: 1px solid #e2e8f0;
    background: #ffffff;
    color: #334155;
    padding: 8px 16px;
}
.main div.stButton > button:hover {
    background: #f8fafc;
    border-color: #cbd5e1;
}

/* Chat bar styling */
[data-testid="stChatInput"] {
    border: 1px solid #e2e8f0 !important;
    border-radius: 12px !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.04) !important;
}

/* Tab styling */
[data-testid="stTab"] {
    font-weight: 500 !important;
    font-size: 0.85rem !important;
}

/* Footer */
.footer {
    text-align: center;
    padding: 16px 0;
    margin-top: 32px;
    font-size: 0.75rem;
    color: #94a3b8;
    border-top: 1px solid #f1f5f9;
}

/* -- MATERIAL ICONS FONT (needed for sidebar toggle) ----------------- */
@import url('https://fonts.googleapis.com/icon?family=Material+Symbols+Rounded');

/* -- FIX SIDEBAR TOGGLE BUTTON TEXT ---------------------------------- */
/* When Material Icons font fails to load, icon name shows as plain text.
   We target ALL sidebar toggle buttons by their data-testid attributes. */

/* Hide ALL Material Icon text leaks throughout the app */
[data-testid="stIconMaterial"] {
    font-size: 0px !important;
    color: transparent !important;
    overflow: hidden !important;
    width: 0px !important;
    height: 0px !important;
    display: none !important;
}

/* Expand sidebar button (appears in header when sidebar is collapsed) */
[data-testid="stExpandSidebarButton"] {
    overflow: hidden !important;
    width: 32px !important;
    height: 32px !important;
    padding: 0 !important;
    font-size: 0px !important;
    color: transparent !important;
    position: relative !important;
}
[data-testid="stExpandSidebarButton"]::after {
    content: "\\203A" !important;
    font-size: 1.5rem !important;
    font-weight: 700 !important;
    color: #64748b !important;
    position: absolute !important;
    top: 50% !important;
    left: 50% !important;
    transform: translate(-50%, -50%) !important;
    font-family: 'Inter', sans-serif !important;
}

/* Collapse sidebar button (appears in sidebar header) */
[data-testid="stSidebarCollapseButton"] {
    overflow: hidden !important;
    max-width: 36px !important;
    max-height: 36px !important;
}
[data-testid="stSidebarCollapseButton"] button {
    overflow: hidden !important;
    width: 32px !important;
    height: 32px !important;
    padding: 0 !important;
    font-size: 0px !important;
    color: transparent !important;
    position: relative !important;
}
[data-testid="stSidebarCollapseButton"] button::after {
    content: "\\2039" !important;
    font-size: 1.5rem !important;
    font-weight: 700 !important;
    color: #64748b !important;
    position: absolute !important;
    top: 50% !important;
    left: 50% !important;
    transform: translate(-50%, -50%) !important;
    font-family: 'Inter', sans-serif !important;
}

/* Legacy collapsed control */
[data-testid="collapsedControl"] {
    overflow: hidden !important;
    max-width: 36px !important;
    max-height: 36px !important;
}
[data-testid="collapsedControl"] button {
    overflow: hidden !important;
    width: 32px !important;
    height: 32px !important;
    padding: 0 !important;
    font-size: 0px !important;
    color: transparent !important;
    position: relative !important;
}

/* Expander toggle arrows (keyboard_arrow_right) */
[data-testid="stExpander"] [data-testid="stIconMaterial"] {
    display: none !important;
}

/* Scrollbar */
::-webkit-scrollbar { width: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: #94a3b8; }

/* ================================================================
   LEGACY COMPAT -- keep old class names working
   ================================================================ */
.slv-header { background: transparent; color: #0f172a; padding: 0; border-radius: 0; margin-bottom: 1.5rem; border-bottom: 1px solid #f1f5f9; padding-bottom: 1rem; }
.slv-header h1 { color: #0f172a; font-size: 1.75rem; font-weight: 700; margin: 0 0 0.25rem 0; }
.slv-header p { color: #64748b; font-size: 0.875rem; margin: 0; }
.slv-section-header { font-size: 0.875rem; font-weight: 700; color: #0f172a; border-bottom: 1px solid #e2e8f0; padding-bottom: 0.5rem; margin: 24px 0 12px 0; }
.slv-metric-card { background: #ffffff; border: 1px solid #e2e8f0; border-radius: 12px; padding: 20px 24px; text-align: left; }
.slv-metric-card .label { font-size: 0.7rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.05em; font-weight: 600; }
.slv-metric-card .value { font-size: 1.5rem; font-weight: 700; color: #0f172a; margin: 4px 0; font-family: 'JetBrains Mono', monospace; }
.slv-metric-card .sub { font-size: 0.75rem; color: #64748b; }
.slv-metric-card .value.green { color: #059669; }
.slv-metric-card .value.red { color: #dc2626; }
.slv-metric-card .value.blue { color: #2563eb; }
.slv-metric-card .value.purple { color: #7c3aed; }
.slv-metric-card--green { border-top: 3px solid #059669; }
.slv-metric-card--red { border-top: 3px solid #dc2626; }
.slv-metric-card--blue { border-top: 3px solid #2563eb; }
.slv-metric-card--purple { border-top: 3px solid #7c3aed; }
.slv-card { background: white; border: 1px solid #e2e8f0; border-radius: 12px; padding: 20px 24px; margin-bottom: 16px; }
.slv-card h3 { color: #0f172a; font-size: 0.875rem; font-weight: 600; border-bottom: 1px solid #f1f5f9; padding-bottom: 0.5rem; margin-bottom: 0.8rem; text-transform: uppercase; letter-spacing: 0.03em; color: #475569; }
.slv-group-card { background: #ffffff; border-radius: 8px; padding: 10px 16px; margin: 2px 0; border: 1px solid transparent; cursor: pointer; transition: all 0.15s ease; display: flex; justify-content: space-between; align-items: center; }
.slv-group-card:hover { background: #f8fafc; border-color: #e2e8f0; }
.slv-group-card--income { border-left: 3px solid #059669; }
.slv-group-card--expense { border-left: 3px solid #dc2626; }
.slv-group-card--asset { border-left: 3px solid #2563eb; }
.slv-group-card--liability { border-left: 3px solid #7c3aed; }
.slv-group-card .name { font-weight: 500; color: #1e293b; font-size: 0.875rem; }
.slv-group-card .amount { font-family: 'JetBrains Mono', monospace; font-weight: 600; color: #475569; font-size: 0.875rem; }
.slv-group-card .count { font-size: 0.7rem; color: #94a3b8; margin-left: 8px; }
.slv-breadcrumb { font-size: 0.8rem; color: #94a3b8; padding: 8px 0; margin-bottom: 1rem; }
.slv-breadcrumb .sep { color: #94a3b8; margin: 0 6px; }
.slv-breadcrumb .current { color: #0f172a; font-weight: 600; }
.slv-amount { font-family: 'JetBrains Mono', monospace; font-weight: 600; }
.slv-amount--positive { color: #059669; }
.slv-amount--negative { color: #dc2626; }
.slv-voucher-box { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 10px; padding: 1.2rem; margin-bottom: 1rem; }
.slv-footer { text-align: center; padding: 16px 0; margin-top: 32px; font-size: 0.75rem; color: #94a3b8; border-top: 1px solid #f1f5f9; }
"""
    st.markdown(f'<style>{css}</style>', unsafe_allow_html=True)


# -- COMPONENT HELPERS --------------------------------------------------------

def page_header(title, subtitle=""):
    """Clean page header with optional subtitle."""
    sub_html = f'<p class="subtitle">{subtitle}</p>' if subtitle else ""
    st.markdown(f'''
    <div class="page-header">
        <h1>{title}</h1>
        {sub_html}
    </div>
    ''', unsafe_allow_html=True)


def section_header(text):
    """Render a clean section divider."""
    st.markdown(f'<div class="section-header">{text}</div>', unsafe_allow_html=True)


def metric_card(label, value, sub="", color_class=""):
    """Render a styled metric card. color_class: green, red, blue, amber, purple."""
    variant = f" metric-card--{color_class}" if color_class else ""
    val_class = f" {color_class}" if color_class else ""
    sub_html = f'<p class="metric-sub">{sub}</p>' if sub else ""
    st.markdown(f'''
    <div class="metric-card{variant}">
        <p class="metric-label">{label}</p>
        <p class="metric-value{val_class}">{value}</p>
        {sub_html}
    </div>
    ''', unsafe_allow_html=True)


def card_start(title=""):
    """Start a card container. Returns HTML string."""
    header = f'<div class="card-header">{title}</div>' if title else ""
    return f'<div class="card">{header}'


def card_end():
    """Close a card container."""
    return '</div>'


def badge(text, color="gray"):
    """Return HTML for a colored badge pill. Colors: green, red, amber, blue, gray, purple."""
    return f'<span class="badge badge-{color}">{text}</span>'


def breadcrumb_html(parts):
    """Render breadcrumb navigation. Last item is current."""
    if not parts:
        return
    items = []
    for i, p in enumerate(parts):
        if i == len(parts) - 1:
            items.append(f'<span class="current">{p}</span>')
        else:
            items.append(f'<span>{p}</span>')
    sep = '<span class="sep">&#8250;</span>'
    st.markdown(f'<div class="breadcrumb">{sep.join(items)}</div>', unsafe_allow_html=True)


def amount_span(amount, show_sign=False):
    """Return colored amount HTML span."""
    if amount is None:
        return '<span class="amt amt-muted">0</span>'
    cls = "amt-pos" if amount >= 0 else "amt-neg"
    formatted = fmt(amount)
    if show_sign and amount < 0:
        formatted = f"-{formatted}"
    return f'<span class="amt {cls}">{formatted}</span>'


def empty_state(title, description="", icon=""):
    """Render a centered empty state message."""
    icon_html = f'<div class="icon">{icon}</div>' if icon else ""
    desc_html = f'<p class="desc">{description}</p>' if description else ""
    st.markdown(f'''
    <div class="empty-state">
        {icon_html}
        <p class="title">{title}</p>
        {desc_html}
    </div>
    ''', unsafe_allow_html=True)


def info_banner(text, banner_type="info"):
    """Render an info/warning/error/success banner. Types: info, warning, error, success."""
    icons = {"info": "i", "warning": "!", "error": "x", "success": "ok"}
    st.markdown(f'''
    <div class="info-banner info-banner--{banner_type}">
        <span>{text}</span>
    </div>
    ''', unsafe_allow_html=True)


def footer(company_name=""):
    """Render page footer."""
    extra = f" | {company_name}" if company_name else ""
    st.markdown(f'''
    <div class="footer">
        Seven Labs Vision{extra}
    </div>
    ''', unsafe_allow_html=True)


def sidebar_company_card(name, entity_type="", business_nature="", complexity=""):
    """Render company info card in sidebar."""
    tags = []
    if entity_type:
        tags.append(f'<span class="tag tag-blue">{entity_type}</span>')
    if business_nature:
        tags.append(f'<span class="tag tag-green">{business_nature}</span>')
    if complexity:
        tags.append(f'<span class="tag tag-amber">{complexity}</span>')
    tags_html = ''.join(tags)
    st.sidebar.markdown(f'''
    <div class="sidebar-company-card">
        <p class="company-name">{name}</p>
        <div class="company-tags">{tags_html}</div>
    </div>
    ''', unsafe_allow_html=True)


def sidebar_section_label(text):
    """Render a section label in the sidebar (e.g., DASHBOARDS, COMPLIANCE)."""
    st.sidebar.markdown(f'<p class="sidebar-section-label">{text}</p>', unsafe_allow_html=True)
/* UI fix 1774135560 */
