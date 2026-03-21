"""
Seven Labs Vision — Shared Design System
Unified CSS, component helpers, and formatters for all pages.
"""

import streamlit as st


# ── FORMATTERS ───────────────────────────────────────────────────────────────

def fmt(amount):
    """Format amount in Indian numbering with sign."""
    if amount is None:
        return "₹0"
    abs_amt = abs(amount)
    if abs_amt >= 10000000:
        return f"₹{abs_amt/10000000:,.2f} Cr"
    elif abs_amt >= 100000:
        return f"₹{abs_amt/100000:,.2f} L"
    elif abs_amt >= 1000:
        return f"₹{abs_amt:,.0f}"
    else:
        return f"₹{abs_amt:,.2f}"


def fmt_full(amount):
    """Full formatted amount with decimals."""
    if amount is None:
        return "₹0.00"
    return f"₹{abs(amount):,.2f}"


# ── CSS INJECTION ────────────────────────────────────────────────────────────

def inject_base_styles():
    """Inject the unified design system CSS. Call once per page."""
    css = """
.block-container { padding-top: 1rem; }
html, body, [class*="st-"] { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; }
.slv-header { background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #334155 100%); color: white; padding: 1.5rem 2rem; border-radius: 12px; margin-bottom: 1.5rem; }
.slv-header h1 { color: #f8fafc; font-size: 1.6rem; font-weight: 700; margin: 0 0 0.25rem 0; font-family: 'Inter', sans-serif; }
.slv-header p { color: #94a3b8; font-size: 0.9rem; margin: 0; }
.slv-section-header { font-size: 1.1rem; font-weight: 700; color: #1e293b; border-bottom: 2px solid #3b82f6; padding-bottom: 0.5rem; margin: 2rem 0 1rem 0; font-family: 'Inter', sans-serif; }
.slv-metric-card { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 12px; padding: 1rem 1.2rem; text-align: center; transition: transform 0.2s, box-shadow 0.2s; }
.slv-metric-card:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(0,0,0,0.08); }
.slv-metric-card .label { font-size: 0.72rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; font-weight: 600; font-family: 'Inter', sans-serif; }
.slv-metric-card .value { font-size: 1.4rem; font-weight: 700; color: #1e293b; margin: 0.2rem 0; font-family: 'JetBrains Mono', monospace; }
.slv-metric-card .sub { font-size: 0.78rem; color: #94a3b8; }
.slv-metric-card .value.green { color: #059669; }
.slv-metric-card .value.red { color: #dc2626; }
.slv-metric-card .value.blue { color: #3b82f6; }
.slv-metric-card .value.purple { color: #7c3aed; }
.slv-metric-card--green { border-left: 3px solid #059669; }
.slv-metric-card--red { border-left: 3px solid #dc2626; }
.slv-metric-card--blue { border-left: 3px solid #3b82f6; }
.slv-metric-card--purple { border-left: 3px solid #7c3aed; }
.slv-card { background: white; border: 1px solid #e2e8f0; border-radius: 12px; padding: 1.2rem 1.5rem; margin-bottom: 1rem; box-shadow: 0 1px 3px rgba(0,0,0,0.04); }
.slv-card h3 { color: #1e293b; font-size: 1rem; font-weight: 700; border-bottom: 2px solid #e2e8f0; padding-bottom: 0.5rem; margin-bottom: 0.8rem; font-family: 'Inter', sans-serif; }
.slv-group-card { background: #f8fafc; border-radius: 10px; padding: 12px 16px; margin: 4px 0; border-left: 4px solid #3b82f6; cursor: pointer; transition: all 0.2s; display: flex; justify-content: space-between; align-items: center; }
.slv-group-card:hover { background: #eff6ff; transform: translateX(4px); box-shadow: 0 2px 8px rgba(59,130,246,0.12); }
.slv-group-card--income { border-left-color: #059669; }
.slv-group-card--income:hover { background: #ecfdf5; }
.slv-group-card--expense { border-left-color: #dc2626; }
.slv-group-card--expense:hover { background: #fef2f2; }
.slv-group-card--asset { border-left-color: #3b82f6; }
.slv-group-card--liability { border-left-color: #7c3aed; }
.slv-group-card--liability:hover { background: #f5f3ff; }
.slv-group-card .name { font-weight: 600; color: #1e293b; font-size: 0.9rem; }
.slv-group-card .amount { font-family: 'JetBrains Mono', monospace; font-weight: 600; color: #475569; font-size: 0.9rem; }
.slv-group-card .count { font-size: 0.75rem; color: #94a3b8; margin-left: 8px; }
.slv-breadcrumb { font-size: 0.85rem; color: #64748b; padding: 10px 16px; background: #f1f5f9; border-radius: 8px; margin-bottom: 1rem; font-family: 'Inter', sans-serif; }
.slv-breadcrumb .sep { color: #94a3b8; margin: 0 6px; }
.slv-breadcrumb .current { color: #1e293b; font-weight: 600; }
table.slv-table { width: 100%; border-collapse: collapse; font-size: 0.85rem; font-family: 'Inter', sans-serif; }
table.slv-table th { background: #0f172a; color: #f8fafc; padding: 10px 12px; font-weight: 600; text-align: right; font-size: 0.8rem; }
table.slv-table th:first-child { text-align: left; }
table.slv-table td { padding: 8px 12px; text-align: right; border-bottom: 1px solid #f1f5f9; color: #334155; font-size: 0.82rem; }
table.slv-table td:first-child { text-align: left; }
table.slv-table tr:nth-child(even) { background: #f8fafc; }
table.slv-table tr:hover { background: #eff6ff; }
table.slv-table tr.total-row td { font-weight: 700; border-top: 2px solid #1e293b; background: #f1f5f9; color: #0f172a; }
table.slv-table--light th { background: #f1f5f9; color: #475569; border-bottom: 2px solid #e2e8f0; }
.slv-amount { font-family: 'JetBrains Mono', monospace; font-weight: 600; }
.slv-amount--positive { color: #059669; }
.slv-amount--negative { color: #dc2626; }
section[data-testid="stSidebar"] { background: #0f172a; }
section[data-testid="stSidebar"] * { color: #e2e8f0 !important; }
section[data-testid="stSidebar"] .stMarkdown h3 { color: #f8fafc !important; font-family: 'Inter', sans-serif; }
section[data-testid="stSidebar"] hr { border-color: #1e293b !important; }
section[data-testid="stSidebar"] .stButton > button { background: transparent !important; color: #e2e8f0 !important; border: 1px solid #1e293b !important; text-align: left !important; transition: all 0.2s; }
section[data-testid="stSidebar"] .stButton > button:hover { background: rgba(59,130,246,0.15) !important; border-color: #3b82f6 !important; }
section[data-testid="stSidebar"] input { background: #1e293b !important; border-color: #334155 !important; color: #f8fafc !important; }
section[data-testid="stSidebar"] .stCaption p { color: #64748b !important; }
div.stButton > button { border-radius: 8px; font-weight: 500; font-family: 'Inter', sans-serif; transition: all 0.15s; font-size: 0.88rem; }
.main div.stButton > button { width: 100%; text-align: left; background: white; border: 1px solid #e2e8f0; border-radius: 8px; padding: 8px 16px; margin: 2px 0; color: #1e293b; }
.main div.stButton > button:hover { background: #eff6ff; border-color: #3b82f6; transform: translateX(3px); }
[data-testid="stMetric"] { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 12px; padding: 0.8rem 1rem; }
[data-testid="stMetric"] label { font-family: 'Inter', sans-serif; font-size: 0.75rem !important; text-transform: uppercase; letter-spacing: 0.04em; color: #64748b !important; }
[data-testid="stMetric"] [data-testid="stMetricValue"] { font-family: 'JetBrains Mono', monospace; font-weight: 700; }
[data-testid="stDataFrame"] { border-radius: 10px; border: 1px solid #e2e8f0; overflow: hidden; }
.slv-voucher-box { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 10px; padding: 1.2rem; margin-bottom: 1rem; }
.slv-footer { background: #f1f5f9; border-radius: 8px; padding: 1rem; margin-top: 1rem; font-size: 0.75rem; color: #64748b; text-align: center; font-family: 'Inter', sans-serif; }
"""
    st.markdown(
        '<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">'
        f'<style>{css}</style>',
        unsafe_allow_html=True,
    )


# ── COMPONENT HELPERS ────────────────────────────────────────────────────────

def page_header(title, subtitle=""):
    """Render a gradient page header."""
    sub_html = f"<p>{subtitle}</p>" if subtitle else ""
    st.markdown(f"""
    <div class="slv-header">
        <h1>{title}</h1>
        {sub_html}
    </div>
    """, unsafe_allow_html=True)


def section_header(text):
    """Render a styled section divider."""
    st.markdown(f'<div class="slv-section-header">{text}</div>', unsafe_allow_html=True)


def metric_card(label, value, sub="", color_class=""):
    """Render a styled metric card. color_class: green, red, blue, purple."""
    variant = f" slv-metric-card--{color_class}" if color_class else ""
    val_class = f" {color_class}" if color_class else ""
    sub_html = f'<div class="sub">{sub}</div>' if sub else ""
    st.markdown(f"""
    <div class="slv-metric-card{variant}">
        <div class="label">{label}</div>
        <div class="value{val_class}">{value}</div>
        {sub_html}
    </div>
    """, unsafe_allow_html=True)


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
    sep = '<span class="sep">›</span>'
    st.markdown(f'<div class="slv-breadcrumb">{sep.join(items)}</div>', unsafe_allow_html=True)


def amount_span(amount, show_sign=False):
    """Return colored amount HTML span."""
    if amount is None:
        return '<span class="slv-amount">₹0</span>'
    cls = "slv-amount--positive" if amount >= 0 else "slv-amount--negative"
    formatted = fmt(amount)
    if show_sign and amount < 0:
        formatted = f"-{formatted}"
    return f'<span class="slv-amount {cls}">{formatted}</span>'
