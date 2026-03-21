"""
GST Returns Dashboard -- SLV
Fully interactive: every party name, invoice, and month is clickable.
Drill-down: GSTR-3B -> Invoice List -> Voucher Detail / Party Ledger.
"""

import streamlit as st
import sqlite3
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from gst_engine import (
    get_conn, get_available_months, format_indian,
    gstr1_b2b_invoices, gstr1_b2c_invoices, gstr1_credit_notes,
    gstr1_hsn_summary, gstr1_monthly_summary,
    input_tax_invoices, input_tax_debit_notes, input_tax_monthly_summary,
    gstr3b_summary, gst_monthly_comparison,
    _get_company_gstin, _get_company_state, DB_PATH,
)
from tally_reports import ledger_detail

st.set_page_config(page_title="GST Returns \u2014 SLV", page_icon="\U0001f9fe", layout="wide")

# ======================================================================
#  SESSION STATE DEFAULTS
# ======================================================================

_defaults = {
    "gst_view": "summary",
    "gst_month": "202601",
    "gst_voucher_guid": None,
    "gst_party_name": None,
    "gst_drill_type": None,
    "gst_drill_vchno": None,
    "gst_back_view": None,
    "gst_back_drill": None,
}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ======================================================================
#  CSS
# ======================================================================

st.markdown("""
<style>
    .gst-header {
        background: linear-gradient(135deg, #0a1628 0%, #1a365d 100%);
        color: white;
        padding: 1.2rem 1.8rem;
        border-radius: 12px;
        margin-bottom: 1rem;
    }
    .gst-header h1 { color: white; margin: 0; font-size: 1.6rem; }
    .gst-header p { color: #94a3b8; margin: 0.2rem 0 0 0; font-size: 0.9rem; }

    .metric-card {
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 1rem 1.2rem;
        text-align: center;
    }
    .metric-card .label { color: #64748b; font-size: 0.8rem; text-transform: uppercase; }
    .metric-card .value { color: #1e293b; font-size: 1.3rem; font-weight: 700; }
    .metric-card .value.green { color: #16a34a; }
    .metric-card .value.red { color: #dc2626; }
    .metric-card .value.blue { color: #2563eb; }

    .section-3b {
        background: white;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 1.2rem;
        margin-bottom: 1rem;
    }
    .section-3b h3 {
        color: #1e3a5f;
        font-size: 1rem;
        border-bottom: 2px solid #e2e8f0;
        padding-bottom: 0.5rem;
        margin-bottom: 0.8rem;
    }

    table.gst-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 0.85rem;
    }
    table.gst-table th {
        background: #f1f5f9;
        color: #475569;
        padding: 0.5rem 0.8rem;
        text-align: right;
        font-weight: 600;
        border-bottom: 2px solid #e2e8f0;
    }
    table.gst-table th:first-child { text-align: left; }
    table.gst-table td {
        padding: 0.5rem 0.8rem;
        text-align: right;
        border-bottom: 1px solid #f1f5f9;
        color: #334155;
    }
    table.gst-table td:first-child { text-align: left; }
    table.gst-table tr.total-row td {
        font-weight: 700;
        border-top: 2px solid #1e3a5f;
        color: #1e3a5f;
    }
    table.gst-table tr:hover { background: #f8fafc; }

    .inv-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 0.82rem;
    }
    .inv-table th {
        background: #1e3a5f;
        color: white;
        padding: 0.5rem 0.6rem;
        text-align: right;
        font-weight: 600;
    }
    .inv-table th:first-child, .inv-table th:nth-child(2), .inv-table th:nth-child(3), .inv-table th:nth-child(4) {
        text-align: left;
    }
    .inv-table td {
        padding: 0.45rem 0.6rem;
        text-align: right;
        border-bottom: 1px solid #e2e8f0;
        color: #334155;
    }
    .inv-table td:first-child, .inv-table td:nth-child(2), .inv-table td:nth-child(3), .inv-table td:nth-child(4) {
        text-align: left;
    }
    .inv-table tr:hover { background: #eff6ff; }
    .inv-table tr.total-row td {
        font-weight: 700;
        border-top: 2px solid #1e3a5f;
        background: #f1f5f9;
    }

    .voucher-box {
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 1.2rem;
        margin-bottom: 1rem;
    }

    div.stButton > button {
        padding: 0.1rem 0.3rem;
        font-size: 0.8rem;
        min-height: 0;
        line-height: 1.2;
    }
</style>
""", unsafe_allow_html=True)

# ======================================================================
#  HELPERS
# ======================================================================

def fi(amount):
    """Format Indian number."""
    return format_indian(amount)


def _nav(view, **kwargs):
    """Navigate to a view, storing kwargs in session state."""
    st.session_state.gst_view = view
    for k, v in kwargs.items():
        st.session_state[k] = v


def _format_date_display(d):
    """YYYYMMDD -> DD-Mon-YYYY"""
    if d and len(d) >= 8:
        month_names = {
            "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
            "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
            "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec",
        }
        return f"{d[6:8]}-{month_names.get(d[4:6], d[4:6])}-{d[0:4]}"
    return d or ""


def _month_label(m):
    """YYYYMM -> Apr'25 style label."""
    mn = {"01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
          "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
          "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec"}
    if m and len(m) == 6:
        return f"{mn.get(m[4:6], m[4:6])}'{m[2:4]}"
    return m or ""


def _month_label_full(m):
    """YYYYMM -> April 2025 style label."""
    mn = {"01": "January", "02": "February", "03": "March", "04": "April",
          "05": "May", "06": "June", "07": "July", "08": "August",
          "09": "September", "10": "October", "11": "November", "12": "December"}
    if m and len(m) == 6:
        return f"{mn.get(m[4:6], m[4:6])} {m[0:4]}"
    return m or ""


# ======================================================================
#  HEADER
# ======================================================================

conn = get_conn()
months = get_available_months(conn)
month_codes = [m[0] for m in months]

_company_gstin = _get_company_gstin(conn)
_company_state = _get_company_state(conn)

st.markdown(f"""
<div class="gst-header">
    <h1>GST Returns Dashboard</h1>
    <p>GSTIN: {_company_gstin or 'N/A'} &nbsp;|&nbsp; State: {_company_state or 'N/A'}</p>
</div>
""", unsafe_allow_html=True)

# ======================================================================
#  MONTH SELECTOR BAR
# ======================================================================

st.markdown("##### Select Month")
month_cols = st.columns(len(month_codes))
for i, mc in enumerate(month_codes):
    label = _month_label(mc)
    is_active = st.session_state.gst_month == mc
    with month_cols[i]:
        if st.button(
            f"**{label}**" if is_active else label,
            key=f"month_{mc}",
            use_container_width=True,
            type="primary" if is_active else "secondary",
        ):
            st.session_state.gst_month = mc
            st.session_state.gst_view = "summary"
            st.session_state.gst_drill_type = None
            st.rerun()

sel_month = st.session_state.gst_month
st.markdown(f"---")

# ======================================================================
#  ROUTING
# ======================================================================

view = st.session_state.gst_view

if view == "summary":
    # ──────────────────────────────────────────────────────────────────
    #  GSTR-3B SUMMARY
    # ──────────────────────────────────────────────────────────────────
    data = gstr3b_summary(conn, sel_month)
    s31 = data["section_3_1"]
    s4 = data["section_4"]
    s61 = data["section_6_1"]

    st.subheader(f"GSTR-3B  |  {_month_label_full(sel_month)}")

    # Top metrics
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.markdown(f"""<div class="metric-card">
            <div class="label">Output Tax</div>
            <div class="value red">{fi(s31['net_total_tax'])}</div>
        </div>""", unsafe_allow_html=True)
    with m2:
        st.markdown(f"""<div class="metric-card">
            <div class="label">Input Tax Credit</div>
            <div class="value green">{fi(s4['net_itc_total'])}</div>
        </div>""", unsafe_allow_html=True)
    with m3:
        st.markdown(f"""<div class="metric-card">
            <div class="label">Net Payable</div>
            <div class="value blue">{fi(s61['total_payable'])}</div>
        </div>""", unsafe_allow_html=True)
    with m4:
        st.markdown(f"""<div class="metric-card">
            <div class="label">Taxable Turnover</div>
            <div class="value">{fi(s31['net_taxable'])}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("")

    # ── Section 3.1 Outward Supplies ──
    st.markdown("""<div class="section-3b"><h3>3.1 Outward Supplies</h3>""", unsafe_allow_html=True)
    html_31 = """<table class="gst-table">
    <tr><th>Nature of Supplies</th><th>Taxable Value</th><th>IGST</th><th>CGST</th><th>SGST</th><th>Total Tax</th></tr>"""
    html_31 += f"""<tr><td>Outward taxable supplies (other than zero/nil/exempted)</td>
        <td>{fi(s31['a_taxable'])}</td><td>{fi(s31['a_igst'])}</td>
        <td>{fi(s31['a_cgst'])}</td><td>{fi(s31['a_sgst'])}</td><td>{fi(s31['a_total_tax'])}</td></tr>"""
    html_31 += f"""<tr><td>Less: Credit Notes</td>
        <td>{fi(s31['cn_taxable'])}</td><td>{fi(s31['cn_igst'])}</td>
        <td>{fi(s31['cn_cgst'])}</td><td>{fi(s31['cn_sgst'])}</td>
        <td>{fi(round(s31['cn_cgst']+s31['cn_sgst']+s31['cn_igst'],2))}</td></tr>"""
    html_31 += f"""<tr class="total-row"><td>Net Outward Supplies</td>
        <td>{fi(s31['net_taxable'])}</td><td>{fi(s31['net_igst'])}</td>
        <td>{fi(s31['net_cgst'])}</td><td>{fi(s31['net_sgst'])}</td><td>{fi(s31['net_total_tax'])}</td></tr>"""
    html_31 += "</table></div>"
    st.markdown(html_31, unsafe_allow_html=True)

    # Drill-down buttons for 3.1
    st.markdown("**Drill into invoices:**")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        if st.button("B2B Sales Invoices", key="drill_b2b", use_container_width=True):
            _nav("b2b_invoices", gst_drill_type="b2b")
            st.rerun()
    with c2:
        if st.button("B2C Sales Invoices", key="drill_b2c", use_container_width=True):
            _nav("b2c_invoices", gst_drill_type="b2c")
            st.rerun()
    with c3:
        if st.button("Credit Notes", key="drill_cn", use_container_width=True):
            _nav("credit_notes", gst_drill_type="cn")
            st.rerun()
    with c4:
        if st.button("HSN Summary", key="drill_hsn", use_container_width=True):
            _nav("hsn_summary", gst_drill_type="hsn")
            st.rerun()

    st.markdown("")

    # ── Section 4 Eligible ITC ──
    st.markdown("""<div class="section-3b"><h3>4. Eligible ITC</h3>""", unsafe_allow_html=True)
    html_4 = """<table class="gst-table">
    <tr><th>Details</th><th>IGST</th><th>CGST</th><th>SGST</th><th>Total</th></tr>"""
    html_4 += f"""<tr><td>ITC Available (from purchases)</td>
        <td>{fi(s4['itc_igst'])}</td><td>{fi(s4['itc_cgst'])}</td>
        <td>{fi(s4['itc_sgst'])}</td><td>{fi(s4['itc_total'])}</td></tr>"""
    html_4 += f"""<tr><td>Less: ITC Reversed (Debit Notes)</td>
        <td>{fi(s4['reversal_igst'])}</td><td>{fi(s4['reversal_cgst'])}</td>
        <td>{fi(s4['reversal_sgst'])}</td><td>{fi(s4['reversal_total'])}</td></tr>"""
    html_4 += f"""<tr class="total-row"><td>Net ITC Available</td>
        <td>{fi(s4['net_itc_igst'])}</td><td>{fi(s4['net_itc_cgst'])}</td>
        <td>{fi(s4['net_itc_sgst'])}</td><td>{fi(s4['net_itc_total'])}</td></tr>"""
    html_4 += "</table></div>"
    st.markdown(html_4, unsafe_allow_html=True)

    c5, c6 = st.columns(2)
    with c5:
        if st.button("Purchase Invoices (ITC)", key="drill_itc", use_container_width=True):
            _nav("purchase_invoices", gst_drill_type="itc")
            st.rerun()
    with c6:
        if st.button("Debit Notes", key="drill_dn", use_container_width=True):
            _nav("debit_notes", gst_drill_type="dn")
            st.rerun()

    st.markdown("")

    # ── Section 6.1 Payment ──
    st.markdown("""<div class="section-3b"><h3>6.1 Payment of Tax</h3>""", unsafe_allow_html=True)
    html_6 = """<table class="gst-table">
    <tr><th>Description</th><th>IGST</th><th>CGST</th><th>SGST</th><th>Total</th></tr>"""
    html_6 += f"""<tr><td>Tax Liability</td>
        <td>{fi(s61['igst_liability'])}</td><td>{fi(s61['cgst_liability'])}</td>
        <td>{fi(s61['sgst_liability'])}</td><td>{fi(s61['total_liability'])}</td></tr>"""
    html_6 += f"""<tr><td>ITC Utilised</td>
        <td>{fi(s61['igst_itc_used'])}</td><td>{fi(s61['cgst_itc_used'])}</td>
        <td>{fi(s61['sgst_itc_used'])}</td><td>{fi(s61['total_itc_used'])}</td></tr>"""
    html_6 += f"""<tr class="total-row"><td>Tax Payable (Cash)</td>
        <td>{fi(s61['igst_payable'])}</td><td>{fi(s61['cgst_payable'])}</td>
        <td>{fi(s61['sgst_payable'])}</td><td>{fi(s61['total_payable'])}</td></tr>"""
    html_6 += "</table></div>"
    st.markdown(html_6, unsafe_allow_html=True)

    if s61.get("igst_credit_remaining", 0) > 0:
        st.info(f"IGST Credit remaining after cross-utilisation: {fi(s61['igst_credit_remaining'])}")

    st.markdown("")

    # ── Monthly Trends quick link ──
    if st.button("View Monthly Comparison (All Months)", key="drill_trends", use_container_width=True):
        _nav("monthly_trends")
        st.rerun()

# ══════════════════════════════════════════════════════════════════════
#  INVOICE LIST VIEWS
# ══════════════════════════════════════════════════════════════════════

elif view in ("b2b_invoices", "b2c_invoices", "credit_notes", "purchase_invoices", "debit_notes"):

    if st.button("\u2190 Back to GSTR-3B", key="back_to_3b"):
        _nav("summary")
        st.rerun()

    titles = {
        "b2b_invoices": "B2B Sales Invoices",
        "b2c_invoices": "B2C Sales Invoices",
        "credit_notes": "Credit Notes",
        "purchase_invoices": "Purchase Invoices (ITC)",
        "debit_notes": "Debit Notes",
    }
    st.subheader(f"{titles[view]}  |  {_month_label_full(sel_month)}")

    # Fetch data
    if view == "b2b_invoices":
        invoices = gstr1_b2b_invoices(conn, sel_month)
        party_key = "party"
        inv_key = "invoice_no"
    elif view == "b2c_invoices":
        invoices = gstr1_b2c_invoices(conn, sel_month)
        party_key = "party"
        inv_key = "invoice_no"
    elif view == "credit_notes":
        invoices = gstr1_credit_notes(conn, sel_month)
        party_key = "party"
        inv_key = "note_no"
    elif view == "purchase_invoices":
        invoices = input_tax_invoices(conn, sel_month)
        party_key = "supplier"
        inv_key = "invoice_no"
    elif view == "debit_notes":
        invoices = input_tax_debit_notes(conn, sel_month)
        party_key = "supplier"
        inv_key = "note_no"
    else:
        invoices = []
        party_key = "party"
        inv_key = "invoice_no"

    if not invoices:
        st.info("No invoices found for this month.")
    else:
        # Totals
        tot_taxable = sum(inv.get("taxable_value", 0) for inv in invoices)
        tot_cgst = sum(inv.get("cgst", 0) for inv in invoices)
        tot_sgst = sum(inv.get("sgst", 0) for inv in invoices)
        tot_igst = sum(inv.get("igst", 0) for inv in invoices)
        tot_total = sum(inv.get("total_tax", 0) for inv in invoices)
        tot_value = sum(inv.get("invoice_value", inv.get("note_value", 0)) for inv in invoices)

        st.markdown(f"**{len(invoices)} invoices** &nbsp;|&nbsp; Taxable: **{fi(tot_taxable)}** &nbsp;|&nbsp; Total Tax: **{fi(tot_total)}** &nbsp;|&nbsp; Invoice Value: **{fi(tot_value)}**")

        # Render each invoice as a row with clickable party and invoice number
        # Use columns to create table-like layout
        # Header
        hdr_cols = st.columns([0.7, 1.0, 2.0, 1.5, 1.2, 0.8, 0.8, 0.8, 1.2])
        headers = ["Date", "Invoice No", "Party Name", "GSTIN", "Taxable", "CGST", "SGST", "IGST", "Total"]
        for i, h in enumerate(headers):
            hdr_cols[i].markdown(f"**{h}**")

        st.markdown("---")

        for idx, inv in enumerate(invoices):
            cols = st.columns([0.7, 1.0, 2.0, 1.5, 1.2, 0.8, 0.8, 0.8, 1.2])

            cols[0].markdown(f"<small>{inv.get('date', '')}</small>", unsafe_allow_html=True)

            # Clickable invoice number -> voucher detail
            inv_no = inv.get(inv_key, "")
            with cols[1]:
                if st.button(f"{inv_no}", key=f"inv_{view}_{idx}"):
                    # Look up GUID for this invoice
                    guid_row = conn.execute(
                        "SELECT GUID FROM trn_voucher WHERE VOUCHERNUMBER = ? AND SUBSTR(DATE,1,6) = ? LIMIT 1",
                        (inv_no, sel_month)
                    ).fetchone()
                    if guid_row:
                        _nav("voucher_detail",
                             gst_voucher_guid=guid_row[0],
                             gst_drill_vchno=inv_no,
                             gst_back_view=view,
                             gst_back_drill=st.session_state.gst_drill_type)
                        st.rerun()

            # Clickable party name -> party ledger
            party = inv.get(party_key, "")
            with cols[2]:
                if st.button(f"{party}", key=f"party_{view}_{idx}"):
                    _nav("party_ledger",
                         gst_party_name=party,
                         gst_back_view=view,
                         gst_back_drill=st.session_state.gst_drill_type)
                    st.rerun()

            gstin = inv.get("gstin", "")
            cols[3].markdown(f"<small>{gstin}</small>", unsafe_allow_html=True)
            cols[4].markdown(f"<small style='text-align:right;display:block'>{fi(inv.get('taxable_value', 0))}</small>", unsafe_allow_html=True)
            cols[5].markdown(f"<small style='text-align:right;display:block'>{fi(inv.get('cgst', 0))}</small>", unsafe_allow_html=True)
            cols[6].markdown(f"<small style='text-align:right;display:block'>{fi(inv.get('sgst', 0))}</small>", unsafe_allow_html=True)
            cols[7].markdown(f"<small style='text-align:right;display:block'>{fi(inv.get('igst', 0))}</small>", unsafe_allow_html=True)
            val = inv.get("invoice_value", inv.get("note_value", inv.get("total_tax", 0)))
            cols[8].markdown(f"<small style='text-align:right;display:block'>**{fi(val)}**</small>", unsafe_allow_html=True)

        # Totals row
        st.markdown("---")
        tot_cols = st.columns([0.7, 1.0, 2.0, 1.5, 1.2, 0.8, 0.8, 0.8, 1.2])
        tot_cols[0].markdown("**TOTAL**")
        tot_cols[4].markdown(f"**{fi(tot_taxable)}**")
        tot_cols[5].markdown(f"**{fi(tot_cgst)}**")
        tot_cols[6].markdown(f"**{fi(tot_sgst)}**")
        tot_cols[7].markdown(f"**{fi(tot_igst)}**")
        tot_cols[8].markdown(f"**{fi(tot_value)}**")

# ══════════════════════════════════════════════════════════════════════
#  HSN SUMMARY VIEW
# ══════════════════════════════════════════════════════════════════════

elif view == "hsn_summary":
    if st.button("\u2190 Back to GSTR-3B", key="back_to_3b_hsn"):
        _nav("summary")
        st.rerun()

    st.subheader(f"HSN Summary  |  {_month_label_full(sel_month)}")

    hsn_data = gstr1_hsn_summary(conn, sel_month)
    if not hsn_data:
        st.info("No HSN data for this month.")
    else:
        html = """<table class="inv-table">
        <tr><th>HSN Code</th><th>Description</th><th>Rate %</th><th>Taxable Value</th><th>CGST</th><th>SGST</th><th>IGST</th><th>Total Tax</th></tr>"""
        for h in hsn_data:
            html += f"""<tr>
                <td>{h['hsn_code']}</td><td>{h['description']}</td><td>{h['gst_rate']}</td>
                <td style="text-align:right">{fi(h['taxable_value'])}</td>
                <td style="text-align:right">{fi(h['cgst'])}</td>
                <td style="text-align:right">{fi(h['sgst'])}</td>
                <td style="text-align:right">{fi(h['igst'])}</td>
                <td style="text-align:right">{fi(h['total_tax'])}</td></tr>"""
        # Total
        t_tax = sum(h['taxable_value'] for h in hsn_data)
        t_c = sum(h['cgst'] for h in hsn_data)
        t_s = sum(h['sgst'] for h in hsn_data)
        t_i = sum(h['igst'] for h in hsn_data)
        t_t = sum(h['total_tax'] for h in hsn_data)
        html += f"""<tr class="total-row"><td colspan="3">TOTAL</td>
            <td style="text-align:right">{fi(t_tax)}</td>
            <td style="text-align:right">{fi(t_c)}</td>
            <td style="text-align:right">{fi(t_s)}</td>
            <td style="text-align:right">{fi(t_i)}</td>
            <td style="text-align:right">{fi(t_t)}</td></tr>"""
        html += "</table>"
        st.markdown(html, unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════
#  VOUCHER DETAIL VIEW
# ══════════════════════════════════════════════════════════════════════

elif view == "voucher_detail":
    back_view = st.session_state.gst_back_view or "summary"
    back_label = {
        "b2b_invoices": "B2B Invoices",
        "b2c_invoices": "B2C Invoices",
        "credit_notes": "Credit Notes",
        "purchase_invoices": "Purchase Invoices",
        "debit_notes": "Debit Notes",
        "party_ledger": "Party Ledger",
    }.get(back_view, "GSTR-3B")

    if st.button(f"\u2190 Back to {back_label}", key="back_from_voucher"):
        _nav(back_view, gst_drill_type=st.session_state.gst_back_drill)
        st.rerun()

    guid = st.session_state.gst_voucher_guid
    vchno = st.session_state.gst_drill_vchno

    if not guid:
        st.warning("No voucher selected.")
    else:
        # Voucher header
        hdr = conn.execute("""
            SELECT DATE, VOUCHERNUMBER, VOUCHERTYPENAME, PARTYLEDGERNAME, NARRATION
            FROM trn_voucher WHERE GUID = ?
        """, (guid,)).fetchone()

        if hdr:
            date, num, vtype, party, narration = hdr
            st.subheader(f"Voucher Detail  |  {vtype} #{num}")

            st.markdown(f"""<div class="voucher-box">
                <b>Date:</b> {_format_date_display(date)} &nbsp;&nbsp;
                <b>Number:</b> {num} &nbsp;&nbsp;
                <b>Type:</b> {vtype} &nbsp;&nbsp;
                <b>Party:</b> {party or '-'} &nbsp;&nbsp;
                <b>Narration:</b> {narration or '-'}
            </div>""", unsafe_allow_html=True)

            # Party link
            if party:
                if st.button(f"Open Ledger: {party}", key="vch_party_link"):
                    _nav("party_ledger",
                         gst_party_name=party,
                         gst_back_view="voucher_detail",
                         gst_back_drill=st.session_state.gst_back_drill)
                    st.rerun()

            # Accounting entries
            entries = conn.execute("""
                SELECT LEDGERNAME, CAST(AMOUNT AS REAL) as amount, ISDEEMEDPOSITIVE
                FROM trn_accounting WHERE VOUCHER_GUID = ?
                AND (GSTTAXRATE IS NULL OR GSTTAXRATE = '')
            """, (guid,)).fetchall()

            st.markdown("**Accounting Entries:**")

            html_e = """<table class="inv-table">
            <tr><th>Ledger Name</th><th>Debit</th><th>Credit</th></tr>"""
            total_dr = 0
            total_cr = 0
            for ledger, amt, deemed in entries:
                amt = float(amt) if amt else 0
                if deemed == "Yes":
                    debit = abs(amt) if amt < 0 else 0
                    credit = abs(amt) if amt > 0 else 0
                else:
                    debit = abs(amt) if amt > 0 else 0
                    credit = abs(amt) if amt < 0 else 0

                # Simpler: negative = debit, positive = credit in Tally
                debit = abs(amt) if amt < 0 else 0
                credit = abs(amt) if amt > 0 else 0

                total_dr += debit
                total_cr += credit
                dr_str = fi(debit) if debit else ""
                cr_str = fi(credit) if credit else ""
                html_e += f"""<tr><td>{ledger}</td>
                    <td style="text-align:right">{dr_str}</td>
                    <td style="text-align:right">{cr_str}</td></tr>"""

            html_e += f"""<tr class="total-row"><td>TOTAL</td>
                <td style="text-align:right">{fi(total_dr)}</td>
                <td style="text-align:right">{fi(total_cr)}</td></tr>"""
            html_e += "</table>"
            st.markdown(html_e, unsafe_allow_html=True)
        else:
            st.error("Voucher not found.")

# ══════════════════════════════════════════════════════════════════════
#  PARTY LEDGER VIEW
# ══════════════════════════════════════════════════════════════════════

elif view == "party_ledger":
    back_view = st.session_state.gst_back_view or "summary"
    back_label = {
        "b2b_invoices": "B2B Invoices",
        "b2c_invoices": "B2C Invoices",
        "credit_notes": "Credit Notes",
        "purchase_invoices": "Purchase Invoices",
        "debit_notes": "Debit Notes",
        "voucher_detail": "Voucher Detail",
    }.get(back_view, "GSTR-3B")

    if st.button(f"\u2190 Back to {back_label}", key="back_from_ledger"):
        _nav(back_view, gst_drill_type=st.session_state.gst_back_drill)
        st.rerun()

    party = st.session_state.gst_party_name
    if not party:
        st.warning("No party selected.")
    else:
        st.subheader(f"Ledger: {party}")

        opening, txns, closing = ledger_detail(conn, party)

        # Opening
        ob_type = "Dr" if opening < 0 else "Cr"
        st.markdown(f"""<div class="voucher-box">
            <b>Opening Balance:</b> {fi(abs(opening))} {ob_type} &nbsp;&nbsp;|&nbsp;&nbsp;
            <b>Transactions:</b> {len(txns)} &nbsp;&nbsp;|&nbsp;&nbsp;
            <b>Closing Balance:</b> {fi(abs(closing))} {"Dr" if closing < 0 else "Cr"}
        </div>""", unsafe_allow_html=True)

        if not txns:
            st.info("No transactions found for this party.")
        else:
            # Header
            hdr_cols = st.columns([0.8, 1.0, 0.8, 1.2, 1.2, 1.2])
            for i, h in enumerate(["Date", "Vch Type", "Vch No", "Debit", "Credit", "Balance"]):
                hdr_cols[i].markdown(f"**{h}**")
            st.markdown("---")

            for idx, t in enumerate(txns):
                cols = st.columns([0.8, 1.0, 0.8, 1.2, 1.2, 1.2])
                cols[0].markdown(f"<small>{_format_date_display(t['date'])}</small>", unsafe_allow_html=True)
                cols[1].markdown(f"<small>{t['voucher_type']}</small>", unsafe_allow_html=True)

                # Clickable voucher number
                vnum = t['voucher_number']
                with cols[2]:
                    if vnum:
                        if st.button(f"{vnum}", key=f"ldg_vch_{idx}"):
                            guid_row = conn.execute(
                                "SELECT GUID FROM trn_voucher WHERE VOUCHERNUMBER = ? LIMIT 1",
                                (vnum,)
                            ).fetchone()
                            if guid_row:
                                _nav("voucher_detail",
                                     gst_voucher_guid=guid_row[0],
                                     gst_drill_vchno=vnum,
                                     gst_back_view="party_ledger",
                                     gst_back_drill=st.session_state.gst_back_drill)
                                st.rerun()
                    else:
                        cols[2].markdown("-")

                dr_str = fi(t['debit']) if t['debit'] else ""
                cr_str = fi(t['credit']) if t['credit'] else ""
                bal = t['balance']
                bal_str = f"{fi(abs(bal))} {'Dr' if bal < 0 else 'Cr'}"

                cols[3].markdown(f"<small style='text-align:right;display:block'>{dr_str}</small>", unsafe_allow_html=True)
                cols[4].markdown(f"<small style='text-align:right;display:block'>{cr_str}</small>", unsafe_allow_html=True)
                cols[5].markdown(f"<small style='text-align:right;display:block'>{bal_str}</small>", unsafe_allow_html=True)

            # Closing
            st.markdown("---")
            cl_cols = st.columns([0.8, 1.0, 0.8, 1.2, 1.2, 1.2])
            cl_cols[0].markdown("**Closing**")
            cl_type = "Dr" if closing < 0 else "Cr"
            cl_cols[5].markdown(f"**{fi(abs(closing))} {cl_type}**")

# ══════════════════════════════════════════════════════════════════════
#  MONTHLY TRENDS VIEW
# ══════════════════════════════════════════════════════════════════════

elif view == "monthly_trends":
    if st.button("\u2190 Back to GSTR-3B", key="back_to_3b_trends"):
        _nav("summary")
        st.rerun()

    st.subheader("Monthly GST Comparison  |  FY 2025-26")

    comparison = gst_monthly_comparison(conn)

    if comparison:
        html = """<table class="inv-table">
        <tr><th>Month</th><th>Output Taxable</th><th>Output Tax</th><th>Input Taxable</th>
            <th>Input Tax (ITC)</th><th>Net Payable</th><th>Status</th></tr>"""
        for r in comparison:
            color = "color:#dc2626" if r['status'] == 'Payable' else "color:#16a34a" if r['status'] == 'Refundable' else ""
            html += f"""<tr>
                <td>{r['month_label']}</td>
                <td style="text-align:right">{fi(r['output_taxable'])}</td>
                <td style="text-align:right">{fi(r['output_tax'])}</td>
                <td style="text-align:right">{fi(r['input_taxable'])}</td>
                <td style="text-align:right">{fi(r['input_tax'])}</td>
                <td style="text-align:right;{color};font-weight:700">{fi(r['net_payable'])}</td>
                <td style="{color};font-weight:600">{r['status']}</td></tr>"""

        # Totals
        t_ot = sum(r['output_taxable'] for r in comparison)
        t_otx = sum(r['output_tax'] for r in comparison)
        t_it = sum(r['input_taxable'] for r in comparison)
        t_itx = sum(r['input_tax'] for r in comparison)
        t_np = sum(r['net_payable'] for r in comparison)
        html += f"""<tr class="total-row">
            <td>TOTAL</td>
            <td style="text-align:right">{fi(t_ot)}</td>
            <td style="text-align:right">{fi(t_otx)}</td>
            <td style="text-align:right">{fi(t_it)}</td>
            <td style="text-align:right">{fi(t_itx)}</td>
            <td style="text-align:right">{fi(t_np)}</td>
            <td></td></tr>"""
        html += "</table>"
        st.markdown(html, unsafe_allow_html=True)

        # Clickable month rows to jump to that month's 3B
        st.markdown("")
        st.markdown("**Click a month to view its GSTR-3B:**")
        mcols = st.columns(min(len(comparison), 5))
        for i, r in enumerate(comparison):
            with mcols[i % 5]:
                if st.button(r['month_label'], key=f"trend_month_{r['month']}"):
                    st.session_state.gst_month = r['month']
                    _nav("summary")
                    st.rerun()

# ══════════════════════════════════════════════════════════════════════
#  CLOSE CONNECTION
# ══════════════════════════════════════════════════════════════════════

conn.close()

# ══════════════════════════════════════════════════════════════════════
#  CHAT BAR
# ══════════════════════════════════════════════════════════════════════

st.markdown("---")
from chat_engine import ask, format_result_as_text
chat_input = st.chat_input("Ask anything \u2014 P&L, Balance Sheet, ledger of [party], debtors, creditors...")
if chat_input:
    result = ask(chat_input)
    st.markdown(f"**You:** {chat_input}")
    if result.get("type") == "chat":
        st.markdown(result.get("message", ""))
    else:
        st.markdown(format_result_as_text(result))
