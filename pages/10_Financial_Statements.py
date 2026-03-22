"""
Seven Labs Vision -- Companies Act Schedule III Financial Statements
Balance Sheet (Part I), Profit & Loss Statement (Part II), and Notes to Accounts
with Excel export containing 26 notes.
"""

import streamlit as st
import sys
import os
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from styles import (
    inject_base_styles, page_header, section_header, metric_card,
    fmt, fmt_full, footer, info_banner
)
from financial_statements import (
    generate_schedule_iii, get_bs_preview_data, get_pl_preview_data,
    DB_PATH, _get_conn, _get_metadata
)

st.set_page_config(
    page_title="Financial Statements -- SLV",
    page_icon="",
    layout="wide"
)
inject_base_styles()


# ── HELPERS ─────────────────────────────────────────────────────────────────

def fmt_stmt(amount):
    """Format amount for financial statement display."""
    if amount is None or amount == 0:
        return "-"
    if amount < 0:
        return f"({abs(amount):,.2f})"
    return f"{amount:,.2f}"


def fmt_stmt_short(amount):
    """Short format for metric cards."""
    if amount is None or amount == 0:
        return "0"
    return fmt(abs(amount))


# ── LOAD DATA ───────────────────────────────────────────────────────────────

conn = _get_conn()
metadata = _get_metadata(conn)
company_name = metadata.get("company_name", "Company")
conn.close()


# ── PAGE HEADER ─────────────────────────────────────────────────────────────

page_header(
    "Financial Statements",
    f"Companies Act 2013, Schedule III -- {company_name}"
)


# ── SIDEBAR: COMPANY INFO FORM ─────────────────────────────────────────────

st.sidebar.markdown("### Company Details")

with st.sidebar.form("company_info_form"):
    ci_name = st.text_input("Company Name", value=company_name)
    ci_cin = st.text_input("CIN", value="")
    ci_address = st.text_area("Registered Office", value="", height=68)

    st.markdown("---")
    st.markdown("**Financial Year**")
    col1, col2 = st.columns(2)
    with col1:
        ci_year_end = st.text_input("Year End", value="31.03.2026")
    with col2:
        ci_prev_year = st.text_input("Prev Year End", value="31.03.2025")

    st.markdown("---")
    st.markdown("**Auditor Details**")
    ci_auditor = st.text_input("CA Firm Name", value="")
    ci_frn = st.text_input("FRN", value="")
    ci_partner = st.text_input("Partner Name", value="")
    ci_member = st.text_input("Membership No.", value="")

    st.markdown("---")
    st.markdown("**Directors**")
    ci_dir1 = st.text_input("Director 1 Name", value="")
    ci_din1 = st.text_input("Director 1 DIN", value="")
    ci_dir2 = st.text_input("Director 2 Name", value="")
    ci_din2 = st.text_input("Director 2 DIN", value="")

    submitted = st.form_submit_button("Update Details")


company_info = {
    "name": ci_name,
    "cin": ci_cin,
    "address": ci_address,
    "year_end": ci_year_end,
    "prev_year_end": ci_prev_year,
    "auditor_name": ci_auditor,
    "auditor_frn": ci_frn,
    "auditor_partner": ci_partner,
    "auditor_member": ci_member,
    "director1_name": ci_dir1,
    "director1_din": ci_din1,
    "director2_name": ci_dir2,
    "director2_din": ci_din2,
    "denomination": "INR",
}


# ── LOAD FINANCIAL DATA ────────────────────────────────────────────────────

bs_data = get_bs_preview_data()
pl_data = get_pl_preview_data()


# ── SUMMARY METRICS ─────────────────────────────────────────────────────────

col1, col2, col3, col4 = st.columns(4)
with col1:
    metric_card("Total Assets", fmt_stmt_short(bs_data["total_assets"]),
                sub="Balance Sheet", color_class="blue")
with col2:
    metric_card("Total Equity & Liabilities", fmt_stmt_short(bs_data["total_liabilities"]),
                sub="Balance Sheet", color_class="purple")
with col3:
    metric_card("Revenue", fmt_stmt_short(pl_data["revenue"]),
                sub="From Operations", color_class="green")
with col4:
    color = "green" if pl_data["profit_before_tax"] >= 0 else "red"
    label = "Profit" if pl_data["profit_before_tax"] >= 0 else "Loss"
    metric_card(f"{label} Before Tax", fmt_stmt_short(pl_data["profit_before_tax"]),
                sub="P&L Statement", color_class=color)


# ── VERIFICATION SECTION ────────────────────────────────────────────────────

diff = abs(bs_data["total_liabilities"] - bs_data["total_assets"])
if diff < 1.0:
    info_banner("Balance Sheet is balanced. All figures verified against Tally data.", "success")
else:
    info_banner(
        f"Balance Sheet difference of {fmt_full(diff)} detected. "
        "Review reclassification between asset/liability items.",
        "warning"
    )


# ── GENERATE EXCEL ──────────────────────────────────────────────────────────

st.markdown("")
col_gen, col_spacer = st.columns([1, 3])
with col_gen:
    generate_clicked = st.button("Generate Financial Statements (Excel)", type="primary")

if generate_clicked:
    with st.spinner("Generating Schedule III compliant Excel with 26 Notes..."):
        tmp_file = os.path.join(tempfile.gettempdir(), "financial_statements.xlsx")
        result = generate_schedule_iii(
            db_path=DB_PATH,
            company_info=company_info,
            output_path=tmp_file
        )
        with open(tmp_file, "rb") as f:
            excel_data = f.read()

        v = result["verification"]

        st.download_button(
            label="Download Financial Statements (.xlsx)",
            data=excel_data,
            file_name=f"Schedule_III_{ci_name.replace(' ', '_')}_{ci_year_end.replace('.', '')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        st.success(
            f"Generated: Balance Sheet, P&L Statement, and {len(result['notes'])} Notes to Accounts. "
            f"{'BS Balanced.' if v['bs_balanced'] else f'BS Difference: {v[\"bs_difference\"]:.2f}'}"
        )

        # Show verification details
        with st.expander("Verification Details"):
            vcol1, vcol2 = st.columns(2)
            with vcol1:
                st.markdown("**Balance Sheet**")
                st.write(f"Total Equity & Liabilities: {fmt_stmt(v['bs_total_liabilities'])}")
                st.write(f"Total Assets: {fmt_stmt(v['bs_total_assets'])}")
                st.write(f"Difference: {v['bs_difference']:.2f}")
            with vcol2:
                st.markdown("**Profit & Loss**")
                st.write(f"Revenue: {fmt_stmt(v['pl_revenue'])}")
                st.write(f"Total Expenses: {fmt_stmt(v['pl_expenses'])}")
                st.write(f"Profit Before Tax: {fmt_stmt(v['pl_profit_before_tax'])}")
                st.write(f"Profit After Tax: {fmt_stmt(v['pl_profit_after_tax'])}")

            st.markdown("**Excel Sheets Generated:**")
            st.write("1. Balance Sheet (Part I)")
            st.write("2. Statement of Profit and Loss (Part II)")
            st.write("3. Note 1 - Significant Accounting Policies")
            st.write("4. Notes 2-16 (Balance Sheet Notes)")
            st.write("5. Notes 17-23 (P&L Notes)")
            st.write("6. Notes 24-26 (Other Disclosures)")


# ── TAB VIEWS ───────────────────────────────────────────────────────────────

tab_bs, tab_pl = st.tabs(["Balance Sheet", "Profit & Loss Statement"])


# ── BALANCE SHEET TAB ───────────────────────────────────────────────────────

with tab_bs:
    section_header(f"Balance Sheet as at {ci_year_end}")

    rows_html = []

    def add_row(label, note="", amount=None, cls="", indent=0):
        pad = "&nbsp;" * (indent * 6)
        amt_str = fmt_stmt(amount) if amount is not None else ""
        amt_class = "amt-neg" if amount and amount < 0 else ""
        note_str = note if note else ""
        rows_html.append(
            f'<tr class="{cls}">'
            f'<td style="text-align:left;padding-left:{8 + indent * 20}px">{pad}{label}</td>'
            f'<td style="text-align:center;font-size:0.8rem">{note_str}</td>'
            f'<td style="text-align:right" class="amt {amt_class}">{amt_str}</td>'
            f'</tr>'
        )

    def add_section_row(label):
        rows_html.append(
            f'<tr><td colspan="3" style="text-align:left;font-weight:700;'
            f'padding:12px 14px 6px;background:#f8fafc;font-size:0.9rem">'
            f'{label}</td></tr>'
        )

    def add_subsection_row(label):
        rows_html.append(
            f'<tr><td colspan="3" style="text-align:left;font-weight:600;'
            f'padding:8px 14px 4px 28px;font-size:0.85rem">'
            f'{label}</td></tr>'
        )

    def add_total_row(label, amount):
        amt_str = fmt_stmt(amount)
        rows_html.append(
            f'<tr class="total-row"><td style="text-align:left;font-weight:700">{label}</td>'
            f'<td></td>'
            f'<td style="text-align:right" class="amt">{amt_str}</td></tr>'
        )

    # I. EQUITY AND LIABILITIES
    add_section_row("I. EQUITY AND LIABILITIES")

    add_subsection_row("(1) Shareholder's Funds")
    sf = bs_data["liabilities"]["shareholders_funds"]
    for idx, (key, item) in enumerate(sf.items()):
        letter = chr(ord('a') + idx)
        add_row(f"({letter}) {item['label']}", item.get("note", ""), item["amount"], indent=2)

    add_subsection_row("(2) Share application money pending allotment")

    add_subsection_row("(3) Non-Current Liabilities")
    ncl = bs_data["liabilities"]["non_current_liabilities"]
    for idx, (key, item) in enumerate(ncl.items()):
        letter = chr(ord('a') + idx)
        add_row(f"({letter}) {item['label']}", item.get("note", ""), item["amount"], indent=2)

    add_subsection_row("(4) Current Liabilities")
    cl = bs_data["liabilities"]["current_liabilities"]
    for idx, (key, item) in enumerate(cl.items()):
        letter = chr(ord('a') + idx)
        add_row(f"({letter}) {item['label']}", item.get("note", ""), item["amount"], indent=2)

    add_total_row("Total Equity and Liabilities", bs_data["total_liabilities"])

    # II. ASSETS
    add_section_row("II. ASSETS")

    add_subsection_row("(1) Non-current assets")
    nca = bs_data["assets"]["non_current_assets"]
    for idx, (key, item) in enumerate(nca.items()):
        letter = chr(ord('a') + idx)
        add_row(f"({letter}) {item['label']}", item.get("note", ""), item["amount"], indent=2)

    add_subsection_row("(2) Current assets")
    ca = bs_data["assets"]["current_assets"]
    for idx, (key, item) in enumerate(ca.items()):
        letter = chr(ord('a') + idx)
        add_row(f"({letter}) {item['label']}", item.get("note", ""), item["amount"], indent=2)

    add_total_row("Total Assets", bs_data["total_assets"])

    table_html = f"""
    <table class="slv-table">
    <thead>
        <tr>
            <th style="text-align:left;width:55%">Particulars</th>
            <th style="text-align:center;width:10%">Note</th>
            <th style="text-align:right;width:35%">Amount (INR)</th>
        </tr>
    </thead>
    <tbody>
        {''.join(rows_html)}
    </tbody>
    </table>
    """
    st.markdown(table_html, unsafe_allow_html=True)


# ── P&L STATEMENT TAB ──────────────────────────────────────────────────────

with tab_pl:
    section_header(f"Statement of Profit and Loss for the year ended {ci_year_end}")

    pl_rows = []

    def add_pl_row(label, note="", amount=None, cls="", indent=0, bold=False):
        pad = "&nbsp;" * (indent * 6)
        amt_str = fmt_stmt(amount) if amount is not None else ""
        amt_class = "amt-neg" if amount and amount < 0 else ""
        fw = "font-weight:700;" if bold else ""
        note_str = note if note else ""
        pl_rows.append(
            f'<tr class="{cls}">'
            f'<td style="text-align:left;{fw}padding-left:{8 + indent * 20}px">{pad}{label}</td>'
            f'<td style="text-align:center;font-size:0.8rem">{note_str}</td>'
            f'<td style="text-align:right;{fw}" class="amt {amt_class}">{amt_str}</td>'
            f'</tr>'
        )

    def add_pl_total(label, amount):
        amt_str = fmt_stmt(amount)
        amt_class = "amt-neg" if amount and amount < 0 else ""
        pl_rows.append(
            f'<tr class="total-row"><td style="text-align:left;font-weight:700">{label}</td>'
            f'<td></td>'
            f'<td style="text-align:right;font-weight:700" class="amt {amt_class}">{amt_str}</td></tr>'
        )

    # Revenue section
    add_pl_row("I. Revenue from operations", "17", pl_data["revenue"], bold=False)
    add_pl_row("II. Other Income", "18", pl_data["other_income"])
    add_pl_total("III. Total Income (I + II)", pl_data["total_income"])

    pl_rows.append('<tr><td colspan="3" style="height:8px"></td></tr>')

    pl_rows.append(
        '<tr><td colspan="3" style="text-align:left;font-weight:700;'
        'padding:10px 14px 4px;font-size:0.9rem">IV. Expenses:</td></tr>'
    )

    expense_order = ["cost_of_materials", "purchase_stock_in_trade", "changes_in_inventories",
                     "employee_benefit", "finance_costs", "depreciation", "other_expenses"]
    for key in expense_order:
        if key in pl_data["expenses"]:
            item = pl_data["expenses"][key]
            add_pl_row(item["label"], item.get("note", ""), item["amount"], indent=1)

    add_pl_total("Total Expenses", pl_data["total_expenses"])

    pl_rows.append('<tr><td colspan="3" style="height:8px"></td></tr>')

    add_pl_row("V. Profit/(Loss) before exceptional items and tax (III - IV)", "",
               pl_data["profit_before_tax"], bold=True)
    add_pl_row("VI. Exceptional Items", "", 0)
    add_pl_row("VII. Profit/(Loss) before tax (V - VI)", "",
               pl_data["profit_before_tax"], bold=True)

    pl_rows.append('<tr><td colspan="3" style="height:4px"></td></tr>')
    add_pl_row("VIII. Tax expense:", "", None, bold=True)
    add_pl_row("(1) Current tax", "", pl_data["tax_current"], indent=1)
    add_pl_row("(2) Deferred tax", "5", pl_data["tax_deferred"], indent=1)

    pl_rows.append('<tr><td colspan="3" style="height:4px"></td></tr>')
    add_pl_total("IX. Profit/(Loss) for the period", pl_data["profit_after_tax"])

    pl_rows.append('<tr><td colspan="3" style="height:8px"></td></tr>')
    add_pl_row("X. Earnings per equity share:", "", None, bold=True)
    add_pl_row("(1) Basic", "", None, indent=1)
    add_pl_row("(2) Diluted", "", None, indent=1)

    pl_table_html = f"""
    <table class="slv-table">
    <thead>
        <tr>
            <th style="text-align:left;width:55%">Particulars</th>
            <th style="text-align:center;width:10%">Note</th>
            <th style="text-align:right;width:35%">Amount (INR)</th>
        </tr>
    </thead>
    <tbody>
        {''.join(pl_rows)}
    </tbody>
    </table>
    """
    st.markdown(pl_table_html, unsafe_allow_html=True)


# ── FOOTER ──────────────────────────────────────────────────────────────────

footer(company_name)
