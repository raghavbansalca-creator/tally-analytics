"""
Seven Labs Vision — Group-Context Classifier (Layer 0)
Classifies transactions using voucher type + ledger group hierarchy
BEFORE reading any narration text.

This is the highest-confidence, fastest classifier:
- Zero regex, zero NLP, zero API calls
- Uses structural accounting data that Tally already provides
- 100% deterministic — same input always gives same output

Classification order in the full pipeline:
  Layer 0: Group-Context Rules  ← THIS MODULE (handles 60-70%)
  Layer 1: Bank Statement Parser (handles 15-20% of remainder)
  Layer 2: Regex on narration text (handles remaining)
  Layer 3: LLM for unresolved (final fallback)
"""

import sqlite3
from collections import defaultdict


# ── TALLY PRIMARY GROUP CONSTANTS ───────────────────────────────────────────
# These are Tally's 28 standard groups mapped to their primary parent.

PRIMARY_GROUPS = {
    # Balance Sheet — Assets
    "Current Assets": "ASSETS",
    "Bank Accounts": "ASSETS",
    "Cash-in-Hand": "ASSETS",
    "Deposits (Asset)": "ASSETS",
    "Loans & Advances (Asset)": "ASSETS",
    "Stock-in-Hand": "ASSETS",
    "Sundry Debtors": "ASSETS",
    "Fixed Assets": "ASSETS",
    "Investments": "ASSETS",
    "Misc. Expenses (ASSET)": "ASSETS",

    # Balance Sheet — Liabilities
    "Capital Account": "LIABILITIES",
    "Reserves & Surplus": "LIABILITIES",
    "Current Liabilities": "LIABILITIES",
    "Duties & Taxes": "LIABILITIES",
    "Provisions": "LIABILITIES",
    "Sundry Creditors": "LIABILITIES",
    "Loans (Liability)": "LIABILITIES",
    "Secured Loans": "LIABILITIES",
    "Unsecured Loans": "LIABILITIES",
    "Bank OD A/c": "LIABILITIES",

    # P&L — Revenue
    "Sales Accounts": "REVENUE",
    "Direct Incomes": "REVENUE",
    "Indirect Incomes": "REVENUE",

    # P&L — Expenses
    "Purchase Accounts": "EXPENSES",
    "Direct Expenses": "EXPENSES",
    "Indirect Expenses": "EXPENSES",

    # Other
    "Branch / Divisions": "OTHER",
    "Suspense A/c": "OTHER",
}


# ── BANK-LIKE GROUPS (any group that represents bank/cash) ──────────────────
# Bank OD A/c is functionally a bank account for payment/receipt purposes.
BANK_GROUPS = ["Bank Accounts", "Cash-in-Hand", "Bank OD A/c"]

# ── VOUCHER TYPE ALIASES ────────────────────────────────────────────────────
# Tally allows custom voucher type names. Map common variants to base types.
VOUCHER_TYPE_ALIASES = {
    "SALE INVOICE": "Sales",
    "Sale Invoice": "Sales",
    "Sales Invoice": "Sales",
    "PURCHASE INVOICE": "Purchase",
    "Purchase Invoice": "Purchase",
    "JOURNAL VOUCHER": "Journal",
    "Journal Voucher": "Journal",
    "PAYMENT VOUCHER": "Payment",
    "RECEIPT VOUCHER": "Receipt",
    "DEBIT NOTE": "Debit Note",
    "CREDIT NOTE": "Credit Note",
}


def _normalise_voucher_type(vtype):
    """Normalise custom voucher type names to standard Tally base types."""
    if not vtype:
        return ""
    return VOUCHER_TYPE_ALIASES.get(vtype, vtype)


# ── GROUP-CONTEXT RULES ─────────────────────────────────────────────────────
# Each rule maps (voucher_type, debit_group, credit_group) patterns to a
# classification. The group can be a specific Tally group name or a primary
# category (ASSETS, LIABILITIES, REVENUE, EXPENSES).
#
# Rules are checked in order. First match wins.
# BANK_GROUPS is used to match any bank-like group (Bank Accounts, Cash-in-Hand, Bank OD A/c).

CONTEXT_RULES = [
    # ═══ RECEIPT VOUCHERS ═══
    # Receipt = money coming in (debit Bank/Cash/OD, credit the source)
    {
        "voucher_types": ["Receipt"],
        "debit_groups": BANK_GROUPS,
        "credit_groups": ["Sundry Debtors"],
        "category": "Debtor Receipt",
        "confidence": 0.95,
        "comment": "Payment received from debtor — verify outstanding reconciliation",
    },
    {
        "voucher_types": ["Receipt"],
        "debit_groups": BANK_GROUPS,
        "credit_groups": ["Sales Accounts", "Direct Incomes"],
        "category": "Sales/Revenue",
        "confidence": 0.90,
        "comment": "Cash/direct sale receipt — verify GST treatment",
    },
    {
        "voucher_types": ["Receipt"],
        "debit_groups": BANK_GROUPS,
        "credit_groups": ["Indirect Incomes"],
        "category": "Sales/Revenue",
        "confidence": 0.85,
        "comment": "Non-operating income received — verify classification",
    },
    {
        "voucher_types": ["Receipt"],
        "debit_groups": BANK_GROUPS,
        "credit_groups": ["Loans & Advances (Asset)"],
        "category": "Loan/Advance",
        "confidence": 0.90,
        "comment": "Advance/loan recovered — verify original advance ledger",
    },
    {
        "voucher_types": ["Receipt"],
        "debit_groups": BANK_GROUPS,
        "credit_groups": ["Unsecured Loans", "Secured Loans", "Loans (Liability)"],
        "category": "Loan/Advance",
        "confidence": 0.90,
        "comment": "Loan received — verify Sec 185/186 compliance, interest terms",
    },
    {
        "voucher_types": ["Receipt"],
        "debit_groups": BANK_GROUPS,
        "credit_groups": ["Capital Account", "Reserves & Surplus"],
        "category": "Capital Transaction",
        "confidence": 0.90,
        "comment": "Capital contribution received — verify partner/shareholder records",
    },
    {
        "voucher_types": ["Receipt"],
        "debit_groups": BANK_GROUPS,
        "credit_groups": ["Investments"],
        "category": "Capital Transaction",
        "confidence": 0.85,
        "comment": "Investment redemption/dividend received — verify valuation, TDS",
    },
    {
        "voucher_types": ["Receipt"],
        "debit_groups": BANK_GROUPS,
        "credit_groups": ["Suspense A/c"],
        "category": "Suspense/Clearing",
        "confidence": 0.85,
        "comment": "Receipt through clearing/suspense — verify clearance, identify actual party",
    },
    {
        "voucher_types": ["Receipt"],
        "debit_groups": BANK_GROUPS,
        "credit_groups": ["Sundry Creditors"],
        "category": "Creditor Payment",
        "confidence": 0.80,
        "comment": "Receipt from creditor (refund/credit note) — verify original transaction",
        "override_category": "Creditor Refund",
    },
    # Receipt: inter-bank (bank to bank)
    {
        "voucher_types": ["Receipt"],
        "debit_groups": BANK_GROUPS,
        "credit_groups": BANK_GROUPS,
        "category": "Contra/Bank Transfer",
        "confidence": 0.90,
        "comment": "Inter-bank transfer via receipt voucher — verify bank reconciliation",
    },

    # ═══ PAYMENT VOUCHERS ═══
    # Payment = money going out (credit Bank/Cash/OD, debit the destination)
    {
        "voucher_types": ["Payment"],
        "debit_groups": ["Sundry Creditors"],
        "credit_groups": BANK_GROUPS,
        "category": "Creditor Payment",
        "confidence": 0.95,
        "comment": "Payment to creditor — verify bill reconciliation",
    },
    {
        "voucher_types": ["Payment"],
        "debit_groups": ["Sundry Debtors"],
        "credit_groups": BANK_GROUPS,
        "category": "Loan/Advance",
        "confidence": 0.80,
        "comment": "Payment to debtor (advance/refund/adjustment) — verify nature: sales return refund or advance to customer",
        "needs_narration_refinement": True,
        "override_category": "Advance to Debtor",
    },
    {
        "voucher_types": ["Payment"],
        "debit_groups": ["Indirect Expenses"],
        "credit_groups": BANK_GROUPS,
        "category": "Utility/Office",
        "confidence": 0.80,
        "comment": "Indirect expense payment — narration needed for sub-classification (rent/salary/professional/etc.)",
        "needs_narration_refinement": True,
    },
    {
        "voucher_types": ["Payment"],
        "debit_groups": ["Direct Expenses"],
        "credit_groups": BANK_GROUPS,
        "category": "Purchase/Material",
        "confidence": 0.80,
        "comment": "Direct expense payment — verify cost allocation",
    },
    {
        "voucher_types": ["Payment"],
        "debit_groups": ["Duties & Taxes"],
        "credit_groups": BANK_GROUPS,
        "category": "GST Payment",
        "confidence": 0.85,
        "comment": "Tax payment — verify challan, GST/TDS classification from narration",
        "needs_narration_refinement": True,
    },
    {
        "voucher_types": ["Payment"],
        "debit_groups": ["Fixed Assets"],
        "credit_groups": BANK_GROUPS,
        "category": "Capital Expenditure",
        "confidence": 0.95,
        "comment": "Fixed asset purchase — verify capitalisation, depreciation schedule",
    },
    {
        "voucher_types": ["Payment"],
        "debit_groups": ["Loans & Advances (Asset)"],
        "credit_groups": BANK_GROUPS,
        "category": "Loan/Advance",
        "confidence": 0.90,
        "comment": "Advance given — verify purpose, recoverability, Sec 185/186",
    },
    {
        "voucher_types": ["Payment"],
        "debit_groups": ["Unsecured Loans", "Secured Loans", "Loans (Liability)", "Bank OD A/c"],
        "credit_groups": BANK_GROUPS,
        "category": "Loan/Advance",
        "confidence": 0.90,
        "comment": "Loan repayment — verify EMI schedule, interest split",
    },
    {
        "voucher_types": ["Payment"],
        "debit_groups": ["Capital Account"],
        "credit_groups": BANK_GROUPS,
        "category": "Capital Transaction",
        "confidence": 0.90,
        "comment": "Partner/director withdrawal — verify drawing account, Sec 185",
    },
    {
        "voucher_types": ["Payment"],
        "debit_groups": ["Investments"],
        "credit_groups": BANK_GROUPS,
        "category": "Capital Expenditure",
        "confidence": 0.85,
        "comment": "Investment made — verify valuation, disclosure",
    },
    {
        "voucher_types": ["Payment"],
        "debit_groups": ["Suspense A/c"],
        "credit_groups": BANK_GROUPS,
        "category": "Suspense/Clearing",
        "confidence": 0.85,
        "comment": "Payment through suspense/clearing — verify clearance, identify actual payee",
    },
    # Payment: inter-bank (bank to bank)
    {
        "voucher_types": ["Payment"],
        "debit_groups": BANK_GROUPS,
        "credit_groups": BANK_GROUPS,
        "category": "Contra/Bank Transfer",
        "confidence": 0.90,
        "comment": "Inter-bank transfer via payment voucher — verify bank reconciliation",
    },

    # ═══ CONTRA VOUCHERS ═══
    {
        "voucher_types": ["Contra"],
        "debit_groups": BANK_GROUPS,
        "credit_groups": BANK_GROUPS,
        "category": "Contra/Bank Transfer",
        "confidence": 0.90,
        "comment": "Inter-bank transfer / cash deposit — verify bank reconciliation",
    },

    # ═══ SALES VOUCHERS (including custom "SALE INVOICE") ═══
    {
        "voucher_types": ["Sales"],
        "debit_groups": ["Sundry Debtors", "Sundry Creditors"] + BANK_GROUPS,
        "credit_groups": ["Sales Accounts", "Duties & Taxes", "Indirect Expenses", "Direct Incomes"],
        "category": "Sales/Revenue",
        "confidence": 0.95,
        "comment": "Sales transaction — verify GST invoice, revenue recognition",
    },

    # ═══ PURCHASE VOUCHERS ═══
    {
        "voucher_types": ["Purchase"],
        "debit_groups": ["Purchase Accounts", "Duties & Taxes", "Indirect Expenses"],
        "credit_groups": ["Sundry Creditors", "Sundry Debtors"] + BANK_GROUPS,
        "category": "Purchase/Material",
        "confidence": 0.95,
        "comment": "Purchase transaction — verify GST input credit eligibility",
    },

    # ═══ JOURNAL VOUCHERS (most audit-critical) ═══
    {
        "voucher_types": ["Journal"],
        "debit_groups": ["Indirect Expenses"],
        "credit_groups": ["Sundry Creditors"],
        "category": "Utility/Office",
        "confidence": 0.75,
        "comment": "Expense provision/accrual — narration needed for sub-classification",
        "needs_narration_refinement": True,
    },
    {
        "voucher_types": ["Journal"],
        "debit_groups": ["Indirect Expenses"],
        "credit_groups": ["Provisions"],
        "category": "Provision/Write-off",
        "confidence": 0.90,
        "comment": "Provision entry — verify board resolution, documentation",
    },
    {
        "voucher_types": ["Journal"],
        "debit_groups": ["Direct Expenses"],
        "credit_groups": ["Sundry Creditors"],
        "category": "Purchase/Material",
        "confidence": 0.80,
        "comment": "Direct cost accrual — verify cost allocation, period",
    },
    {
        "voucher_types": ["Journal"],
        "debit_groups": ["Fixed Assets"],
        "credit_groups": ["Sundry Creditors", "Bank Accounts"],
        "category": "Capital Expenditure",
        "confidence": 0.95,
        "comment": "Fixed asset addition via journal — verify capitalisation policy",
    },
    {
        "voucher_types": ["Journal"],
        "debit_groups": ["Duties & Taxes"],
        "credit_groups": ["Duties & Taxes"],
        "category": "GST Payment",
        "confidence": 0.85,
        "comment": "Tax adjustment/set-off — verify GST set-off rules",
        "override_category": "Tax Adjustment",
    },
    {
        "voucher_types": ["Journal"],
        "debit_groups": ["Sundry Debtors"],
        "credit_groups": ["Sundry Creditors"],
        "category": "Inter-company/Branch",
        "confidence": 0.70,
        "comment": "Debtor-creditor adjustment — narration critical for understanding purpose",
        "needs_narration_refinement": True,
    },
    {
        "voucher_types": ["Journal"],
        "debit_groups": ["Capital Account"],
        "credit_groups": ["Indirect Expenses", "Direct Expenses"],
        "category": "Capital Transaction",
        "confidence": 0.80,
        "comment": "Expense charged to capital — verify if personal expense of partner/director",
    },
    {
        "voucher_types": ["Journal"],
        "debit_groups": ["Suspense A/c"],
        "credit_groups": None,  # Any credit group
        "category": "Suspense/Clearing",
        "confidence": 0.95,
        "comment": "Suspense entry — MUST be cleared before year-end",
    },
    {
        "voucher_types": ["Journal"],
        "debit_groups": None,  # Any debit group
        "credit_groups": ["Suspense A/c"],
        "category": "Suspense/Clearing",
        "confidence": 0.95,
        "comment": "Suspense entry — MUST be cleared before year-end",
    },

    # ═══ DEBIT NOTE / CREDIT NOTE ═══
    {
        "voucher_types": ["Debit Note"],
        "debit_groups": ["Sundry Creditors", "Purchase Accounts"],
        "credit_groups": ["Purchase Accounts", "Sundry Creditors", "Duties & Taxes"],
        "category": "Purchase/Material",
        "confidence": 0.85,
        "comment": "Purchase return / debit note — verify credit note from supplier, GST adjustment",
        "override_category": "Purchase Return",
    },
    {
        "voucher_types": ["Credit Note"],
        "debit_groups": ["Sales Accounts", "Sundry Debtors", "Duties & Taxes", "Indirect Expenses", "Sundry Creditors"],
        "credit_groups": ["Sundry Debtors", "Sundry Creditors", "Sales Accounts", "Duties & Taxes", "Indirect Expenses"],
        "category": "Sales/Revenue",
        "confidence": 0.85,
        "comment": "Sales return / credit note — verify GST credit note issued",
        "override_category": "Sales Return",
    },
]


# ── FALLBACK: Group-only classification when no rule matches ────────────────
# If no specific rule matches, classify based on the PRIMARY group involved.
# This ensures zero "unclassified" — every ledger has a group in Tally.

GROUP_FALLBACK = {
    "Sundry Debtors": ("Debtor Receipt", 0.60, "Sundry Debtor transaction — verify nature from narration"),
    "Sundry Creditors": ("Creditor Payment", 0.60, "Sundry Creditor transaction — verify nature from narration"),
    "Sales Accounts": ("Sales/Revenue", 0.70, "Sales account entry — verify revenue recognition"),
    "Purchase Accounts": ("Purchase/Material", 0.70, "Purchase account entry — verify ITC eligibility"),
    "Direct Expenses": ("Purchase/Material", 0.60, "Direct expense — verify cost allocation"),
    "Indirect Expenses": ("Utility/Office", 0.60, "Indirect expense — narration needed for sub-classification"),
    "Direct Incomes": ("Sales/Revenue", 0.65, "Direct income — verify classification"),
    "Indirect Incomes": ("Sales/Revenue", 0.60, "Non-operating income — verify nature"),
    "Fixed Assets": ("Capital Expenditure", 0.70, "Fixed asset entry — verify capitalisation"),
    "Investments": ("Capital Transaction", 0.65, "Investment entry — verify valuation"),
    "Capital Account": ("Capital Transaction", 0.70, "Capital account entry — verify partner/shareholder records"),
    "Reserves & Surplus": ("Capital Transaction", 0.65, "Reserves entry — verify appropriation"),
    "Duties & Taxes": ("GST Payment", 0.60, "Tax/duty entry — verify GST/TDS from narration"),
    "Provisions": ("Provision/Write-off", 0.70, "Provision entry — verify documentation"),
    "Secured Loans": ("Loan/Advance", 0.70, "Secured loan entry — verify terms"),
    "Unsecured Loans": ("Loan/Advance", 0.70, "Unsecured loan entry — verify Sec 185/186"),
    "Loans (Liability)": ("Loan/Advance", 0.65, "Loan liability entry — verify terms"),
    "Loans & Advances (Asset)": ("Loan/Advance", 0.70, "Loan/advance asset entry — verify recoverability"),
    "Bank Accounts": ("Bank Charges", 0.50, "Bank entry — verify bank reconciliation"),
    "Bank OD A/c": ("Loan/Advance", 0.55, "Bank OD entry — verify interest, limit"),
    "Cash-in-Hand": ("Cash Transactions", 0.60, "Cash entry — verify Sec 269SS/269ST"),
    "Suspense A/c": ("Suspense/Clearing", 0.80, "Suspense entry — MUST be cleared before year-end"),
    "Stock-in-Hand": ("Purchase/Material", 0.60, "Stock entry — verify inventory valuation"),
    "Deposits (Asset)": ("Loan/Advance", 0.55, "Deposit entry — verify terms, recoverability"),
    "Branch / Divisions": ("Inter-company/Branch", 0.70, "Branch entry — verify transfer pricing"),
    "Misc. Expenses (ASSET)": ("Utility/Office", 0.50, "Misc asset expense — verify amortisation"),
}


# ── GROUP HIERARCHY BUILDER ─────────────────────────────────────────────────

def build_group_hierarchy(conn):
    """Build a dict mapping each group to its full parent chain.

    Returns:
        {group_name: [group_name, parent, grandparent, ...up to Primary]}
    """
    cur = conn.cursor()
    cur.execute("SELECT NAME, PARENT FROM mst_group")
    parent_map = {row[0]: row[1] for row in cur.fetchall()}

    hierarchy = {}
    for group in parent_map:
        chain = [group]
        current = group
        seen = set()
        while current in parent_map and current not in seen:
            seen.add(current)
            parent = parent_map[current]
            if parent == "Primary" or parent == current:
                break
            chain.append(parent)
            current = parent
        hierarchy[group] = chain
    return hierarchy


def build_ledger_group_map(conn):
    """Build a dict mapping ledger name to its immediate parent group.

    Returns:
        {ledger_name: group_name}
    """
    cur = conn.cursor()
    cur.execute("SELECT NAME, PARENT FROM mst_ledger")
    return {row[0]: row[1] for row in cur.fetchall()}


# ── VOUCHER ANALYSIS ────────────────────────────────────────────────────────

def get_voucher_legs(conn, voucher_guid):
    """Get all accounting entries for a voucher with their group info.

    Returns list of dicts with: ledger, amount, group, primary_side (ASSETS/
    LIABILITIES/REVENUE/EXPENSES), is_debit (bool).
    """
    ledger_group_map = build_ledger_group_map(conn)

    cur = conn.cursor()
    cur.execute("""
        SELECT LEDGERNAME, AMOUNT
        FROM trn_accounting
        WHERE VOUCHER_GUID = ?
    """, (voucher_guid,))

    legs = []
    for row in cur.fetchall():
        ledger = row[0] or ""
        try:
            amount = float(row[1] or 0)
        except (ValueError, TypeError):
            amount = 0.0

        group = ledger_group_map.get(ledger, "")
        primary = PRIMARY_GROUPS.get(group, "OTHER")

        legs.append({
            "ledger": ledger,
            "amount": amount,
            "group": group,
            "primary_side": primary,
            "is_debit": amount < 0,  # Tally: negative = debit
        })
    return legs


def classify_by_context(voucher_type, legs):
    """Classify a single voucher using group-context rules.

    Args:
        voucher_type: Tally voucher type name (Receipt, Payment, etc.)
        legs: List of dicts from get_voucher_legs()

    Returns:
        dict with: category, confidence, comment, method, needs_narration_refinement,
                   debit_groups, credit_groups
        or None if no rule matched.
    """
    if not legs:
        return None

    # Normalise custom voucher type names (e.g., "SALE INVOICE" → "Sales")
    normalised_type = _normalise_voucher_type(voucher_type)

    # Separate debit and credit legs
    debit_groups = set()
    credit_groups = set()
    for leg in legs:
        if leg["is_debit"]:
            debit_groups.add(leg["group"])
        else:
            credit_groups.add(leg["group"])

    # Try each rule (using both original and normalised voucher type)
    for rule in CONTEXT_RULES:
        # Check voucher type (match original OR normalised)
        if voucher_type not in rule["voucher_types"] and normalised_type not in rule["voucher_types"]:
            continue

        # Check debit groups (None = any group matches)
        if rule["debit_groups"] is not None:
            if not debit_groups.intersection(set(rule["debit_groups"])):
                continue

        # Check credit groups (None = any group matches)
        if rule["credit_groups"] is not None:
            if not credit_groups.intersection(set(rule["credit_groups"])):
                continue

        # Rule matched
        return {
            "category": rule.get("override_category", rule["category"]),
            "confidence": rule["confidence"],
            "comment": rule["comment"],
            "method": "group_context",
            "needs_narration_refinement": rule.get("needs_narration_refinement", False),
            "debit_groups": list(debit_groups),
            "credit_groups": list(credit_groups),
        }

    # ── FALLBACK: No specific rule matched → use primary group ──────────
    # Pick the most "meaningful" group (not bank, not tax) for classification
    all_groups = debit_groups | credit_groups
    meaningful_groups = all_groups - {"Bank Accounts", "Cash-in-Hand", "Bank OD A/c", "Duties & Taxes"}
    target_groups = meaningful_groups if meaningful_groups else all_groups

    for group in target_groups:
        if group in GROUP_FALLBACK:
            cat, conf, comment = GROUP_FALLBACK[group]
            return {
                "category": cat,
                "confidence": conf,
                "comment": f"[Fallback] {comment}",
                "method": "group_fallback",
                "needs_narration_refinement": True,
                "debit_groups": list(debit_groups),
                "credit_groups": list(credit_groups),
            }

    # Absolute last resort — groups exist but not in our mapping
    if all_groups:
        return {
            "category": "Uncategorized",
            "confidence": 0.30,
            "comment": f"Groups found ({', '.join(all_groups)}) but no classification rule matches. Review required.",
            "method": "group_unmatched",
            "needs_narration_refinement": True,
            "debit_groups": list(debit_groups),
            "credit_groups": list(credit_groups),
        }

    return None


# ── BATCH ANALYSIS ──────────────────────────────────────────────────────────

def classify_all_vouchers(db_path, from_date=None, to_date=None):
    """Classify all vouchers using group-context rules.

    Returns dict with:
        classified: list of {guid, date, voucher_type, narration, party,
                             category, confidence, comment, method, ...}
        unclassified: list of vouchers where no rule matched (need narration analysis)
        stats: summary statistics
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ledger_group_map = build_ledger_group_map(conn)

    # Build date filter
    date_filter = ""
    params = []
    if from_date:
        date_filter += " AND v.DATE >= ?"
        params.append(from_date)
    if to_date:
        date_filter += " AND v.DATE <= ?"
        params.append(to_date)

    # Get all vouchers
    cur = conn.cursor()
    cur.execute(f"""
        SELECT v.GUID, v.DATE, v.VOUCHERTYPENAME, v.NARRATION,
               v.PARTYLEDGERNAME, v.VOUCHERNUMBER
        FROM trn_voucher v
        WHERE 1=1 {date_filter}
        ORDER BY v.DATE
    """, params)
    vouchers = cur.fetchall()

    # Pre-fetch all accounting entries grouped by voucher
    cur.execute("SELECT VOUCHER_GUID, LEDGERNAME, AMOUNT FROM trn_accounting")
    voucher_legs = defaultdict(list)
    for row in cur.fetchall():
        guid = row[0]
        ledger = row[1] or ""
        try:
            amount = float(row[2] or 0)
        except (ValueError, TypeError):
            amount = 0.0
        group = ledger_group_map.get(ledger, "")
        primary = PRIMARY_GROUPS.get(group, "OTHER")
        voucher_legs[guid].append({
            "ledger": ledger,
            "amount": amount,
            "group": group,
            "primary_side": primary,
            "is_debit": amount < 0,
        })

    classified = []
    unclassified = []
    category_counts = defaultdict(int)
    method_counts = defaultdict(int)

    for vch in vouchers:
        guid = vch["GUID"]
        vtype = vch["VOUCHERTYPENAME"] or ""
        narration = vch["NARRATION"] or ""
        party = vch["PARTYLEDGERNAME"] or ""
        legs = voucher_legs.get(guid, [])

        result = classify_by_context(vtype, legs)

        vch_data = {
            "guid": guid,
            "date": vch["DATE"] or "",
            "voucher_type": vtype,
            "voucher_number": vch["VOUCHERNUMBER"] or "",
            "narration": narration,
            "party": party,
            "debit_groups": list({l["group"] for l in legs if l["is_debit"]}),
            "credit_groups": list({l["group"] for l in legs if not l["is_debit"]}),
            "amount": sum(abs(l["amount"]) for l in legs if l["is_debit"]),
        }

        if result:
            vch_data.update(result)
            classified.append(vch_data)
            category_counts[result["category"]] += 1
            method_counts["group_context"] += 1
        else:
            vch_data["method"] = "unclassified"
            unclassified.append(vch_data)
            method_counts["unclassified"] += 1

    conn.close()

    total = len(vouchers)
    return {
        "classified": classified,
        "unclassified": unclassified,
        "stats": {
            "total_vouchers": total,
            "classified_by_group": len(classified),
            "unclassified": len(unclassified),
            "coverage_pct": round(len(classified) / total * 100, 1) if total else 0,
            "category_distribution": dict(category_counts),
            "needs_narration_refinement": sum(
                1 for c in classified if c.get("needs_narration_refinement")
            ),
        },
    }


# ── CROSS-CHECK: NARRATION vs GROUP ─────────────────────────────────────────

# Expected narration categories for each Tally group.
# If the narration says "salary" but the group is "Fixed Assets", that's a mismatch.
EXPECTED_NARRATION_FOR_GROUP = {
    "Sundry Debtors": ["Debtor Receipt", "Sales/Revenue", "Sales Return"],
    "Sundry Creditors": ["Creditor Payment", "Purchase/Material", "Purchase Return",
                         "Utility/Office", "Professional/Consultancy", "Contractor Payments",
                         "Rent Payments", "Insurance"],
    "Bank Accounts": ["Bank Charges", "Contra/Bank Transfer", "Debtor Receipt",
                      "Creditor Payment"],
    "Cash-in-Hand": ["Cash Transactions", "Debtor Receipt", "Creditor Payment"],
    "Fixed Assets": ["Capital Expenditure"],
    "Investments": ["Capital Expenditure", "Capital Transaction"],
    "Capital Account": ["Capital Transaction"],
    "Reserves & Surplus": ["Capital Transaction", "Year-end Adjustments"],
    "Secured Loans": ["Loan/Advance"],
    "Unsecured Loans": ["Loan/Advance"],
    "Bank OD A/c": ["Loan/Advance", "Contra/Bank Transfer", "Bank Charges"],
    "Loans & Advances (Asset)": ["Loan/Advance"],
    "Duties & Taxes": ["GST Payment", "GST Output", "GST Input", "Tax Adjustment",
                       "TDS Payment"],
    "Provisions": ["Provision/Write-off", "Year-end Adjustments"],
    "Sales Accounts": ["Sales/Revenue", "Sales Return"],
    "Purchase Accounts": ["Purchase/Material", "Purchase Return"],
    "Direct Expenses": ["Purchase/Material", "Contractor Payments"],
    "Indirect Expenses": ["Salary/Wages", "Rent Payments", "Professional/Consultancy",
                          "Contractor Payments", "Insurance", "Utility/Office",
                          "Travel/Conveyance", "Donation/CSR", "Bank Charges"],
    "Suspense A/c": ["Suspense/Clearing"],
    "Stock-in-Hand": ["Purchase/Material", "Sales/Revenue"],
    "Deposits (Asset)": ["Rent Payments", "Utility/Office"],
}


def cross_check_narration_vs_group(narration_category, ledger_groups):
    """Check if a narration category is consistent with the ledger groups.

    Args:
        narration_category: Category from regex/LLM classification
        ledger_groups: List of Tally group names involved in the voucher

    Returns:
        dict with: is_match (bool), severity (HIGH/MEDIUM/LOW/OK),
                   expected_categories, detail
    """
    mismatches = []

    for group in ledger_groups:
        expected = EXPECTED_NARRATION_FOR_GROUP.get(group, [])
        if not expected:
            continue  # No mapping defined for this group — skip

        if narration_category not in expected:
            mismatches.append({
                "group": group,
                "expected_categories": expected,
                "actual_category": narration_category,
            })

    if not mismatches:
        return {
            "is_match": True,
            "severity": "OK",
            "detail": "Narration category consistent with all ledger groups",
        }

    # Determine severity based on the nature of the mismatch
    severity = "LOW"
    for m in mismatches:
        group = m["group"]
        cat = m["actual_category"]

        # HIGH severity mismatches
        if group == "Fixed Assets" and cat not in ["Capital Expenditure"]:
            severity = "HIGH"
            break
        if group in ("Indirect Expenses", "Direct Expenses") and cat == "Capital Expenditure":
            severity = "HIGH"
            break
        if group == "Sundry Creditors" and cat == "Loan/Advance":
            severity = "HIGH"
            break
        if group == "Sundry Debtors" and cat == "Loan/Advance":
            severity = "HIGH"
            break
        if group == "Sales Accounts" and cat in ("Loan/Advance", "Capital Transaction"):
            severity = "HIGH"
            break
        if cat == "Related Party":
            severity = "HIGH"
            break

        # MEDIUM severity
        if severity != "HIGH":
            severity = "MEDIUM"

    return {
        "is_match": False,
        "severity": severity,
        "mismatches": mismatches,
        "detail": f"Narration says '{narration_category}' but ledger groups are: "
                  + ", ".join(m["group"] for m in mismatches),
    }
