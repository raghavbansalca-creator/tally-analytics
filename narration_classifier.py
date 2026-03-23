"""
Seven Labs Vision — Narration Classification Orchestrator
Master classifier that chains all layers in order:

  Layer 0: Group-Context Rules    (voucher type + ledger group → 60-70% coverage)
  Layer 1: Bank Statement Parser  (structured bank narrations → 15-20% of remainder)
  Layer 2: Regex on narration     (pattern matching on text → handles rest)
  Layer 3: LLM (future)           (ambiguous cases → final fallback)

Each layer adds its classification. If a higher layer already classified with
high confidence, lower layers are skipped (unless refinement is requested).

This module replaces the standalone narration_engine.classify_narration()
as the primary entry point for narration analysis.
"""

import sqlite3
from collections import defaultdict

from group_context_classifier import (
    classify_by_context,
    cross_check_narration_vs_group,
    build_ledger_group_map,
    PRIMARY_GROUPS,
)
from bank_statement_parser import (
    parse_bank_narration,
    classify_bank_transaction,
    fuzzy_match_party,
)
from narration_engine import classify_narration, CATEGORIES as REGEX_CATEGORIES


# ── SEVERITY ORDERING ───────────────────────────────────────────────────────
SEVERITY_ORDER = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}


# ── SINGLE VOUCHER CLASSIFICATION ───────────────────────────────────────────

def classify_voucher(
    narration,
    voucher_type,
    party_name,
    amount,
    legs,
    ledger_names=None,
):
    """Classify a single voucher using the full multi-layer pipeline.

    CRITICAL DESIGN: Group and Narration classify INDEPENDENTLY.
    The disagreement between them is the most valuable audit finding.

    - Group says "Indirect Expenses" → expects Salary/Rent/Professional/Office
    - Narration says "purchased laptop" → regex says Capital Expenditure
    - DISAGREEMENT = the grouping is likely WRONG (should be Fixed Assets)

    Args:
        narration: Narration text from trn_voucher.NARRATION
        voucher_type: Tally voucher type (Receipt, Payment, Sales, etc.)
        party_name: Party ledger name from trn_voucher.PARTYLEDGERNAME
        amount: Voucher amount (positive)
        legs: List of dicts with: ledger, amount, group, is_debit
              (from group_context_classifier.get_voucher_legs or equivalent)
        ledger_names: Optional list of all ledger names (for fuzzy matching)

    Returns:
        dict with:
            category: Primary classification (best available)
            group_says: What the ledger group implies
            narration_says: What the narration text implies
            verdict: AGREE / DISAGREE_GROUP_LIKELY_WRONG /
                     DISAGREE_NARRATION_UNCLEAR / NO_NARRATION
            confidence: 0.0-1.0
            method: Which layer produced the final category
            comment: Audit comment
            layers: Dict of all layer results for transparency
            cross_check: Detailed mismatch analysis
            needs_review: bool
            suggested_correct_group: If narration contradicts group, what group SHOULD it be?
    """
    result = {
        "category": "Uncategorized",
        "group_says": None,
        "narration_says": None,
        "verdict": "UNCLASSIFIED",
        "confidence": 0.0,
        "method": "none",
        "comment": "",
        "layers": {},
        "cross_check": None,
        "needs_review": False,
        "suggested_correct_group": None,
    }

    debit_groups = set(l["group"] for l in legs if l.get("is_debit"))
    credit_groups = set(l["group"] for l in legs if not l.get("is_debit"))
    all_groups = debit_groups | credit_groups

    # ════════════════════════════════════════════════════════════════════
    # STEP 1: Classify INDEPENDENTLY from BOTH sources
    # ════════════════════════════════════════════════════════════════════

    # ─── SOURCE A: Group-Context (what the ledger structure says) ──────
    group_category = None
    group_confidence = 0.0
    ctx_result = classify_by_context(voucher_type, legs)
    if ctx_result:
        result["layers"]["group_context"] = ctx_result
        group_category = ctx_result["category"]
        group_confidence = ctx_result["confidence"]
        result["group_says"] = group_category

    # ─── SOURCE B: Narration Text (what the operator intended) ────────
    narration_category = None
    narration_confidence = 0.0
    has_narration = bool(narration and narration.strip())

    # B1: Try bank statement parser first
    bank_parsed = parse_bank_narration(narration)
    if bank_parsed and bank_parsed.get("is_bank_narration"):
        result["layers"]["bank_parsed"] = bank_parsed
        bank_class = classify_bank_transaction(
            bank_parsed, voucher_type, debit_groups, credit_groups
        )
        if bank_class:
            result["layers"]["bank_classification"] = bank_class
            # Bank narrations are structured; their category relates to payment
            # mode, not the accounting nature. Store but don't use as narration_says
            # unless regex has nothing.

        # Fuzzy party matching
        if bank_parsed.get("party_extracted") and ledger_names:
            fuzzy = fuzzy_match_party(bank_parsed["party_extracted"], ledger_names)
            if fuzzy:
                result["layers"]["fuzzy_party_match"] = fuzzy

    # B2: Regex on narration text (the core narration intelligence)
    regex_matches = classify_narration(narration)
    if regex_matches:
        result["layers"]["regex_raw"] = regex_matches

        # Filter out meta-categories
        real_matches = [
            m for m in regex_matches
            if m["category"] not in ("No Narration", "Unusually Short Narration",
                                     "Unusually Long Narration")
        ]

        # Party-based cash detection
        if party_name and party_name.strip().upper() in (
            "CASH", "CASH A/C", "CASH ACCOUNT", "CASH IN HAND",
            "CASH-IN-HAND", "PETTY CASH",
        ):
            has_cash = any(m["category"] == "Cash Transactions" for m in real_matches)
            if not has_cash:
                real_matches.append({
                    "category": "Cash Transactions",
                    "comment": "Cash transaction — verify Sec 269ST/269SS",
                    "severity": "MEDIUM",
                })

        if real_matches:
            real_matches.sort(key=lambda m: SEVERITY_ORDER.get(m.get("severity", "LOW"), 9))
            primary_regex = real_matches[0]
            narration_category = primary_regex["category"]
            narration_confidence = 1.0 if len(real_matches) == 1 else 0.7

            result["layers"]["regex_primary"] = {
                "category": narration_category,
                "confidence": narration_confidence,
                "comment": primary_regex["comment"],
                "severity": primary_regex.get("severity", "LOW"),
                "all_matches": len(real_matches),
            }
            result["narration_says"] = narration_category

    if not has_narration:
        result["layers"]["regex_primary"] = {
            "category": "No Narration",
            "confidence": 0.0,
            "comment": "WARNING: No narration — transaction purpose unclear",
        }

    # ════════════════════════════════════════════════════════════════════
    # STEP 2: COMPARE the two independent classifications
    # ════════════════════════════════════════════════════════════════════

    if group_category and narration_category:
        # Both sources have an opinion — compare them
        if group_category == narration_category:
            # ── AGREE: Group and narration say the same thing ──────────
            result["verdict"] = "AGREE"
            result["category"] = group_category
            result["confidence"] = min(0.99, max(group_confidence, narration_confidence) + 0.10)
            result["method"] = "consensus"
            result["comment"] = (ctx_result or {}).get("comment", "")

        elif _categories_are_compatible(group_category, narration_category):
            # ── COMPATIBLE: Different names but not contradictory ──────
            # e.g., group says "Creditor Payment", narration says "Purchase/Material"
            # Both are valid for Sundry Creditors — narration is more specific
            result["verdict"] = "AGREE_NARRATION_MORE_SPECIFIC"
            result["category"] = narration_category  # narration is more specific
            result["confidence"] = min(0.95, narration_confidence + 0.05)
            result["method"] = "narration_refined"
            result["comment"] = (result["layers"].get("regex_primary", {}).get("comment", ""))

        else:
            # ── DISAGREE: Group and narration contradict each other ────
            # THIS IS THE KEY AUDIT FINDING
            result["verdict"] = "DISAGREE_POSSIBLE_MISCLASSIFICATION"
            result["needs_review"] = True

            # The narration reflects operator INTENT — it's more likely
            # to reveal the true nature of the transaction.
            # The group may be wrong (operator selected wrong ledger).
            result["category"] = narration_category
            result["confidence"] = narration_confidence
            result["method"] = "narration_overrides_group"

            # Build the audit finding
            result["comment"] = (
                f"MISMATCH: Narration implies '{narration_category}' but entry is "
                f"posted under '{', '.join(all_groups)}' which suggests "
                f"'{group_category}'. Possible misclassification — "
                f"verify if the ledger grouping is correct."
            )

            # Suggest what the correct group SHOULD be
            result["suggested_correct_group"] = _suggest_correct_group(narration_category)

            # Build cross-check detail
            result["cross_check"] = {
                "is_match": False,
                "severity": _mismatch_severity(group_category, narration_category),
                "group_classification": group_category,
                "narration_classification": narration_category,
                "current_groups": list(all_groups),
                "suggested_groups": _suggest_correct_group(narration_category),
                "detail": (
                    f"Group structure says '{group_category}' but narration "
                    f"text says '{narration_category}'. The narration suggests "
                    f"this entry may be incorrectly grouped."
                ),
            }

    elif group_category and not narration_category:
        if has_narration:
            # Group classified, narration didn't match any pattern
            result["verdict"] = "GROUP_ONLY_NARRATION_UNCLEAR"
            result["category"] = group_category
            result["confidence"] = group_confidence
            result["method"] = "group_context"
            result["comment"] = (ctx_result or {}).get("comment", "")
        else:
            # No narration at all — group is only source
            result["verdict"] = "NO_NARRATION"
            result["category"] = group_category
            result["confidence"] = group_confidence * 0.8  # penalise for missing narration
            result["method"] = "group_context"
            result["comment"] = (
                "No narration provided. Classification based on ledger group only. "
                "Transaction purpose cannot be independently verified from narration."
            )
            result["needs_review"] = True  # no narration = always review-worthy

    elif narration_category and not group_category:
        # Narration classified but no group rule matched
        result["verdict"] = "NARRATION_ONLY"
        result["category"] = narration_category
        result["confidence"] = narration_confidence
        result["method"] = "regex"
        result["comment"] = result["layers"].get("regex_primary", {}).get("comment", "")

        # Run cross-check: does the narration category fit the actual groups?
        cross = cross_check_narration_vs_group(narration_category, list(all_groups))
        result["cross_check"] = cross
        if not cross["is_match"]:
            result["needs_review"] = True
            result["verdict"] = "NARRATION_CONTRADICTS_GROUP"
            result["suggested_correct_group"] = _suggest_correct_group(narration_category)
            result["comment"] = (
                f"Narration says '{narration_category}' but ledger is under "
                f"'{', '.join(all_groups)}' — possible wrong grouping."
            )

    else:
        # Neither source could classify
        result["verdict"] = "UNCLASSIFIED"
        result["category"] = "Uncategorized"
        result["confidence"] = 0.0
        result["method"] = "none"

    # Ensure confidence is capped
    result["confidence"] = round(min(0.99, max(0.0, result["confidence"])), 2)

    return result


# ── HELPER: Check if two categories are compatible ──────────────────────────

# Some categories are different names for related concepts.
# "Creditor Payment" (from group) and "Purchase/Material" (from narration)
# are compatible because a purchase IS a creditor payment.
COMPATIBLE_PAIRS = {
    ("Creditor Payment", "Purchase/Material"),
    ("Creditor Payment", "Professional/Consultancy"),
    ("Creditor Payment", "Contractor Payments"),
    ("Creditor Payment", "Rent Payments"),
    ("Creditor Payment", "Insurance"),
    ("Creditor Payment", "Salary/Wages"),
    ("Creditor Payment", "Utility/Office"),
    ("Creditor Payment", "Travel/Conveyance"),
    ("Debtor Receipt", "Sales/Revenue"),
    ("Utility/Office", "Salary/Wages"),
    ("Utility/Office", "Rent Payments"),
    ("Utility/Office", "Professional/Consultancy"),
    ("Utility/Office", "Contractor Payments"),
    ("Utility/Office", "Insurance"),
    ("Utility/Office", "Bank Charges"),
    ("Utility/Office", "Travel/Conveyance"),
    ("Utility/Office", "Donation/CSR"),
    ("Purchase/Material", "Contractor Payments"),
    ("GST Payment", "GST Output"),
    ("GST Payment", "GST Input"),
    ("GST Payment", "Tax Adjustment"),
    ("GST Payment", "TDS Payment"),
    ("Capital Expenditure", "Capital Transaction"),
    ("Loan/Advance", "Capital Transaction"),
    ("Contra/Bank Transfer", "Bank Charges"),
}


def _categories_are_compatible(cat_a, cat_b):
    """Check if two categories, while named differently, are not contradictory."""
    if cat_a == cat_b:
        return True
    return (cat_a, cat_b) in COMPATIBLE_PAIRS or (cat_b, cat_a) in COMPATIBLE_PAIRS


# ── HELPER: Suggest correct group based on narration category ───────────────

NARRATION_TO_EXPECTED_GROUP = {
    "Capital Expenditure": "Fixed Assets / Investments",
    "Salary/Wages": "Indirect Expenses (Salary sub-group)",
    "Rent Payments": "Indirect Expenses (Rent sub-group)",
    "Professional/Consultancy": "Indirect Expenses (Professional Fees sub-group)",
    "Contractor Payments": "Direct Expenses or Indirect Expenses (Contract sub-group)",
    "Loan/Advance": "Loans & Advances (Asset) or Secured/Unsecured Loans (Liability)",
    "Related Party": "Depends on nature — must be separately disclosed under AS-18/Ind AS 24",
    "Provision/Write-off": "Provisions (Current Liabilities) or Indirect Expenses",
    "Reversal/Correction": "Same group as the original entry being reversed",
    "Suspense/Clearing": "Suspense A/c — must be cleared before year-end",
    "Sales/Revenue": "Sales Accounts or Direct Incomes",
    "Purchase/Material": "Purchase Accounts or Direct Expenses",
    "Debtor Receipt": "Bank Accounts (debit) + Sundry Debtors (credit)",
    "Creditor Payment": "Sundry Creditors (debit) + Bank Accounts (credit)",
    "Insurance": "Indirect Expenses (Insurance sub-group) or Prepaid (if multi-year)",
    "Donation/CSR": "Indirect Expenses (Donation/CSR sub-group)",
    "Foreign/Forex": "Depends on nature — verify FEMA compliance",
    "GST Payment": "Duties & Taxes (Current Liabilities)",
    "TDS Payment": "Duties & Taxes (Current Liabilities or Current Assets)",
    "Bank Charges": "Indirect Expenses (Bank Charges sub-group)",
    "Cash Transactions": "Cash-in-Hand (Current Assets)",
    "Year-end Adjustments": "Prepaid (Assets) or Outstanding (Liabilities)",
    "Inter-company/Branch": "Branch / Divisions or relevant inter-company accounts",
}


def _suggest_correct_group(narration_category):
    """Given a narration category, suggest what Tally group the entry should be in."""
    return NARRATION_TO_EXPECTED_GROUP.get(narration_category, "Review required — no standard mapping")


# ── HELPER: Determine mismatch severity ─────────────────────────────────────

def _mismatch_severity(group_category, narration_category):
    """Determine how serious a group-vs-narration disagreement is."""
    # HIGH: Fundamental misclassification affecting financial statement structure
    high_mismatches = {
        # Capital vs Revenue (affects P&L and Balance Sheet)
        ("Utility/Office", "Capital Expenditure"),
        ("Purchase/Material", "Capital Expenditure"),
        ("Creditor Payment", "Capital Expenditure"),
        # Loan vs Current (affects classification of liabilities)
        ("Creditor Payment", "Loan/Advance"),
        # Revenue vs Liability (affects revenue recognition)
        ("Sales/Revenue", "Loan/Advance"),
        ("Debtor Receipt", "Loan/Advance"),
        # Related party in any non-RP group
        ("Creditor Payment", "Related Party"),
        ("Debtor Receipt", "Related Party"),
        ("Utility/Office", "Related Party"),
        ("Loan/Advance", "Related Party"),
        ("Capital Transaction", "Related Party"),
        # Provision not in provision group
        ("Utility/Office", "Provision/Write-off"),
    }

    if (group_category, narration_category) in high_mismatches or \
       (narration_category, group_category) in high_mismatches:
        return "HIGH"

    # MEDIUM: Material but not structural
    return "MEDIUM"


# ── BATCH CLASSIFICATION ────────────────────────────────────────────────────

def classify_all(db_path, from_date=None, to_date=None):
    """Classify ALL vouchers in the database using the full pipeline.
    DEFENSIVE: Handles missing tables, empty data. Never crashes.

    Returns:
        dict with:
            results: list of classification results per voucher
            stats: summary statistics
    """
    _empty_result = {
        "results": [],
        "stats": {
            "total": 0, "by_method": {}, "by_category": {}, "by_verdict": {},
            "needs_review": 0, "cross_check_mismatches": 0,
            "high_severity_mismatches": 0, "group_narration_disagreements": 0,
            "no_narration": 0, "bank_narrations": 0,
        },
    }
    try:
        conn = sqlite3.connect(db_path)
    except Exception:
        return _empty_result
    conn.row_factory = sqlite3.Row

    # Check required tables exist
    try:
        tbl_check = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='trn_voucher'"
        ).fetchone()
        if not tbl_check or tbl_check[0] == 0:
            conn.close()
            return _empty_result
    except Exception:
        conn.close()
        return _empty_result

    try:
        ledger_group_map = build_ledger_group_map(conn)
    except Exception:
        ledger_group_map = {}
    ledger_names = list(ledger_group_map.keys())

    # Date filter
    date_filter = ""
    params = []
    if from_date:
        date_filter += " AND v.DATE >= ?"
        params.append(from_date)
    if to_date:
        date_filter += " AND v.DATE <= ?"
        params.append(to_date)

    # Fetch all vouchers
    cur = conn.cursor()
    cur.execute(f"""
        SELECT v.GUID, v.DATE, v.VOUCHERTYPENAME, v.NARRATION,
               v.PARTYLEDGERNAME, v.VOUCHERNUMBER
        FROM trn_voucher v
        WHERE 1=1 {date_filter}
        ORDER BY v.DATE
    """, params)
    vouchers = cur.fetchall()

    # Pre-fetch all accounting entries
    cur.execute("SELECT VOUCHER_GUID, LEDGERNAME, AMOUNT FROM trn_accounting")
    voucher_legs_raw = defaultdict(list)
    for row in cur.fetchall():
        guid = row[0]
        ledger = row[1] or ""
        try:
            amount = float(row[2] or 0)
        except (ValueError, TypeError):
            amount = 0.0
        group = ledger_group_map.get(ledger, "")
        voucher_legs_raw[guid].append({
            "ledger": ledger,
            "amount": amount,
            "group": group,
            "primary_side": PRIMARY_GROUPS.get(group, "OTHER"),
            "is_debit": amount < 0,
        })

    conn.close()

    # Classify each voucher
    results = []
    stats = {
        "total": 0,
        "by_method": defaultdict(int),
        "by_category": defaultdict(int),
        "by_verdict": defaultdict(int),
        "needs_review": 0,
        "cross_check_mismatches": 0,
        "high_severity_mismatches": 0,
        "group_narration_disagreements": 0,
        "no_narration": 0,
        "bank_narrations": 0,
    }

    for vch in vouchers:
        guid = vch["GUID"]
        narration = vch["NARRATION"] or ""
        vtype = vch["VOUCHERTYPENAME"] or ""
        party = vch["PARTYLEDGERNAME"] or ""
        legs = voucher_legs_raw.get(guid, [])

        amount = sum(abs(l["amount"]) for l in legs if l["is_debit"])

        classification = classify_voucher(
            narration=narration,
            voucher_type=vtype,
            party_name=party,
            amount=amount,
            legs=legs,
            ledger_names=ledger_names,
        )

        result_row = {
            "guid": guid,
            "date": vch["DATE"] or "",
            "voucher_type": vtype,
            "voucher_number": vch["VOUCHERNUMBER"] or "",
            "party": party,
            "narration": narration,
            "amount": amount,
            **classification,
        }
        results.append(result_row)

        # Update stats
        stats["total"] += 1
        stats["by_method"][classification["method"]] += 1
        stats["by_category"][classification["category"]] += 1
        stats["by_verdict"][classification.get("verdict", "UNKNOWN")] += 1
        if classification["needs_review"]:
            stats["needs_review"] += 1
        if classification.get("cross_check") and not classification["cross_check"].get("is_match", True):
            stats["cross_check_mismatches"] += 1
            if classification["cross_check"].get("severity") == "HIGH":
                stats["high_severity_mismatches"] += 1
        if classification.get("verdict") == "DISAGREE_POSSIBLE_MISCLASSIFICATION":
            stats["group_narration_disagreements"] += 1
        if not narration or not narration.strip():
            stats["no_narration"] += 1
        if classification["layers"].get("bank_parsed"):
            stats["bank_narrations"] += 1

    # Convert defaultdicts
    stats["by_method"] = dict(stats["by_method"])
    stats["by_category"] = dict(stats["by_category"])
    stats["by_verdict"] = dict(stats["by_verdict"])

    return {
        "results": results,
        "stats": stats,
    }
