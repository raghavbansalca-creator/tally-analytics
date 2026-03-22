"""
Seven Labs Vision — Bank Statement Narration Parser (Layer 1)
Parses structured bank statement narrations that are auto-imported into Tally.

These narrations follow bank-specific formats:
  NEFT CR-UBIN0908941-MS NARESH MEDICAL AG
  CHQ PAID-TRANSFER IN-TORRENTPHARMACEUTI
  IB FUNDS TRANSFER DR-50200076575313-ROHIT PHARMA
  ACH-CR-HYUNDAI MOTOR INDIA-NACH-689449
  TRF/HAI MAA MEDICO/trf
  CLG/024238/140525/Central Ba /

This module extracts:
  - Payment mode (NEFT, RTGS, IMPS, CHQ, ACH, UPI, IB Transfer, etc.)
  - Direction (credit = money in, debit = money out)
  - Counterparty name (often truncated)
  - Reference numbers

It does NOT classify the transaction — that's done by the orchestrator
using the extracted info + group context.
"""

import re


# ── BANK NARRATION PATTERNS ─────────────────────────────────────────────────
# Ordered by specificity (most specific first).

BANK_PATTERNS = [
    # NEFT transfers
    {
        "pattern": re.compile(
            r"NEFT[\s/]*(?:CR|DR|MB)?[\s/-]*"
            r"(?:(?P<ifsc>[A-Z]{4}\d{7})[\s/-]*)?"
            r"(?P<party>.+?)(?:\s*/\s*|\s*$)",
            re.IGNORECASE,
        ),
        "mode": "NEFT",
        "direction_from_text": True,  # CR/DR in the narration
    },
    # RTGS transfers
    {
        "pattern": re.compile(
            r"RTGS[\s/]*(?:CR|DR)?[\s/-]*"
            r"(?:(?P<ifsc>[A-Z]{4}\d{7})[\s/-]*)?"
            r"(?P<party>.+?)(?:\s*/\s*|\s*$)",
            re.IGNORECASE,
        ),
        "mode": "RTGS",
        "direction_from_text": True,
    },
    # IMPS transfers
    {
        "pattern": re.compile(
            r"IMPS[\s/]*(?:CR|DR)?[\s/-]*"
            r"(?P<ref>\d+)?[\s/-]*"
            r"(?P<party>.+?)(?:\s*/\s*|\s*$)",
            re.IGNORECASE,
        ),
        "mode": "IMPS",
        "direction_from_text": True,
    },
    # UPI transfers
    {
        "pattern": re.compile(
            r"UPI[\s/]*(?:CR|DR)?[\s/-]*"
            r"(?P<ref>\d+)?[\s/-]*"
            r"(?P<party>.+?)(?:\s*/\s*|\s*$)",
            re.IGNORECASE,
        ),
        "mode": "UPI",
        "direction_from_text": True,
    },
    # IB (Internet Banking) Funds Transfer
    {
        "pattern": re.compile(
            r"IB\s+FUNDS?\s+TRANSFER\s+(?P<direction>CR|DR)[\s/-]*"
            r"(?P<ref>[\d]+)?[\s/-]*"
            r"(?P<party>.+?)$",
            re.IGNORECASE,
        ),
        "mode": "IB_TRANSFER",
        "direction_from_text": True,
    },
    # Cheque paid (outward)
    {
        "pattern": re.compile(
            r"CHQ\s+PAID[\s/-]*"
            r"(?:TRANSFER\s+IN|INWARD\s+TRAN|MICR\s+CTS|CTS\s+S\d+)?[\s/-]*"
            r"(?P<party>.+?)$",
            re.IGNORECASE,
        ),
        "mode": "CHEQUE",
        "direction": "DR",
    },
    # Cheque deposit (inward)
    {
        "pattern": re.compile(
            r"CHQ\s+DEP[\s/-]*"
            r"(?:MICR\s+CLG|CTS\s+CLG\d*|MICR)?[\s/-]*"
            r"(?:WBO)?[\s/-]*"
            r"(?P<city>[A-Z]+)?[\s:/-]*"
            r"(?P<party>.+?)(?:\s*:\s*(?P<bank>.+?))?$",
            re.IGNORECASE,
        ),
        "mode": "CHEQUE",
        "direction": "CR",
    },
    # ACH (Automated Clearing House)
    {
        "pattern": re.compile(
            r"ACH[\s/-]*(?P<direction>CR|DR)[\s/-]*"
            r"(?P<party>.+?)[\s/-]*"
            r"(?:NACH|NACHCN|NACHDR)?[\s/-]*"
            r"(?P<ref>[\d]+)?",
            re.IGNORECASE,
        ),
        "mode": "ACH",
        "direction_from_text": True,
    },
    # Clearing (CLG)
    {
        "pattern": re.compile(
            r"CLG[\s/]*(?P<ref>\d+)[\s/]*"
            r"(?P<date>\d{6})?[\s/]*"
            r"(?P<party>.+?)(?:\s*/\s*|\s*$)",
            re.IGNORECASE,
        ),
        "mode": "CLEARING",
        "direction": "CR",
    },
    # Transfer (TRF) — generic
    {
        "pattern": re.compile(
            r"(?:TRF|TPT)[\s/]*(?P<party>.+?)[\s/]*(?:trf|$)",
            re.IGNORECASE,
        ),
        "mode": "TRANSFER",
        "direction_from_text": False,
    },
    # Account number based (e.g., "50200007299610-TPT-SHIKHER-SHIKHER PHARMA")
    {
        "pattern": re.compile(
            r"(?P<accnum>\d{10,20})[\s/-]*(?:TPT|TRF)?[\s/-]*"
            r"(?P<party>.+?)$",
            re.IGNORECASE,
        ),
        "mode": "ACCOUNT_TRANSFER",
        "direction_from_text": False,
    },
]


# ── PARSER ──────────────────────────────────────────────────────────────────

def parse_bank_narration(narration):
    """Parse a bank statement narration.

    Args:
        narration: Raw narration text from Tally voucher.

    Returns:
        dict with: is_bank_narration (bool), mode, direction (CR/DR/UNKNOWN),
                   party_extracted, reference, raw_narration
        Returns None if narration doesn't match any bank pattern.
    """
    if not narration or len(narration.strip()) < 5:
        return None

    text = narration.strip()

    for bp in BANK_PATTERNS:
        match = bp["pattern"].search(text)
        if not match:
            continue

        groups = match.groupdict()

        # Extract direction
        direction = "UNKNOWN"
        if bp.get("direction_from_text"):
            dir_match = re.search(r"\b(CR|DR)\b", text, re.IGNORECASE)
            if dir_match:
                direction = dir_match.group(1).upper()
            elif "direction" in groups and groups["direction"]:
                direction = groups["direction"].upper()
        elif "direction" in bp:
            direction = bp["direction"]

        # Extract and clean party name
        party = groups.get("party", "").strip()
        # Remove trailing reference numbers and bank codes
        party = re.sub(r"[\s/-]*(?:UTIB|HDFC|ICIC|SBIN|BARB|UBIN|CNRB)\d*\s*$", "", party, flags=re.IGNORECASE)
        # Remove trailing tab characters and whitespace
        party = re.sub(r"[\t\s]+$", "", party)
        # Remove leading/trailing dashes and slashes
        party = party.strip("-/ \t")

        return {
            "is_bank_narration": True,
            "mode": bp["mode"],
            "direction": direction,
            "party_extracted": party if len(party) > 2 else "",
            "reference": groups.get("ref", "") or "",
            "ifsc": groups.get("ifsc", "") or "",
            "city": groups.get("city", "") or "",
            "bank": groups.get("bank", "") or "",
            "raw_narration": text,
        }

    return None


def classify_bank_transaction(parsed, voucher_type, debit_groups, credit_groups):
    """Classify a parsed bank narration using payment mode + direction + groups.

    Args:
        parsed: Output from parse_bank_narration()
        voucher_type: Tally voucher type
        debit_groups: Set of debit-side Tally groups
        credit_groups: Set of credit-side Tally groups

    Returns:
        dict with: category, confidence, comment, method
        or None if cannot classify.
    """
    if not parsed or not parsed.get("is_bank_narration"):
        return None

    mode = parsed["mode"]
    direction = parsed["direction"]

    # ACH credits are often dividends, interest, or EMI credits
    if mode == "ACH" and direction == "CR":
        return {
            "category": "Debtor Receipt",
            "confidence": 0.70,
            "comment": f"ACH credit ({parsed['party_extracted']}) — verify source: dividend / interest / collection",
            "method": "bank_parser",
            "payment_mode": mode,
        }

    if mode == "ACH" and direction == "DR":
        return {
            "category": "Creditor Payment",
            "confidence": 0.70,
            "comment": f"ACH debit ({parsed['party_extracted']}) — verify: EMI / insurance / subscription auto-debit",
            "method": "bank_parser",
            "payment_mode": mode,
        }

    # IB Transfer between own accounts = Contra
    if mode == "IB_TRANSFER":
        if "Bank Accounts" in debit_groups and ("Bank Accounts" in credit_groups or "Bank OD A/c" in credit_groups):
            return {
                "category": "Contra/Bank Transfer",
                "confidence": 0.95,
                "comment": "Inter-bank fund transfer — verify bank reconciliation",
                "method": "bank_parser",
                "payment_mode": mode,
            }

    # NEFT/RTGS/IMPS with debtor group = collection
    if mode in ("NEFT", "RTGS", "IMPS", "UPI") and direction == "CR":
        if "Sundry Debtors" in credit_groups:
            return {
                "category": "Debtor Receipt",
                "confidence": 0.90,
                "comment": f"{mode} receipt from {parsed['party_extracted']} — verify outstanding matching",
                "method": "bank_parser",
                "payment_mode": mode,
            }

    # NEFT/RTGS/IMPS with creditor group = payment
    if mode in ("NEFT", "RTGS", "IMPS", "UPI") and direction == "DR":
        if "Sundry Creditors" in debit_groups:
            return {
                "category": "Creditor Payment",
                "confidence": 0.90,
                "comment": f"{mode} payment to {parsed['party_extracted']} — verify bill reconciliation",
                "method": "bank_parser",
                "payment_mode": mode,
            }

    # Cheque with party context
    if mode == "CHEQUE":
        if direction == "CR" and "Sundry Debtors" in credit_groups:
            return {
                "category": "Debtor Receipt",
                "confidence": 0.85,
                "comment": f"Cheque deposit from {parsed['party_extracted']} — verify clearing status",
                "method": "bank_parser",
                "payment_mode": mode,
            }
        if direction == "DR" and "Sundry Creditors" in debit_groups:
            return {
                "category": "Creditor Payment",
                "confidence": 0.85,
                "comment": f"Cheque payment to {parsed['party_extracted']} — verify clearing",
                "method": "bank_parser",
                "payment_mode": mode,
            }

    # Generic: use direction + groups for best guess
    if direction == "CR":
        return {
            "category": "Debtor Receipt",
            "confidence": 0.60,
            "comment": f"{mode} credit ({parsed['party_extracted']}) — verify nature of receipt",
            "method": "bank_parser",
            "payment_mode": mode,
        }
    if direction == "DR":
        return {
            "category": "Creditor Payment",
            "confidence": 0.60,
            "comment": f"{mode} debit ({parsed['party_extracted']}) — verify nature of payment",
            "method": "bank_parser",
            "payment_mode": mode,
        }

    return None


# ── FUZZY PARTY MATCHING ────────────────────────────────────────────────────

def fuzzy_match_party(extracted_name, ledger_names, threshold=80):
    """Match a truncated bank narration party name to known ledger names.

    Uses simple substring + length-ratio matching (no external dependency).
    For better results, install rapidfuzz: pip install rapidfuzz

    Args:
        extracted_name: Party name from bank narration (often truncated)
        ledger_names: List of known ledger names from mst_ledger
        threshold: Minimum similarity score (0-100)

    Returns:
        dict with: matched_ledger, score, method
        or None if no match found.
    """
    if not extracted_name or len(extracted_name) < 3:
        return None

    name_upper = extracted_name.upper().strip()

    # Try rapidfuzz if available (best results)
    try:
        from rapidfuzz import fuzz, process
        result = process.extractOne(
            name_upper,
            [l.upper() for l in ledger_names],
            scorer=fuzz.partial_ratio,
            score_cutoff=threshold,
        )
        if result:
            # Find original case ledger name
            idx = [l.upper() for l in ledger_names].index(result[0])
            return {
                "matched_ledger": ledger_names[idx],
                "score": result[1],
                "method": "rapidfuzz_partial_ratio",
            }
    except ImportError:
        pass

    # Fallback: simple substring matching
    best_match = None
    best_score = 0

    for ledger in ledger_names:
        ledger_upper = ledger.upper().strip()

        # Exact substring match (bank truncated the name)
        if name_upper in ledger_upper or ledger_upper in name_upper:
            # Score based on length ratio
            shorter = min(len(name_upper), len(ledger_upper))
            longer = max(len(name_upper), len(ledger_upper))
            score = (shorter / longer) * 100 if longer > 0 else 0

            if score > best_score and score >= threshold * 0.8:  # slightly lower threshold for substring
                best_score = score
                best_match = ledger

        # First N characters match (common truncation pattern)
        elif len(name_upper) >= 5:
            match_len = min(len(name_upper), len(ledger_upper))
            prefix_match = sum(
                1 for a, b in zip(name_upper[:match_len], ledger_upper[:match_len]) if a == b
            )
            score = (prefix_match / match_len) * 100 if match_len > 0 else 0

            if score > best_score and score >= threshold:
                best_score = score
                best_match = ledger

    if best_match:
        return {
            "matched_ledger": best_match,
            "score": round(best_score, 1),
            "method": "substring_match",
        }

    return None
