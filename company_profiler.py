"""
Seven Labs Vision — Company Profiler (Layer 1)
Auto-detects entity type, business nature, industry, and complexity from Tally data.
Runs after every sync and stores the profile in SQLite.

DEFENSIVE: Handles missing tables, missing columns, very small/large companies.
"""

import sqlite3
import re
from collections import Counter
from defensive_helpers import (
    table_exists, column_exists, get_table_columns,
    safe_fetchone, safe_fetchall, safe_float, safe_divide
)


def profile_company(db_path="tally_data.db"):
    """
    Analyze the Tally data and return a comprehensive company profile.
    Returns a dict with: entity_type, business_nature, industry, complexity, and detailed signals.
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
    except Exception as e:
        return {
            "entity_type": "Unknown", "business_nature": "Unknown",
            "industry": "General", "complexity": "Unknown",
            "complexity_score": 0, "company_name": "",
            "gstin": "", "gst_registration_type": "", "state": "",
            "signals": {}, "features": {}, "stats": {},
            "recommendations": [], "error": str(e),
        }

    profile = {
        "entity_type": None,          # Proprietorship | Partnership | LLP | Pvt Ltd | Public Ltd | Trust | HUF
        "business_nature": None,       # Trading | Manufacturing | Service | Mixed
        "industry": None,              # Pharma | Real Estate | Jewellery | Transport | Hospital | Education | Restaurant | Import/Export | General
        "complexity": None,            # Simple | Moderate | Complex
        "complexity_score": 0,
        "company_name": "",
        "gstin": "",
        "gst_registration_type": "",
        "state": "",
        "signals": {},                 # Detailed detection signals
        "features": {},                # Feature flags detected
        "stats": {},                   # Volume statistics
        "recommendations": [],         # What analyses are relevant
    }

    # ── GET COMPANY BASICS ──
    try:
        if table_exists(conn, "_metadata"):
            cur.execute("SELECT value FROM _metadata WHERE key='company_name'")
            row = cur.fetchone()
            if row:
                profile["company_name"] = row[0]
    except Exception:
        pass

    # Get GSTIN and registration type from first voucher
    try:
        if table_exists(conn, "trn_voucher"):
            voucher_cols = get_table_columns(conn, "trn_voucher")
            gstin_cols = []
            if "CMPGSTIN" in voucher_cols:
                gstin_cols.append("CMPGSTIN")
            if "CMPGSTREGISTRATIONTYPE" in voucher_cols:
                gstin_cols.append("CMPGSTREGISTRATIONTYPE")
            if "CMPGSTSTATE" in voucher_cols:
                gstin_cols.append("CMPGSTSTATE")
            if gstin_cols:
                sql = f"SELECT {', '.join(gstin_cols)} FROM trn_voucher WHERE "
                if "CMPGSTIN" in voucher_cols:
                    sql += "CMPGSTIN IS NOT NULL AND CMPGSTIN != '' "
                else:
                    sql += "1=1 "
                sql += "LIMIT 1"
                cur.execute(sql)
                row = cur.fetchone()
                if row:
                    if "CMPGSTIN" in voucher_cols:
                        profile["gstin"] = (row["CMPGSTIN"] or "") if "CMPGSTIN" in gstin_cols else ""
                    if "CMPGSTREGISTRATIONTYPE" in voucher_cols:
                        profile["gst_registration_type"] = (row["CMPGSTREGISTRATIONTYPE"] or "") if "CMPGSTREGISTRATIONTYPE" in gstin_cols else ""
                    if "CMPGSTSTATE" in voucher_cols:
                        profile["state"] = (row["CMPGSTSTATE"] or "") if "CMPGSTSTATE" in gstin_cols else ""
    except Exception:
        pass

    # ── VOLUME STATISTICS ──
    stats = {}
    for tbl in ["mst_group", "mst_ledger", "mst_stock_item", "mst_godown", "mst_voucher_type", "trn_voucher", "trn_accounting"]:
        try:
            if table_exists(conn, tbl):
                cur.execute(f"SELECT COUNT(*) FROM {tbl}")
                row = cur.fetchone()
                stats[tbl] = row[0] if row else 0
            else:
                stats[tbl] = 0
        except Exception:
            stats[tbl] = 0

    profile["stats"] = stats

    # ── DETECT ENTITY TYPE ──
    profile["entity_type"], profile["signals"]["entity_type"] = _detect_entity_type(cur, profile)

    # ── DETECT BUSINESS NATURE ──
    profile["business_nature"], profile["signals"]["business_nature"] = _detect_business_nature(cur, profile)

    # ── DETECT INDUSTRY ──
    profile["industry"], profile["signals"]["industry"] = _detect_industry(cur, profile)

    # ── DETECT COMPLEXITY ──
    profile["complexity"], profile["complexity_score"], profile["features"] = _detect_complexity(cur, profile)

    # ── GENERATE RECOMMENDATIONS ──
    profile["recommendations"] = _generate_recommendations(profile)

    # ── SAVE PROFILE TO DB ──
    _save_profile(conn, profile)

    conn.close()
    return profile


# ════════════════════════════════════════════════════════════════════════════
# ENTITY TYPE DETECTION
# ════════════════════════════════════════════════════════════════════════════

def _detect_entity_type(cur, profile):
    """Detect: Proprietorship | Partnership | LLP | Pvt Ltd | Public Ltd | Trust | HUF"""
    signals = {}
    scores = Counter()

    # Signal 1: PAN 4th character (from GSTIN)
    gstin = profile.get("gstin", "")
    if len(gstin) >= 12:
        pan = gstin[2:12]  # GSTIN format: 2-digit state + 10-digit PAN + 1Z + checkdigit
        pan_4th = pan[3] if len(pan) >= 4 else ""
        signals["pan_4th_char"] = pan_4th
        pan_map = {
            "P": "Proprietorship",   # Individual
            "F": "Partnership",       # Firm
            "C": "Company",           # Company (Pvt or Public)
            "H": "HUF",
            "T": "Trust",
            "A": "AOP/BOI",
            "L": "Local Authority",
            "J": "Artificial Juridical Person",
            "G": "Government",
        }
        if pan_4th in pan_map:
            signals["pan_entity"] = pan_map[pan_4th]
            scores[pan_map[pan_4th]] += 10  # Strong signal

    # Signal 2: Capital Account ledger patterns
    try:
        cur.execute("""
            SELECT NAME, PARENT FROM mst_ledger
            WHERE UPPER(PARENT) IN ('CAPITAL ACCOUNT', 'CAPITAL ACCOUNTS')
            OR UPPER(NAME) LIKE '%CAPITAL%'
            OR UPPER(NAME) LIKE '%DRAWING%'
            OR UPPER(NAME) LIKE '%SHARE CAPITAL%'
        """)
        capital_ledgers = [dict(row) for row in cur.fetchall()]
        signals["capital_ledgers"] = [l["NAME"] for l in capital_ledgers]

        partner_count = 0
        has_share_capital = False
        has_proprietor = False
        has_drawings = False

        for l in capital_ledgers:
            name_upper = l["NAME"].upper()
            if "SHARE CAPITAL" in name_upper or "EQUITY" in name_upper or "PREFERENCE" in name_upper:
                has_share_capital = True
            if "PROPRIETOR" in name_upper:
                has_proprietor = True
            if "PARTNER" in name_upper and "CAPITAL" in name_upper:
                partner_count += 1
            if "DRAWING" in name_upper:
                has_drawings = True

        if has_share_capital:
            scores["Company"] += 8
            signals["has_share_capital"] = True
        if partner_count >= 2:
            scores["Partnership"] += 8
            signals["partner_capital_count"] = partner_count
        if has_proprietor:
            scores["Proprietorship"] += 8
        if partner_count == 0 and not has_share_capital and len(capital_ledgers) <= 2:
            scores["Proprietorship"] += 3
    except:
        pass

    # Signal 3: Company name patterns
    company_name = profile.get("company_name", "").upper()
    if "PVT" in company_name or "PRIVATE" in company_name:
        scores["Pvt Ltd"] += 6
        signals["name_has_pvt"] = True
    elif "LTD" in company_name or "LIMITED" in company_name:
        scores["Public Ltd"] += 6
        signals["name_has_ltd"] = True
    if "LLP" in company_name:
        scores["LLP"] += 8
        signals["name_has_llp"] = True
    if "TRUST" in company_name or "FOUNDATION" in company_name:
        scores["Trust"] += 6
    if "HUF" in company_name:
        scores["HUF"] += 8
    if "& SONS" in company_name or "& CO" in company_name or "ASSOCIATES" in company_name:
        scores["Partnership"] += 3

    # Signal 4: Ledger patterns for company vs others
    try:
        cur.execute("SELECT NAME FROM mst_ledger WHERE UPPER(NAME) LIKE '%DIRECTOR%REMUNERATION%' OR UPPER(NAME) LIKE '%ROC%' OR UPPER(NAME) LIKE '%AUDIT FEE%'")
        company_ledgers = [row[0] for row in cur.fetchall()]
        if company_ledgers:
            scores["Company"] += 4
            signals["company_indicator_ledgers"] = company_ledgers
    except:
        pass

    # Signal 5: GST registration type
    gst_reg = profile.get("gst_registration_type", "").upper()
    if gst_reg == "COMPOSITION":
        scores["Proprietorship"] += 2  # Small businesses often composition
        signals["gst_composition"] = True
    elif gst_reg == "INPUT SERVICE DISTRIBUTOR":
        scores["Company"] += 3  # ISD = multi-location = typically company
        signals["gst_isd"] = True

    # Determine result
    if not scores:
        entity_type = "Unknown"
    else:
        top = scores.most_common(1)[0][0]
        # Refine Company into Pvt Ltd or Public Ltd
        if top == "Company":
            if scores.get("Pvt Ltd", 0) > scores.get("Public Ltd", 0):
                entity_type = "Pvt Ltd"
            elif scores.get("Public Ltd", 0) > 0:
                entity_type = "Public Ltd"
            else:
                entity_type = "Pvt Ltd"  # Default assumption
        else:
            entity_type = top

    signals["scores"] = dict(scores)
    return entity_type, signals


# ════════════════════════════════════════════════════════════════════════════
# BUSINESS NATURE DETECTION
# ════════════════════════════════════════════════════════════════════════════

def _detect_business_nature(cur, profile):
    """Detect: Trading | Manufacturing | Service | Mixed"""
    signals = {}
    scores = Counter()

    # Signal 1: Stock Groups hierarchy — check for RM / WIP / FG separation
    try:
        cur.execute("SELECT NAME, PARENT FROM mst_group")
        groups = {row["NAME"].upper(): row["PARENT"] for row in cur.fetchall()}
        signals["all_groups"] = list(groups.keys())

        manufacturing_groups = ["RAW MATERIALS", "WORK-IN-PROGRESS", "WORK IN PROGRESS",
                                "WIP", "FINISHED GOODS", "SEMI-FINISHED GOODS",
                                "CONSUMABLES", "PACKING MATERIAL"]
        found_mfg_groups = [g for g in manufacturing_groups if g in groups]
        if found_mfg_groups:
            scores["Manufacturing"] += len(found_mfg_groups) * 3
            signals["manufacturing_groups"] = found_mfg_groups
    except:
        pass

    # Signal 2: Voucher types — check ACTUAL USAGE, not just type definitions
    # (TONOTO has Job Work types defined but never uses them)
    try:
        cur.execute("SELECT NAME, PARENT FROM mst_voucher_type")
        vtypes = [(row["NAME"].upper(), (row["PARENT"] or "").upper()) for row in cur.fetchall()]
        signals["voucher_types"] = [v[0] for v in vtypes]

        # Only count manufacturing if types are ACTUALLY USED in vouchers
        mfg_vch_count = 0
        if column_exists(conn, "trn_voucher", "MFGJOURNAL"):
            cur.execute("SELECT COUNT(*) FROM trn_voucher WHERE UPPER(MFGJOURNAL) = 'YES'")
            row = cur.fetchone()
            mfg_vch_count = row[0] if row else 0
        if mfg_vch_count > 0:
            scores["Manufacturing"] += 10  # Strong signal — actual manufacturing entries
            signals["manufacturing_voucher_count"] = mfg_vch_count

        # Check if manufacturing voucher types have actual transactions
        cur.execute("""
            SELECT VOUCHERTYPENAME, COUNT(*) as cnt FROM trn_voucher
            WHERE UPPER(VOUCHERTYPENAME) IN ('STOCK JOURNAL', 'MANUFACTURING JOURNAL')
            GROUP BY VOUCHERTYPENAME
        """)
        used_mfg_types = {row[0]: row[1] for row in cur.fetchall()}
        if used_mfg_types:
            scores["Manufacturing"] += sum(min(v, 5) for v in used_mfg_types.values())
            signals["used_manufacturing_types"] = used_mfg_types
        else:
            # Types exist but not used — weak signal, just +1
            mfg_vtypes = [v for v in vtypes if any(kw in v[0] for kw in
                          ["MANUFACTURING", "PRODUCTION", "JOB WORK", "STOCK JOURNAL"])]
            if mfg_vtypes:
                scores["Manufacturing"] += 1  # Weak — defined but unused
                signals["defined_but_unused_mfg_types"] = [v[0] for v in mfg_vtypes]
    except:
        pass

    # Signal 3: Stock items — scale score by count (1 item vs 185 items is very different)
    stock_count = profile["stats"].get("mst_stock_item", 0)
    if stock_count > 20:
        scores["Trading"] += 7  # Strong inventory presence
        signals["has_stock_items"] = True
        signals["stock_item_count"] = stock_count
    elif stock_count > 0:
        scores["Trading"] += 2  # Minimal inventory
        signals["has_stock_items"] = True
        signals["stock_item_count"] = stock_count
    else:
        scores["Service"] += 5
        signals["has_stock_items"] = False

    # Signal 4: Income/Expense ledger analysis
    try:
        # Service income indicators
        cur.execute("""
            SELECT NAME FROM mst_ledger
            WHERE UPPER(PARENT) IN ('DIRECT INCOMES', 'DIRECT INCOME', 'INDIRECT INCOMES', 'INDIRECT INCOME')
            AND (UPPER(NAME) LIKE '%PROFESSIONAL%' OR UPPER(NAME) LIKE '%CONSULTING%'
                 OR UPPER(NAME) LIKE '%SERVICE%' OR UPPER(NAME) LIKE '%COMMISSION%'
                 OR UPPER(NAME) LIKE '%BROKERAGE%' OR UPPER(NAME) LIKE '%FEES%')
        """)
        service_income = [row[0] for row in cur.fetchall()]
        if service_income:
            scores["Service"] += len(service_income) * 3
            signals["service_income_ledgers"] = service_income

        # Trading indicators — Sales/Purchase accounts
        cur.execute("""
            SELECT COUNT(*) FROM mst_ledger
            WHERE UPPER(PARENT) IN ('SALES ACCOUNTS', 'PURCHASE ACCOUNTS')
        """)
        trade_ledger_count = cur.fetchone()[0]
        if trade_ledger_count > 0:
            scores["Trading"] += 3
            signals["trade_account_count"] = trade_ledger_count
    except:
        pass

    # Signal 5: ISCOSTCENTRE flag — project/service tracking
    try:
        if column_exists(conn, "trn_voucher", "ISCOSTCENTRE"):
            cur.execute("SELECT COUNT(*) FROM trn_voucher WHERE UPPER(ISCOSTCENTRE) = 'YES'")
            row = cur.fetchone()
            cost_centre_vch = row[0] if row else 0
            if cost_centre_vch > 0:
                scores["Service"] += 2
                signals["cost_centre_vouchers"] = cost_centre_vch
    except Exception:
        pass

    # Determine result
    if not scores:
        nature = "Unknown"
    else:
        sorted_scores = scores.most_common()
        top_score = sorted_scores[0][1]
        # If top two are close (within 30%), it's Mixed
        if len(sorted_scores) >= 2 and sorted_scores[1][1] >= top_score * 0.7:
            nature = "Mixed"
            signals["mixed_primary"] = sorted_scores[0][0]
            signals["mixed_secondary"] = sorted_scores[1][0]
        else:
            nature = sorted_scores[0][0]

    signals["scores"] = dict(scores)
    return nature, signals


# ════════════════════════════════════════════════════════════════════════════
# INDUSTRY DETECTION
# ════════════════════════════════════════════════════════════════════════════

def _detect_industry(cur, profile):
    """Detect specific industry from data patterns"""
    signals = {}
    scores = Counter()

    # ── PHARMA ──
    try:
        # Check batch tracking / expiry (columns may not exist in all databases)
        batch_items = 0
        perishable_items = 0
        if column_exists(conn, "mst_stock_item", "ISBATCHWISEON"):
            cur.execute("SELECT COUNT(*) FROM mst_stock_item WHERE UPPER(ISBATCHWISEON)='YES'")
            row = cur.fetchone()
            batch_items = row[0] if row else 0
        if column_exists(conn, "mst_stock_item", "ISPERISHABLEON"):
            cur.execute("SELECT COUNT(*) FROM mst_stock_item WHERE UPPER(ISPERISHABLEON)='YES'")
            row = cur.fetchone()
            perishable_items = row[0] if row else 0

        if batch_items > 0:
            scores["Pharma"] += 3
            signals["batch_items"] = batch_items
        if perishable_items > 0:
            scores["Pharma"] += 5
            signals["perishable_items"] = perishable_items

        # Check stock item names for pharma keywords
        cur.execute("SELECT NAME FROM mst_stock_item LIMIT 50")
        item_names = [row[0].upper() for row in cur.fetchall()]
        pharma_keywords = ["TAB", "CAP", "SYR", "INJ", "CREAM", "GEL", "OINT",
                          "DROPS", "SUSPENSION", "TABLET", "CAPSULE", "SYRUP",
                          "MG", "ML", "STRIP", "VIAL", "AMPOULE", "SACHET"]
        pharma_matches = sum(1 for name in item_names if any(kw in name for kw in pharma_keywords))
        if pharma_matches > len(item_names) * 0.3:  # >30% items look pharma
            scores["Pharma"] += 8
            signals["pharma_item_match_pct"] = round(pharma_matches / max(len(item_names), 1) * 100)

        # Check ledger names for pharma
        cur.execute("SELECT NAME FROM mst_ledger WHERE UPPER(NAME) LIKE '%PHARMA%' OR UPPER(NAME) LIKE '%MEDICAL%' OR UPPER(NAME) LIKE '%DRUG%' OR UPPER(NAME) LIKE '%CHEMIST%'")
        pharma_ledgers = [row[0] for row in cur.fetchall()]
        if pharma_ledgers:
            scores["Pharma"] += 3
            signals["pharma_ledgers"] = pharma_ledgers
    except:
        pass

    # ── E-COMMERCE / D2C ──
    try:
        ecom_keywords = ["RAZORPAY", "SHIPROCKET", "SHOPIFY", "AMAZON", "FLIPKART",
                         "MEESHO", "PAYTM MALL", "INSTAMOJO", "CASHFREE", "PHONEPE",
                         "DELHIVERY", "ECOM", "COD", "PREPAID ORDER", "MARKETPLACE"]
        cur.execute("SELECT NAME FROM mst_ledger")
        all_ledgers = [row[0].upper() for row in cur.fetchall()]
        ecom_matches = [l for l in all_ledgers if any(kw in l for kw in ecom_keywords)]
        if ecom_matches:
            scores["E-commerce/D2C"] += len(ecom_matches) * 2
            signals["ecommerce_ledgers"] = ecom_matches[:10]
    except:
        pass

    # ── REAL ESTATE / CONSTRUCTION ── (tightened — need STRONG signals)
    try:
        # Only count strong real estate signals — not generic words like "project" or "site"
        cur.execute("""
            SELECT NAME FROM mst_ledger
            WHERE UPPER(NAME) LIKE '%CONSTRUCTION%'
            OR UPPER(NAME) LIKE '%BUILDER%'
            OR UPPER(NAME) LIKE '%FLAT NO%' OR UPPER(NAME) LIKE '%PLOT NO%'
            OR UPPER(NAME) LIKE '%RERA%'
            OR UPPER(NAME) LIKE '%WIP%CONSTRUCTION%'
            OR UPPER(NAME) LIKE '%UNDER CONSTRUCTION%'
        """)
        realestate_ledgers = [row[0] for row in cur.fetchall()]
        if realestate_ledgers:
            scores["Real Estate"] += len(realestate_ledgers) * 3
            signals["realestate_ledgers"] = realestate_ledgers

        # Strong signal: groups named for construction
        cur.execute("""
            SELECT NAME FROM mst_group
            WHERE UPPER(NAME) LIKE '%CONSTRUCTION%' OR UPPER(NAME) LIKE '%REAL ESTATE%'
            OR UPPER(NAME) LIKE '%PROJECTS%WIP%' OR UPPER(NAME) LIKE '%PROPERTY%'
        """)
        re_groups = [row[0] for row in cur.fetchall()]
        if re_groups:
            scores["Real Estate"] += len(re_groups) * 4
    except:
        pass

    # ── JEWELLERY ──
    try:
        cur.execute("""
            SELECT BASEUNITS FROM mst_stock_item
            WHERE UPPER(BASEUNITS) IN ('GMS', 'GM', 'GRAMS', 'GRAM', 'GRM', 'CT', 'CARAT', 'CARATS')
        """)
        weight_items = cur.fetchall()
        if len(weight_items) > 0:
            scores["Jewellery"] += 5
            signals["weight_based_items"] = len(weight_items)

        cur.execute("SELECT NAME FROM mst_stock_item WHERE UPPER(NAME) LIKE '%GOLD%' OR UPPER(NAME) LIKE '%SILVER%' OR UPPER(NAME) LIKE '%DIAMOND%' OR UPPER(NAME) LIKE '%22K%' OR UPPER(NAME) LIKE '%18K%' OR UPPER(NAME) LIKE '%916%'")
        jewel_items = [row[0] for row in cur.fetchall()]
        if jewel_items:
            scores["Jewellery"] += len(jewel_items) * 2
            signals["jewellery_items"] = jewel_items
    except:
        pass

    # ── TRANSPORT ──
    try:
        tracking = 0
        if column_exists(conn, "trn_voucher", "USETRACKINGNUMBER"):
            cur.execute("SELECT COUNT(*) FROM trn_voucher WHERE UPPER(USETRACKINGNUMBER)='YES'")
            row = cur.fetchone()
            tracking = row[0] if row else 0
        if tracking > 0:
            scores["Transport"] += 5
            signals["tracking_number_vouchers"] = tracking

        cur.execute("SELECT NAME FROM mst_ledger WHERE UPPER(NAME) LIKE '%FREIGHT%' OR UPPER(NAME) LIKE '%TRANSPORT%' OR UPPER(NAME) LIKE '%VEHICLE%' OR UPPER(NAME) LIKE '%LORRY%'")
        transport_ledgers = [row[0] for row in cur.fetchall()]
        if transport_ledgers:
            scores["Transport"] += len(transport_ledgers) * 2
            signals["transport_ledgers"] = transport_ledgers
    except:
        pass

    # ── HANDICRAFTS / ARTISAN ──
    try:
        craft_keywords = ["HANDICRAFT", "HANDMADE", "ARTISAN", "CRAFT", "KARIGAR",
                          "EXPORT PROMOTION COUNCIL", "EPCH", "COTTAGE INDUSTRY"]
        cur.execute("SELECT NAME FROM mst_ledger")
        all_ldg = [row[0].upper() for row in cur.fetchall()]
        craft_matches = [l for l in all_ldg if any(kw in l for kw in craft_keywords)]
        # Also check company name
        if any(kw in profile.get("company_name", "").upper() for kw in craft_keywords):
            scores["Handicrafts"] += 6
            signals["company_name_handicraft"] = True
        if craft_matches:
            scores["Handicrafts"] += len(craft_matches) * 2
            signals["handicraft_ledgers"] = craft_matches[:10]
    except:
        pass

    # ── HOSPITAL / HEALTHCARE ── (tightened — "HOSPITALITY" is NOT hospital)
    try:
        cur.execute("""SELECT NAME FROM mst_ledger
            WHERE (UPPER(NAME) LIKE '%PATIENT%' OR UPPER(NAME) LIKE '%CONSULTATION FEE%'
            OR UPPER(NAME) LIKE '%OPD %' OR UPPER(NAME) LIKE '%IPD %'
            OR UPPER(NAME) LIKE '%HOSPITAL %' OR UPPER(NAME) LIKE '%CLINIC%')
            AND UPPER(NAME) NOT LIKE '%HOSPITALITY%'
        """)
        hospital_ledgers = [row[0] for row in cur.fetchall()]
        if hospital_ledgers:
            scores["Hospital"] += len(hospital_ledgers) * 3
            signals["hospital_ledgers"] = hospital_ledgers
    except:
        pass

    # ── EDUCATION ──
    try:
        cur.execute("SELECT NAME FROM mst_ledger WHERE UPPER(NAME) LIKE '%TUITION%' OR UPPER(NAME) LIKE '%STUDENT%' OR UPPER(NAME) LIKE '%SCHOOL FEE%' OR UPPER(NAME) LIKE '%COLLEGE%' OR UPPER(NAME) LIKE '%EXAM FEE%' OR UPPER(NAME) LIKE '%HOSTEL%'")
        education_ledgers = [row[0] for row in cur.fetchall()]
        if education_ledgers:
            scores["Education"] += len(education_ledgers) * 2
            signals["education_ledgers"] = education_ledgers
    except:
        pass

    # ── RESTAURANT / HOTEL ──
    try:
        cur.execute("SELECT NAME FROM mst_ledger WHERE UPPER(NAME) LIKE '%SERVICE CHARGE%' OR UPPER(NAME) LIKE '%FOOD%' OR UPPER(NAME) LIKE '%BEVERAGE%' OR UPPER(NAME) LIKE '%ROOM%RENT%' OR UPPER(NAME) LIKE '%TABLE%'")
        restaurant_ledgers = [row[0] for row in cur.fetchall()]
        if restaurant_ledgers:
            scores["Restaurant/Hotel"] += len(restaurant_ledgers) * 2
            signals["restaurant_ledgers"] = restaurant_ledgers
    except:
        pass

    # ── IMPORT/EXPORT ──
    try:
        # Multi-currency = import/export indicator
        cur.execute("SELECT COUNT(*) FROM mst_ledger WHERE UPPER(NAME) LIKE '%FOREX%' OR UPPER(NAME) LIKE '%FOREIGN EXCHANGE%' OR UPPER(NAME) LIKE '%EXCHANGE GAIN%' OR UPPER(NAME) LIKE '%EXCHANGE LOSS%'")
        forex_ledgers = cur.fetchone()[0]
        if forex_ledgers > 0:
            scores["Import/Export"] += 5
            signals["forex_ledgers"] = forex_ledgers

        # Check for customs duty
        cur.execute("SELECT NAME FROM mst_ledger WHERE UPPER(NAME) LIKE '%CUSTOMS%' OR UPPER(NAME) LIKE '%IMPORT DUTY%' OR UPPER(NAME) LIKE '%EXPORT%'")
        ie_ledgers = [row[0] for row in cur.fetchall()]
        if ie_ledgers:
            scores["Import/Export"] += len(ie_ledgers) * 3
            signals["import_export_ledgers"] = ie_ledgers
    except:
        pass

    # Determine result
    if not scores:
        industry = "General"
    else:
        top = scores.most_common(1)[0]
        if top[1] >= 5:  # Minimum confidence threshold
            industry = top[0]
        else:
            industry = "General"

    signals["scores"] = dict(scores)
    return industry, signals


# ════════════════════════════════════════════════════════════════════════════
# COMPLEXITY DETECTION
# ════════════════════════════════════════════════════════════════════════════

def _detect_complexity(cur, profile):
    """Score complexity from 1-10: Simple (1-3) | Moderate (4-6) | Complex (7-10)"""
    score = 0
    features = {}
    stats = profile["stats"]

    # Ledger count
    ledgers = stats.get("mst_ledger", 0)
    if ledgers > 200:
        score += 2
        features["many_ledgers"] = f"{ledgers} ledgers (>200)"
    elif ledgers > 50:
        score += 1
        features["moderate_ledgers"] = f"{ledgers} ledgers (50-200)"

    # Stock items
    items = stats.get("mst_stock_item", 0)
    if items > 200:
        score += 2
        features["many_stock_items"] = f"{items} items (>200)"
    elif items > 20:
        score += 1
        features["moderate_stock_items"] = f"{items} items (20-200)"

    # Voucher count
    vouchers = stats.get("trn_voucher", 0)
    if vouchers > 10000:
        score += 2
        features["high_volume"] = f"{vouchers} vouchers (>10K)"
    elif vouchers > 1000:
        score += 1
        features["moderate_volume"] = f"{vouchers} vouchers (1K-10K)"

    # Voucher type count
    vtypes = stats.get("mst_voucher_type", 0)
    if vtypes > 15:
        score += 1
        features["many_voucher_types"] = f"{vtypes} types (>15)"

    # Multiple godowns
    godowns = stats.get("mst_godown", 0)
    if godowns > 5:
        score += 2
        features["multi_godown"] = f"{godowns} godowns (>5)"
    elif godowns > 1:
        score += 1
        features["few_godowns"] = f"{godowns} godowns"

    # Cost centres used
    try:
        if column_exists(conn, "trn_voucher", "ISCOSTCENTRE"):
            cur.execute("SELECT COUNT(*) FROM trn_voucher WHERE UPPER(ISCOSTCENTRE)='YES'")
            row = cur.fetchone()
            cc_count = row[0] if row else 0
            if cc_count > 0:
                score += 1
                features["cost_centres_used"] = f"{cc_count} vouchers with cost centres"
    except Exception:
        pass

    # Batch tracking
    try:
        if column_exists(conn, "mst_stock_item", "ISBATCHWISEON"):
            cur.execute("SELECT COUNT(*) FROM mst_stock_item WHERE UPPER(ISBATCHWISEON)='YES'")
            row = cur.fetchone()
            batch = row[0] if row else 0
            if batch > 0:
                score += 1
                features["batch_tracking"] = f"{batch} items with batches"
    except Exception:
        pass

    # GST complexity — multiple tax rates
    try:
        if column_exists(conn, "trn_accounting", "GSTTAXRATE"):
            cur.execute("SELECT COUNT(DISTINCT GSTTAXRATE) FROM trn_accounting WHERE GSTTAXRATE IS NOT NULL AND GSTTAXRATE != ''")
            row = cur.fetchone()
            tax_rates = row[0] if row else 0
            if tax_rates > 3:
                score += 1
                features["multi_gst_rates"] = f"{tax_rates} distinct GST rates"
    except Exception:
        pass

    # Bank accounts
    try:
        cur.execute("SELECT COUNT(*) FROM mst_ledger WHERE UPPER(PARENT) = 'BANK ACCOUNTS'")
        banks = cur.fetchone()[0]
        if banks > 3:
            score += 1
            features["multi_bank"] = f"{banks} bank accounts"
    except:
        pass

    # Cap at 10
    score = min(score, 10)

    if score <= 3:
        complexity = "Simple"
    elif score <= 6:
        complexity = "Moderate"
    else:
        complexity = "Complex"

    return complexity, score, features


# ════════════════════════════════════════════════════════════════════════════
# RECOMMENDATIONS ENGINE
# ════════════════════════════════════════════════════════════════════════════

def _generate_recommendations(profile):
    """Based on the detected profile, recommend which analyses are relevant."""
    recs = []

    # Universal recommendations
    recs.append({"category": "Audit", "name": "Benford's Law Analysis", "description": "First-digit frequency test on all transactions to detect fabrication", "priority": "High"})
    recs.append({"category": "Audit", "name": "Duplicate Invoice Detection", "description": "Find duplicate voucher numbers, amounts, and party combinations", "priority": "High"})
    recs.append({"category": "Audit", "name": "Voucher Gap Analysis", "description": "Detect missing voucher numbers in sequences", "priority": "Medium"})
    recs.append({"category": "Audit", "name": "Sunday/Holiday Entries", "description": "Flag transactions on non-business days", "priority": "Medium"})
    recs.append({"category": "Audit", "name": "Cash Transaction Limits", "description": "Section 269ST — flag cash receipts > Rs 2,00,000", "priority": "High"})
    recs.append({"category": "Compliance", "name": "GST Reconciliation", "description": "Match books with GSTR-1/3B returns", "priority": "High"})

    # Entity-type specific
    entity = profile.get("entity_type", "")
    if entity in ("Pvt Ltd", "Public Ltd"):
        recs.append({"category": "Compliance", "name": "CARO 2020 Checklist", "description": "21-clause compliance verification", "priority": "High"})
        recs.append({"category": "Audit", "name": "Related Party Transactions", "description": "Section 177/188 compliance", "priority": "High"})
        recs.append({"category": "Audit", "name": "Director Loans (Sec 185/186)", "description": "Verify loans to directors", "priority": "Medium"})
    elif entity == "Partnership":
        recs.append({"category": "Analysis", "name": "Partner Capital Reconciliation", "description": "Capital accounts, drawings, interest on capital analysis", "priority": "High"})
        recs.append({"category": "Compliance", "name": "Sec 40(b) Compliance", "description": "Interest/salary to partners limits", "priority": "Medium"})
    elif entity == "Trust":
        recs.append({"category": "Compliance", "name": "Trust Compliance", "description": "FCRA compliance, 85% application rule", "priority": "High"})

    # Business nature specific
    nature = profile.get("business_nature", "")
    if nature in ("Trading", "Mixed"):
        recs.append({"category": "Analysis", "name": "Gross Profit Consistency", "description": "Month-on-month GP ratio analysis", "priority": "High"})
        recs.append({"category": "Analysis", "name": "Inventory Turnover", "description": "Stock rotation and aging analysis", "priority": "High"})
        recs.append({"category": "Audit", "name": "Purchase-to-Sales Ratio", "description": "Monthly consistency check for fictitious purchases", "priority": "Medium"})
    if nature in ("Manufacturing", "Mixed"):
        recs.append({"category": "Analysis", "name": "Raw Material Consumption Ratio", "description": "RM consumed vs sales consistency", "priority": "High"})
        recs.append({"category": "Analysis", "name": "WIP Valuation", "description": "Work-in-progress analysis", "priority": "Medium"})
        recs.append({"category": "Analysis", "name": "Yield Analysis", "description": "Output vs input ratio tracking", "priority": "Medium"})
    if nature == "Service":
        recs.append({"category": "Analysis", "name": "Revenue Per Customer", "description": "Customer concentration and revenue analysis", "priority": "High"})
        recs.append({"category": "Analysis", "name": "Deferred Revenue", "description": "Unbilled revenue and cut-off analysis", "priority": "Medium"})

    # Industry specific
    industry = profile.get("industry", "")
    if industry == "Pharma":
        recs.append({"category": "Industry", "name": "Batch Expiry Analysis", "description": "Near-expiry stock identification (30/60/90 days)", "priority": "High"})
        recs.append({"category": "Industry", "name": "FEFO Compliance", "description": "First Expiry First Out stock rotation verification", "priority": "Medium"})
    elif industry == "Real Estate":
        recs.append({"category": "Industry", "name": "Project-wise P&L", "description": "Individual project profitability tracking", "priority": "High"})
        recs.append({"category": "Industry", "name": "WIP Under Construction", "description": "Capital WIP valuation and progress", "priority": "High"})
    elif industry == "Jewellery":
        recs.append({"category": "Industry", "name": "Weight Reconciliation", "description": "Gold/silver weight tracking and purity analysis", "priority": "High"})
    elif industry == "Transport":
        recs.append({"category": "Industry", "name": "Vehicle-wise P&L", "description": "Per-vehicle revenue and cost analysis", "priority": "High"})
    elif industry == "Import/Export":
        recs.append({"category": "Industry", "name": "Forex Gain/Loss Analysis", "description": "Realized vs unrealized forex tracking", "priority": "High"})
        recs.append({"category": "Industry", "name": "AS-11 Compliance", "description": "Foreign exchange rate effect reporting", "priority": "Medium"})

    # Statutory — always relevant
    recs.append({"category": "Compliance", "name": "Form 3CD Data Extraction", "description": "44-clause tax audit data preparation", "priority": "High"})
    recs.append({"category": "Compliance", "name": "TDS Compliance", "description": "Section-wise TDS deduction verification", "priority": "High"})

    return recs


# ════════════════════════════════════════════════════════════════════════════
# SAVE PROFILE
# ════════════════════════════════════════════════════════════════════════════

def _save_profile(conn, profile):
    """Save the profile to _company_profile table in the database."""
    import json
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS _company_profile")
        cur.execute("""
            CREATE TABLE _company_profile (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        for key in ["company_name", "entity_type", "business_nature", "industry",
                     "complexity", "complexity_score", "gstin", "gst_registration_type", "state"]:
            cur.execute("INSERT INTO _company_profile (key, value) VALUES (?, ?)",
                        (key, str(profile.get(key, ""))))

        # Store detailed data as JSON
        cur.execute("INSERT INTO _company_profile (key, value) VALUES (?, ?)",
                    ("signals", json.dumps(profile.get("signals", {}), default=str)))
        cur.execute("INSERT INTO _company_profile (key, value) VALUES (?, ?)",
                    ("features", json.dumps(profile.get("features", {}), default=str)))
        cur.execute("INSERT INTO _company_profile (key, value) VALUES (?, ?)",
                    ("stats", json.dumps(profile.get("stats", {}), default=str)))
        cur.execute("INSERT INTO _company_profile (key, value) VALUES (?, ?)",
                    ("recommendations", json.dumps(profile.get("recommendations", []), default=str)))

        conn.commit()
    except Exception as e:
        # If saving fails (e.g. read-only DB), just log and continue
        import logging
        logging.getLogger(__name__).warning(f"Could not save profile to DB: {e}")


def load_profile(db_path="tally_data.db"):
    """Load a previously saved profile from the database."""
    import json
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        cur.execute("SELECT key, value FROM _company_profile")
        rows = dict(cur.fetchall())
        profile = {
            "company_name": rows.get("company_name", ""),
            "entity_type": rows.get("entity_type", "Unknown"),
            "business_nature": rows.get("business_nature", "Unknown"),
            "industry": rows.get("industry", "General"),
            "complexity": rows.get("complexity", "Unknown"),
            "complexity_score": int(rows.get("complexity_score", 0)),
            "gstin": rows.get("gstin", ""),
            "gst_registration_type": rows.get("gst_registration_type", ""),
            "state": rows.get("state", ""),
            "signals": json.loads(rows.get("signals", "{}")),
            "features": json.loads(rows.get("features", "{}")),
            "stats": json.loads(rows.get("stats", "{}")),
            "recommendations": json.loads(rows.get("recommendations", "[]")),
        }
        conn.close()
        return profile
    except:
        conn.close()
        return None


# ════════════════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    import json

    db = sys.argv[1] if len(sys.argv) > 1 else "tally_data.db"
    print(f"Profiling company from: {db}")
    print("=" * 60)

    profile = profile_company(db)

    print(f"\n{'=' * 60}")
    print(f"  COMPANY PROFILE: {profile['company_name']}")
    print(f"{'=' * 60}")
    print(f"  GSTIN:              {profile['gstin']}")
    print(f"  State:              {profile['state']}")
    print(f"  GST Registration:   {profile['gst_registration_type']}")
    print(f"  Entity Type:        {profile['entity_type']}")
    print(f"  Business Nature:    {profile['business_nature']}")
    print(f"  Industry:           {profile['industry']}")
    print(f"  Complexity:         {profile['complexity']} ({profile['complexity_score']}/10)")
    print(f"{'=' * 60}")

    print(f"\n  DATA VOLUME:")
    for k, v in profile['stats'].items():
        print(f"    {k}: {v:,} rows")

    print(f"\n  COMPLEXITY FEATURES:")
    for k, v in profile['features'].items():
        print(f"    [x] {v}")

    print(f"\n  DETECTION SIGNALS:")
    for category, sigs in profile['signals'].items():
        print(f"\n  [{category}]")
        if isinstance(sigs, dict):
            for k, v in sigs.items():
                if k != "scores":
                    val = str(v)[:80]
                    print(f"    {k}: {val}")
            if "scores" in sigs:
                print(f"    Confidence scores: {sigs['scores']}")

    print(f"\n  RECOMMENDED ANALYSES ({len(profile['recommendations'])}):")
    for rec in profile['recommendations']:
        icon = "[HIGH]" if rec['priority'] == 'High' else "[MED]" if rec['priority'] == 'Medium' else "[LOW]"
        print(f"    {icon} [{rec['category']}] {rec['name']} — {rec['description']}")

    print(f"\n{'=' * 60}")
    print(f"  Profile saved to _company_profile table in {db}")
    print(f"{'=' * 60}")
