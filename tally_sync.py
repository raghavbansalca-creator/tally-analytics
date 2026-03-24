"""
Seven Labs Vision — Tally Live Sync
Connects to a running TallyPrime instance via its XML API (port 9000)
and extracts ALL data into SQLite, producing the exact same schema
as db_loader.py (dynamic columns, all TEXT).
"""

import datetime
import re
import sqlite3
import xml.etree.ElementTree as ET

import requests


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tally_url(host, port=9000):
    host = host.strip()
    if host.startswith("http"):
        return f"{host}:{port}" if ":" not in host.split("//")[1] else host
    return f"http://{host}:{port}"


def _sanitize_xml(data):
    """Remove illegal XML characters that Tally sometimes emits."""
    if isinstance(data, bytes):
        data = data.decode("utf-8", errors="replace")
    # Remove control character references (&#0; through &#31; except &#9;&#10;&#13;)
    data = re.sub(r'&#(?:0*[0-8]|0*1[0-1]|0*1[4-9]|0*2[0-9]|0*3[0-1]);', '', data)
    # Remove ALL numeric char refs that are control chars
    data = re.sub(r'&#\d+;', lambda m: m.group() if int(m.group()[2:-1]) > 31 else '', data)
    # Remove raw control chars
    data = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', data)
    # Fix unescaped ampersands
    data = re.sub(r'&(?!amp;|lt;|gt;|apos;|quot;|#)', '&amp;', data)
    # Remove UDF namespace tags (Tally custom fields with UDF: prefix)
    lines = data.split('\n')
    clean_lines = []
    skip_udf = False
    for line in lines:
        stripped = line.strip()
        if '<UDF:' in stripped:
            if '.LIST' in stripped and '/>' not in stripped:
                skip_udf = True
            continue
        if skip_udf:
            if '</UDF:' in stripped:
                skip_udf = False
            continue
        clean_lines.append(line)
    return '\n'.join(clean_lines)


def _post_xml(url, xml_body, timeout=120):
    """Send XML request to Tally and return sanitized response text."""
    resp = requests.post(
        url,
        data=xml_body.encode("utf-8"),
        headers={"Content-Type": "application/xml"},
        timeout=timeout,
    )
    resp.raise_for_status()
    return _sanitize_xml(resp.text)


# ---------------------------------------------------------------------------
# XML request builders
# ---------------------------------------------------------------------------

def _build_collection_xml(collection_name, object_type, fetch_fields):
    """Build XML request for a flat master collection."""
    # fetch_fields can be a list or comma-separated string
    if isinstance(fetch_fields, str):
        fetch_fields = [f.strip() for f in fetch_fields.split(",")]
    fetch_line = ", ".join(fetch_fields)
    return f"""<ENVELOPE>
<HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>{collection_name}</ID></HEADER>
<BODY><DESC>
<STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES>
<TDL><TDLMESSAGE>
<COLLECTION NAME="{collection_name}" ISMODIFY="No">
<TYPE>{object_type}</TYPE>
<FETCH>{fetch_line}</FETCH>
</COLLECTION>
</TDLMESSAGE></TDL>
</DESC></BODY></ENVELOPE>"""


def _build_voucher_xml():
    """Build XML request for vouchers with nested ledger/inventory entries."""
    return """<ENVELOPE>
<HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>SLVVouchers</ID></HEADER>
<BODY><DESC>
<STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES>
<TDL><TDLMESSAGE>
<COLLECTION NAME="SLVVouchers" ISMODIFY="No">
<TYPE>Voucher</TYPE>
<FETCH>DATE, VOUCHERTYPENAME, VOUCHERNUMBER, REFERENCE, REFERENCEDATE, PARTYLEDGERNAME, BASICBUYERNAME, AMOUNT, NARRATION, GUID, ALTERID, MASTERID, EFFECTIVEDATE, PLACEOFSUPPLY, ISCANCELLED, ISOPTIONAL, PARTYGSTIN, PARTYLEDGERGSTIN, CMPGSTIN, GSTREGISTRATIONTYPE, ISINVOICE</FETCH>
<FETCH>ALLLEDGERENTRIES.LIST.LEDGERNAME, ALLLEDGERENTRIES.LIST.AMOUNT, ALLLEDGERENTRIES.LIST.ISDEEMEDPOSITIVE, ALLLEDGERENTRIES.LIST.ISPARTYLEDGER, ALLLEDGERENTRIES.LIST.GSTHSNNAME, ALLLEDGERENTRIES.LIST.GSTTAXRATE, ALLLEDGERENTRIES.LIST.GSTOVRDNTAXABILITY, ALLLEDGERENTRIES.LIST.GSTOVRDNTYPEOFSUPPLY, ALLLEDGERENTRIES.LIST.GSTCLASS</FETCH>
<FETCH>ALLINVENTORYENTRIES.LIST.STOCKITEMNAME, ALLINVENTORYENTRIES.LIST.RATE, ALLINVENTORYENTRIES.LIST.AMOUNT, ALLINVENTORYENTRIES.LIST.ACTUALQTY, ALLINVENTORYENTRIES.LIST.BILLEDQTY</FETCH>
</COLLECTION>
</TDLMESSAGE></TDL>
</DESC></BODY></ENVELOPE>"""


def _build_company_info_xml():
    """Request basic company info from Tally."""
    return """<ENVELOPE>
<HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Data</TYPE><ID>List of Companies</ID></HEADER>
<BODY><DESC>
<STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES>
</DESC></BODY></ENVELOPE>"""


def _build_active_company_xml():
    """Request the currently active company name."""
    return """<ENVELOPE>
<HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>SLVCompanyInfo</ID></HEADER>
<BODY><DESC>
<STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES>
<TDL><TDLMESSAGE>
<COLLECTION NAME="SLVCompanyInfo" ISMODIFY="No">
<TYPE>Company</TYPE>
<FETCH>NAME, BASICCOMPANYFORMALNAME, STATENAME, PINCODE, EMAIL, PHONENUMBER, INCOMETAXNUMBER, GSTREGISTRATIONTYPE, GSTIN</FETCH>
</COLLECTION>
</TDLMESSAGE></TDL>
</DESC></BODY></ENVELOPE>"""


# ---------------------------------------------------------------------------
# XML response parsers  (mirrors db_loader.py logic exactly)
# ---------------------------------------------------------------------------

def _parse_flat(xml_str, tag):
    """Parse XML into list of dicts from elements matching *tag*.
    Includes attributes and simple (non-nested) child text.
    This is identical to db_loader.parse_flat."""
    root = ET.fromstring(xml_str)
    records = []
    for el in root.iter(tag):
        rec = dict(el.attrib)
        for child in el:
            if not list(child):  # only leaf nodes
                val = (child.text or "").strip()
                if val:
                    rec[child.tag] = val
        if rec:
            records.append(rec)
    return records


def _parse_vouchers(xml_str):
    """Parse voucher XML into 6 flat tables.
    Identical logic to db_loader.parse_vouchers."""
    root = ET.fromstring(xml_str)

    trn_voucher = []
    trn_accounting = []
    trn_bill = []
    trn_bank = []
    trn_inventory = []
    trn_batch = []

    LEDGER_TAGS = {"ALLLEDGERENTRIES.LIST"}
    INVENTORY_TAGS = {"ALLINVENTORYENTRIES.LIST"}
    SKIP_TAGS = LEDGER_TAGS | INVENTORY_TAGS | {
        "BILLALLOCATIONS.LIST", "BANKALLOCATIONS.LIST",
        "BATCHALLOCATIONS.LIST", "CATEGORYALLOCATIONS.LIST",
        "COSTCENTREALLOCATIONS.LIST", "COSTTRACKALLOCATIONS.LIST",
        "INVENTORYALLOCATIONS.LIST",
    }

    for vch in root.iter("VOUCHER"):
        # --- voucher header ---
        vch_data = dict(vch.attrib)
        for child in vch:
            if child.tag in SKIP_TAGS or child.tag.endswith(".LIST"):
                continue
            if not list(child):
                val = (child.text or "").strip()
                if val:
                    vch_data[child.tag] = val

        vch_guid = vch_data.get("GUID", vch_data.get("REMOTEID", ""))
        if not vch_data.get("GUID") and vch_data.get("REMOTEID"):
            vch_data["GUID"] = vch_data["REMOTEID"]
        if not vch_guid:
            continue
        trn_voucher.append(vch_data)

        # --- ledger entries ---
        for le_tag in LEDGER_TAGS:
            for le in vch.findall(le_tag):
                le_data = {"VOUCHER_GUID": vch_guid}
                le_data.update(le.attrib)
                for child in le:
                    if child.tag in (
                        "BILLALLOCATIONS.LIST", "BANKALLOCATIONS.LIST",
                        "CATEGORYALLOCATIONS.LIST", "COSTCENTREALLOCATIONS.LIST",
                        "COSTTRACKALLOCATIONS.LIST", "INVENTORYALLOCATIONS.LIST",
                    ):
                        continue
                    if child.tag.endswith(".LIST"):
                        continue
                    if not list(child):
                        val = (child.text or "").strip()
                        if val:
                            le_data[child.tag] = val
                trn_accounting.append(le_data)

                # bill allocations
                for bill in le.findall("BILLALLOCATIONS.LIST"):
                    bill_data = {
                        "VOUCHER_GUID": vch_guid,
                        "LEDGERNAME": le_data.get("LEDGERNAME", ""),
                    }
                    for child in bill:
                        if not list(child):
                            val = (child.text or "").strip()
                            if val:
                                bill_data[child.tag] = val
                    trn_bill.append(bill_data)

                # bank allocations
                for bank in le.findall("BANKALLOCATIONS.LIST"):
                    bank_data = {
                        "VOUCHER_GUID": vch_guid,
                        "LEDGERNAME": le_data.get("LEDGERNAME", ""),
                    }
                    for child in bank:
                        if not list(child):
                            val = (child.text or "").strip()
                            if val:
                                bank_data[child.tag] = val
                    trn_bank.append(bank_data)

        # --- inventory entries ---
        for inv_tag in INVENTORY_TAGS:
            for inv in vch.findall(inv_tag):
                inv_data = {"VOUCHER_GUID": vch_guid}
                inv_data.update(inv.attrib)
                for child in inv:
                    if child.tag == "BATCHALLOCATIONS.LIST":
                        continue
                    if child.tag.endswith(".LIST"):
                        continue
                    if not list(child):
                        val = (child.text or "").strip()
                        if val:
                            inv_data[child.tag] = val
                trn_inventory.append(inv_data)

                # batch allocations
                for batch in inv.findall("BATCHALLOCATIONS.LIST"):
                    batch_data = {
                        "VOUCHER_GUID": vch_guid,
                        "STOCKITEMNAME": inv_data.get("STOCKITEMNAME", ""),
                    }
                    for child in batch:
                        if not list(child):
                            val = (child.text or "").strip()
                            if val:
                                batch_data[child.tag] = val
                    trn_batch.append(batch_data)

    return {
        "trn_voucher": trn_voucher,
        "trn_accounting": trn_accounting,
        "trn_bill": trn_bill,
        "trn_bank": trn_bank,
        "trn_inventory": trn_inventory,
        "trn_batch": trn_batch,
    }


# ---------------------------------------------------------------------------
# SQLite writer  (mirrors db_loader.py exactly)
# ---------------------------------------------------------------------------

def _collect_all_keys(records):
    keys = set()
    for rec in records:
        keys.update(rec.keys())
    return sorted(keys)


def _create_table_and_insert(conn, table_name, records):
    """DROP + CREATE + INSERT — all columns TEXT, dynamic schema."""
    if not records:
        print(f"  {table_name}: 0 records (skipped)")
        return 0

    columns = _collect_all_keys(records)
    col_map = {c: c.replace(".", "_") for c in columns}
    clean_cols = [col_map[c] for c in columns]

    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    col_defs = ", ".join(f'"{c}" TEXT' for c in clean_cols)
    conn.execute(f"CREATE TABLE {table_name} ({col_defs})")

    placeholders = ", ".join(["?"] * len(columns))
    col_names = ", ".join(f'"{c}"' for c in clean_cols)
    sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

    rows = []
    for rec in records:
        row = tuple(rec.get(c, "") for c in columns)
        rows.append(row)

    conn.executemany(sql, rows)
    conn.commit()
    print(f"  {table_name}: {len(records)} records, {len(columns)} columns")
    return len(records)


# ---------------------------------------------------------------------------
# Master collection definitions
# ---------------------------------------------------------------------------

MASTER_COLLECTIONS = {
    "mst_group": {
        "collection": "SLVGroups",
        "type": "Group",
        "fetch": "NAME, PARENT, NATUREOFGROUP, ISREVENUE, ISDEEMEDPOSITIVE, AFFECTSGROSSPROFIT, ISSUBLEDGER, ISADDABLE, GUID",
        "xml_tag": "GROUP",
    },
    "mst_ledger": {
        "collection": "SLVLedgers",
        "type": "Ledger",
        "fetch": "NAME, PARENT, ADDRESS, LEDGERSTATENAME, PINCODE, LEDGERPHONE, LEDGERMOBILE, EMAIL, OPENINGBALANCE, CLOSINGBALANCE, PARTYGSTIN, GSTREGISTRATIONTYPE, INCOMETAXNUMBER, BANKACCOUNTNUMBER, IFSCODE, BANKINGCONFIGBANK, CREDITPERIOD, CREDITLIMIT, BILLBYBILL, ISREVENUE, AFFECTSSTOCK, GUID, ALTERID",
        "xml_tag": "LEDGER",
    },
    "mst_stock_group": {
        "collection": "SLVStockGroups",
        "type": "Stock Group",
        "fetch": "NAME, PARENT, ISADDABLE, GUID",
        "xml_tag": "STOCKGROUP",
    },
    "mst_stock_item": {
        "collection": "SLVStockItems",
        "type": "Stock Item",
        "fetch": "NAME, PARENT, CATEGORY, BASEUNITS, OPENINGBALANCE, OPENINGRATE, OPENINGVALUE, CLOSINGBALANCE, CLOSINGRATE, CLOSINGVALUE, HSNCODE, GSTAPPLICABLE, GUID, ALTERID",
        "xml_tag": "STOCKITEM",
    },
    "mst_godown": {
        "collection": "SLVGodowns",
        "type": "Godown",
        "fetch": "NAME, PARENT, HASNOSPACE, GUID",
        "xml_tag": "GODOWN",
    },
    "mst_voucher_type": {
        "collection": "SLVVoucherTypes",
        "type": "Voucher Type",
        "fetch": "NAME, PARENT, NUMBERINGMETHOD, ISTAXINVOICE, GUID",
        "xml_tag": "VOUCHERTYPE",
    },
    "mst_cost_centre": {
        "collection": "SLVCostCentres",
        "type": "Cost Centre",
        "fetch": "NAME, PARENT, CATEGORY, GUID",
        "xml_tag": "COSTCENTRE",
    },
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def test_connection(host, port=9000):
    """Test if Tally is reachable. Returns {success, company, error}."""
    url = _tally_url(host, port)
    try:
        xml_body = _build_active_company_xml()
        resp_text = _post_xml(url, xml_body, timeout=10)
        # Try to extract company name
        root = ET.fromstring(resp_text)
        company = ""
        for el in root.iter("COMPANY"):
            name_el = el.find("NAME")
            if name_el is not None and name_el.text:
                company = name_el.text.strip()
                break
        if not company:
            # Fallback: look for any NAME tag
            for el in root.iter("NAME"):
                if el.text and el.text.strip():
                    company = el.text.strip()
                    break
        return {"success": True, "company": company, "error": ""}
    except requests.ConnectionError:
        return {"success": False, "company": "", "error": f"Cannot connect to Tally at {url}. Is TallyPrime running?"}
    except requests.Timeout:
        return {"success": False, "company": "", "error": f"Connection to {url} timed out."}
    except Exception as e:
        return {"success": False, "company": "", "error": str(e)}


def get_company_info(host, port=9000):
    """Get loaded company details."""
    url = _tally_url(host, port)
    resp_text = _post_xml(url, _build_active_company_xml(), timeout=15)
    root = ET.fromstring(resp_text)

    info = {}
    for el in root.iter("COMPANY"):
        for child in el:
            if not list(child):
                val = (child.text or "").strip()
                if val:
                    info[child.tag] = val
        if info:
            break
    return info


def sync_masters(host, port=9000, db_path="tally_data.db", progress_callback=None):
    """Extract and load master data from Tally into SQLite."""
    url = _tally_url(host, port)
    conn = sqlite3.connect(db_path)
    stats = {}
    total = len(MASTER_COLLECTIONS)

    for idx, (table_name, cfg) in enumerate(MASTER_COLLECTIONS.items(), 1):
        step = f"Extracting {table_name}..."
        print(step)
        if progress_callback:
            progress_callback(step, idx, total)

        try:
            xml_req = _build_collection_xml(cfg["collection"], cfg["type"], cfg["fetch"])
            resp_text = _post_xml(url, xml_req, timeout=60)
            records = _parse_flat(resp_text, cfg["xml_tag"])
            count = _create_table_and_insert(conn, table_name, records)
            stats[table_name] = len(records)
            print(f"  -> {len(records)} found")
        except Exception as e:
            print(f"  ERROR extracting {table_name}: {e}")
            stats[table_name] = f"ERROR: {e}"

    conn.close()
    return stats


def sync_vouchers(host, port=9000, db_path="tally_data.db", progress_callback=None):
    """Extract and load vouchers (with nested entries) from Tally into SQLite."""
    url = _tally_url(host, port)
    conn = sqlite3.connect(db_path)
    stats = {}

    step = "Extracting vouchers (this may take a while for large companies)..."
    print(step)
    if progress_callback:
        progress_callback(step, 1, 2)

    try:
        xml_req = _build_voucher_xml()
        resp_text = _post_xml(url, xml_req, timeout=300)  # 5 min for large datasets
        print(f"  Received {len(resp_text) / 1024 / 1024:.1f} MB of voucher data")

        step = "Parsing and loading voucher data..."
        print(step)
        if progress_callback:
            progress_callback(step, 2, 2)

        tables = _parse_vouchers(resp_text)
        for table_name, records in tables.items():
            count = _create_table_and_insert(conn, table_name, records)
            stats[table_name] = len(records)

    except Exception as e:
        print(f"  ERROR extracting vouchers: {e}")
        stats["vouchers"] = f"ERROR: {e}"

    conn.close()
    return stats


def sync_all(host, port=9000, db_path="tally_data.db", progress_callback=None):
    """
    Extract ALL data from Tally and load into SQLite.
    Returns stats dict with row counts per table.
    progress_callback(step_name, current, total) for UI progress updates.
    """
    started = datetime.datetime.now()
    print(f"\n{'=' * 60}")
    print(f"Seven Labs Vision — Tally Live Sync")
    print(f"{'=' * 60}")
    print(f"Host: {host}:{port}")
    print(f"Database: {db_path}")
    print(f"Started: {started.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}\n")

    # Step 0: Test connection & get company name + details
    step = "Testing connection..."
    print(step)
    if progress_callback:
        progress_callback(step, 0, 10)

    conn_result = test_connection(host, port)
    if not conn_result["success"]:
        print(f"FAILED: {conn_result['error']}")
        return {"success": False, "error": conn_result["error"]}

    company = conn_result["company"]
    print(f"  Connected! Company: {company}")

    # Get company details (GSTIN, state, etc.)
    company_info = {}
    try:
        company_info = get_company_info(host, port)
        if company_info:
            print(f"  GSTIN: {company_info.get('GSTIN', 'N/A')}")
            print(f"  State: {company_info.get('STATENAME', 'N/A')}")
    except Exception:
        pass
    print()

    all_stats = {}

    # Step 1: Masters
    print("--- MASTER DATA ---")
    master_stats = sync_masters(host, port, db_path, progress_callback)
    all_stats.update(master_stats)

    # Step 2: Vouchers
    print("\n--- TRANSACTION DATA ---")
    voucher_stats = sync_vouchers(host, port, db_path, progress_callback)
    all_stats.update(voucher_stats)

    # Step 3: Metadata
    conn = sqlite3.connect(db_path)
    conn.execute("DROP TABLE IF EXISTS _metadata")
    conn.execute("CREATE TABLE _metadata (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT INTO _metadata VALUES (?, ?)", ("company_name", company))
    conn.execute("INSERT INTO _metadata VALUES (?, ?)", ("loaded_at", datetime.datetime.now().isoformat()))
    conn.execute("INSERT INTO _metadata VALUES (?, ?)", ("sync_source", "tally_live"))
    conn.execute("INSERT INTO _metadata VALUES (?, ?)", ("tally_host", f"{host}:{port}"))
    # Try GSTIN from company info first, then fall back to voucher data
    gstin = company_info.get("GSTIN", "")
    if not gstin:
        try:
            vch_cols = {r[1] for r in conn.execute("PRAGMA table_info(trn_voucher)").fetchall()}
            if "CMPGSTIN" in vch_cols:
                row = conn.execute(
                    "SELECT CMPGSTIN FROM trn_voucher WHERE CMPGSTIN IS NOT NULL AND CMPGSTIN != '' LIMIT 1"
                ).fetchone()
                gstin = row[0] if row else ""
        except sqlite3.OperationalError:
            pass
    if gstin:
        conn.execute("INSERT INTO _metadata VALUES (?, ?)", ("company_gstin", gstin))
    if company_info.get("STATENAME"):
        conn.execute("INSERT INTO _metadata VALUES (?, ?)", ("company_state", company_info["STATENAME"]))
    if company_info.get("INCOMETAXNUMBER"):
        conn.execute("INSERT INTO _metadata VALUES (?, ?)", ("company_pan", company_info["INCOMETAXNUMBER"]))
    conn.commit()

    # Summary
    finished = datetime.datetime.now()
    elapsed = (finished - started).total_seconds()

    print(f"\n{'=' * 60}")
    print("SYNC COMPLETE — SUMMARY")
    print(f"{'=' * 60}")
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name != '_metadata' ORDER BY name"
    )
    total_rows = 0
    for (tbl,) in cur:
        count = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        cols = conn.execute(f"PRAGMA table_info({tbl})").fetchall()
        print(f"  {tbl}: {count} rows, {len(cols)} columns")
        total_rows += count

    conn.close()

    print(f"\n  Total: {total_rows} rows across all tables")
    print(f"  Time: {elapsed:.1f} seconds")
    print(f"  Database: {db_path}")
    print(f"{'=' * 60}\n")

    all_stats["success"] = True
    all_stats["company"] = company
    all_stats["total_rows"] = total_rows
    all_stats["elapsed_seconds"] = round(elapsed, 1)
    return all_stats


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sync data from TallyPrime to SQLite")
    parser.add_argument("--host", default="192.168.1.16", help="Tally host IP (default: 192.168.1.16)")
    parser.add_argument("--port", type=int, default=9000, help="Tally port (default: 9000)")
    parser.add_argument("--db", default="tally_data.db", help="SQLite database path")
    parser.add_argument("--test", action="store_true", help="Only test connection")
    parser.add_argument("--masters-only", action="store_true", help="Only sync master data")
    parser.add_argument("--vouchers-only", action="store_true", help="Only sync vouchers")
    args = parser.parse_args()

    if args.test:
        result = test_connection(args.host, args.port)
        if result["success"]:
            print(f"Connected to Tally! Company: {result['company']}")
        else:
            print(f"Connection failed: {result['error']}")
    elif args.masters_only:
        sync_masters(args.host, args.port, args.db)
    elif args.vouchers_only:
        sync_vouchers(args.host, args.port, args.db)
    else:
        sync_all(args.host, args.port, args.db)
