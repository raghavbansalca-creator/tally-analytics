"""
Seven Labs Vision — Tally Data Loader
Reads raw XML files from received_data/ and loads into SQLite.
"""

import json
import os
import re
import sqlite3
import xml.etree.ElementTree as ET

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "received_data")
DB_PATH = os.path.join(os.path.dirname(__file__), "tally_data.db")


def sanitize_xml(data):
    if isinstance(data, bytes):
        data = data.decode("utf-8", errors="replace")
    data = re.sub(r'&#(?:0*[0-8]|0*1[0-1]|0*1[4-9]|0*2[0-9]|0*3[0-1]);', '', data)
    data = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', data)
    return data


def read_xml(filepath):
    with open(filepath, "r", errors="replace") as f:
        return sanitize_xml(f.read())


def parse_flat(xml_str, tag):
    """Parse XML into list of dicts from elements matching tag.
    Includes attributes and simple (non-nested) child text."""
    root = ET.fromstring(xml_str)
    records = []
    for el in root.iter(tag):
        rec = dict(el.attrib)
        has_children = False
        for child in el:
            if not list(child):  # only leaf nodes
                val = (child.text or "").strip()
                if val:
                    rec[child.tag] = val
                has_children = True
            else:
                has_children = True
        if rec:  # skip empty elements
            records.append(rec)
    return records


def parse_vouchers(xml_str):
    """Parse voucher XML into 6 flat tables."""
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
        # Voucher header
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

        # Ledger entries
        for le_tag in LEDGER_TAGS:
            for le in vch.findall(le_tag):
                le_data = {"VOUCHER_GUID": vch_guid}
                le_data.update(le.attrib)
                for child in le:
                    if child.tag in ("BILLALLOCATIONS.LIST", "BANKALLOCATIONS.LIST",
                                     "CATEGORYALLOCATIONS.LIST", "COSTCENTREALLOCATIONS.LIST",
                                     "COSTTRACKALLOCATIONS.LIST", "INVENTORYALLOCATIONS.LIST"):
                        continue
                    if child.tag.endswith(".LIST"):
                        continue
                    if not list(child):
                        val = (child.text or "").strip()
                        if val:
                            le_data[child.tag] = val
                trn_accounting.append(le_data)

                # Bill allocations
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

                # Bank allocations
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

        # Inventory entries
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

                # Batch allocations
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


def collect_all_keys(records):
    """Get all unique keys across all records."""
    keys = set()
    for rec in records:
        keys.update(rec.keys())
    return sorted(keys)


def create_table_and_insert(conn, table_name, records):
    if not records:
        print(f"  {table_name}: 0 records (skipped)")
        return

    columns = collect_all_keys(records)
    # Clean column names (replace dots with underscores)
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


def find_latest_xml(company_raw_dir, prefix):
    """Find the latest XML file matching a prefix."""
    files = [f for f in os.listdir(company_raw_dir) if f.startswith(prefix) and f.endswith(".xml")]
    if not files:
        return None
    files.sort(reverse=True)  # latest first
    return os.path.join(company_raw_dir, files[0])


def load_company(company_name):
    company_raw_dir = os.path.join(RAW_DIR, company_name, "raw")
    if not os.path.isdir(company_raw_dir):
        print(f"No raw data found for {company_name}")
        return

    print(f"\nLoading {company_name} into {DB_PATH}")
    print("=" * 50)

    conn = sqlite3.connect(DB_PATH)

    # Master tables
    master_config = {
        "mst_group": ("groups", "GROUP"),
        "mst_ledger": ("ledgers", "LEDGER"),
        "mst_cost_centre": ("cost_centres", "COSTCENTRE"),
        "mst_stock_group": ("stock_groups", "STOCKGROUP"),
        "mst_stock_item": ("stock_items", "STOCKITEM"),
        "mst_godown": ("godowns", "GODOWN"),
        "mst_voucher_type": ("voucher_types", "VOUCHERTYPE"),
    }

    for table_name, (file_prefix, xml_tag) in master_config.items():
        xml_file = find_latest_xml(company_raw_dir, file_prefix)
        if not xml_file:
            print(f"  {table_name}: no XML file found")
            continue
        xml_str = read_xml(xml_file)
        records = parse_flat(xml_str, xml_tag)
        create_table_and_insert(conn, table_name, records)

    # Vouchers — use the LATEST (largest) file which has nested entries
    vch_files = [f for f in os.listdir(company_raw_dir) if f.startswith("vouchers") and f.endswith(".xml")]
    if vch_files:
        vch_files.sort(key=lambda f: os.path.getsize(os.path.join(company_raw_dir, f)), reverse=True)
        vch_file = os.path.join(company_raw_dir, vch_files[0])
        print(f"\n  Parsing vouchers from {os.path.basename(vch_file)} ({os.path.getsize(vch_file) / 1024 / 1024:.1f} MB)...")

        xml_str = read_xml(vch_file)
        tables = parse_vouchers(xml_str)

        for table_name, records in tables.items():
            create_table_and_insert(conn, table_name, records)
    else:
        print("  No voucher XML found")

    # Create metadata
    conn.execute("DROP TABLE IF EXISTS _metadata")
    conn.execute("CREATE TABLE _metadata (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT INTO _metadata VALUES (?, ?)", ("company_name", company_name))
    conn.execute("INSERT INTO _metadata VALUES (?, ?)", ("loaded_at", __import__("datetime").datetime.now().isoformat()))
    conn.commit()

    # Print summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != '_metadata' ORDER BY name")
    for (table_name,) in cur:
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        cols = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        print(f"  {table_name}: {count} rows, {len(cols)} columns")

    conn.close()
    print(f"\nDatabase saved to: {DB_PATH}")


if __name__ == "__main__":
    # Find all companies in received_data
    if not os.path.isdir(RAW_DIR):
        print(f"No received_data directory found at {RAW_DIR}")
        exit(1)

    companies = [d for d in os.listdir(RAW_DIR) if os.path.isdir(os.path.join(RAW_DIR, d))]
    if not companies:
        print("No company data found")
        exit(1)

    for company in companies:
        load_company(company)
