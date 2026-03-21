"""
Seven Labs Vision -- Dynamic Sidebar Filters
Auto-adapts to whatever Tally company is loaded.
Shows only filters that are relevant to the data present.
"""

import streamlit as st


def _has_table(conn, table_name):
    """Check if a table exists in the database."""
    row = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row[0] > 0


def _has_column(conn, table_name, column_name):
    """Check if a column exists in a table."""
    try:
        cols = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return any(c[1].upper() == column_name.upper() for c in cols)
    except Exception:
        return False


def _safe_query(conn, sql, params=None):
    """Execute a query and return results, or empty list on error."""
    try:
        if params:
            return conn.execute(sql, params).fetchall()
        return conn.execute(sql).fetchall()
    except Exception:
        return []


def render_sidebar_filters(conn, page_key="main"):
    """
    Render dynamic sidebar filters based on what data exists.

    Returns a dict with filter selections:
    {
        "voucher_types": list or None,      # selected voucher types (None = all)
        "cost_centres": list or None,        # selected cost centres (None = all / not applicable)
        "ledger_groups": list or None,       # selected ledger groups (None = all)
        "has_stock": bool,                   # whether stock items exist
        "has_godowns": bool,                 # whether multiple godowns exist
        "godowns": list or None,             # selected godowns (None = all / not applicable)
        "all_vch_types": list,               # full list of voucher types in data
        "all_cost_centres": list,            # full list of cost centres
        "all_ledger_groups": list,           # full list of ledger groups
    }
    """
    filters = {
        "voucher_types": None,
        "cost_centres": None,
        "ledger_groups": None,
        "has_stock": False,
        "has_godowns": False,
        "godowns": None,
        "all_vch_types": [],
        "all_cost_centres": [],
        "all_ledger_groups": [],
    }

    with st.sidebar.expander("Filters", expanded=False):

        # ── 1. VOUCHER TYPE FILTER ────────────────────────────────────────
        _vch_rows = _safe_query(
            conn,
            "SELECT DISTINCT VOUCHERTYPENAME FROM trn_voucher "
            "WHERE VOUCHERTYPENAME IS NOT NULL AND VOUCHERTYPENAME != '' "
            "ORDER BY VOUCHERTYPENAME",
        )
        _vch_type_list = [r[0] for r in _vch_rows if r[0]]
        filters["all_vch_types"] = _vch_type_list

        if _vch_type_list:
            st.markdown("**Voucher Type**")
            selected_vch_types = st.multiselect(
                "Filter by voucher type",
                options=_vch_type_list,
                default=_vch_type_list,
                key=f"vch_type_filter_{page_key}",
                label_visibility="collapsed",
            )
            # Only apply filter when not all are selected
            if selected_vch_types and len(selected_vch_types) < len(_vch_type_list):
                filters["voucher_types"] = selected_vch_types

        # ── 2. COST CENTRE FILTER (only if cost centres exist) ────────────
        _cc_list = []
        if _has_table(conn, "mst_cost_centre"):
            _cc_rows = _safe_query(
                conn,
                "SELECT DISTINCT NAME FROM mst_cost_centre "
                "WHERE NAME IS NOT NULL AND NAME != '' ORDER BY NAME",
            )
            _cc_list = [r[0] for r in _cc_rows if r[0]]
        filters["all_cost_centres"] = _cc_list

        if _cc_list:
            st.markdown("**Cost Centre**")
            selected_cc = st.multiselect(
                "Filter by cost centre",
                options=_cc_list,
                default=_cc_list,
                key=f"cc_filter_{page_key}",
                label_visibility="collapsed",
            )
            if selected_cc and len(selected_cc) < len(_cc_list):
                filters["cost_centres"] = selected_cc

        # ── 3. LEDGER GROUP FILTER ────────────────────────────────────────
        _grp_rows = _safe_query(
            conn,
            "SELECT DISTINCT parent FROM mst_ledger "
            "WHERE parent IS NOT NULL AND parent != '' ORDER BY parent",
        )
        _group_list = [r[0] for r in _grp_rows if r[0]]
        filters["all_ledger_groups"] = _group_list

        if _group_list:
            st.markdown("**Account Group**")
            selected_groups = st.multiselect(
                "Filter by ledger group",
                options=_group_list,
                default=[],
                placeholder="All groups",
                key=f"group_filter_{page_key}",
                label_visibility="collapsed",
            )
            if selected_groups:
                filters["ledger_groups"] = selected_groups

        # ── 4. DETECT STOCK / GODOWN PRESENCE ────────────────────────────
        if _has_table(conn, "mst_stock_item"):
            _stock_count = _safe_query(
                conn, "SELECT COUNT(*) FROM mst_stock_item"
            )
            filters["has_stock"] = bool(
                _stock_count and _stock_count[0][0] > 0
            )

        if _has_table(conn, "mst_godown"):
            _godown_rows = _safe_query(
                conn,
                "SELECT DISTINCT NAME FROM mst_godown "
                "WHERE NAME IS NOT NULL AND NAME != '' ORDER BY NAME",
            )
            _godown_list = [r[0] for r in _godown_rows if r[0]]
            filters["has_godowns"] = len(_godown_list) > 1

            if len(_godown_list) > 1:
                st.markdown("**Godown**")
                selected_godowns = st.multiselect(
                    "Filter by godown",
                    options=_godown_list,
                    default=_godown_list,
                    key=f"godown_filter_{page_key}",
                    label_visibility="collapsed",
                )
                if selected_godowns and len(selected_godowns) < len(
                    _godown_list
                ):
                    filters["godowns"] = selected_godowns

    return filters


def build_vch_type_sql(filters, voucher_table_alias="v"):
    """
    Build a SQL WHERE clause fragment for voucher type filtering.
    Uses parameterised placeholders.

    Returns (sql_fragment, params_list).
    sql_fragment is empty string when no filter is active.
    """
    vch_types = filters.get("voucher_types")
    if not vch_types:
        return "", []
    placeholders = ",".join(["?"] * len(vch_types))
    sql = f" AND {voucher_table_alias}.VOUCHERTYPENAME IN ({placeholders})"
    return sql, list(vch_types)


def build_group_filter_sql(filters, ledger_table_alias="l"):
    """
    Build a SQL WHERE clause fragment for ledger group filtering.
    Uses parameterised placeholders.

    Returns (sql_fragment, params_list).
    """
    groups = filters.get("ledger_groups")
    if not groups:
        return "", []
    placeholders = ",".join(["?"] * len(groups))
    sql = f" AND {ledger_table_alias}.PARENT IN ({placeholders})"
    return sql, list(groups)


def build_cost_centre_sql(conn, filters, accounting_table_alias="a"):
    """
    Build a SQL WHERE clause fragment for cost centre filtering.
    Only applies if the trn_accounting table has a COSTCENTRENAME column.
    Uses parameterised placeholders.

    Returns (sql_fragment, params_list).
    """
    cc = filters.get("cost_centres")
    if not cc:
        return "", []
    # Check if column exists
    if not _has_column(conn, "trn_accounting", "COSTCENTRENAME"):
        return "", []
    placeholders = ",".join(["?"] * len(cc))
    sql = f" AND {accounting_table_alias}.COSTCENTRENAME IN ({placeholders})"
    return sql, list(cc)
