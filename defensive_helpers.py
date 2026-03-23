"""
Defensive helper utilities for Tally data processing.
Provides safe database access patterns that work with ANY company's Tally data.

Author: Seven Labs Vision
Date: 2026-03-23
"""

import sqlite3
import logging

logger = logging.getLogger(__name__)

# Cache for table column info per connection
_column_cache = {}


def get_table_columns(conn, table_name):
    """
    Get list of column names for a table using PRAGMA table_info.
    Results are cached per connection id + table name.

    Args:
        conn: sqlite3 connection
        table_name: name of the table

    Returns:
        set of column names (uppercase), or empty set if table doesn't exist
    """
    cache_key = (id(conn), table_name)
    if cache_key in _column_cache:
        return _column_cache[cache_key]

    try:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table_name})")
        cols = {row[1].upper() for row in cur.fetchall()}
        _column_cache[cache_key] = cols
        return cols
    except Exception:
        _column_cache[cache_key] = set()
        return set()


def table_exists(conn, table_name):
    """Check if a table exists in the database."""
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        return cur.fetchone() is not None
    except Exception:
        return False


def column_exists(conn, table_name, column_name):
    """Check if a column exists in a table."""
    cols = get_table_columns(conn, table_name)
    return column_name.upper() in cols


def safe_fetchone(cursor_or_result):
    """
    Safely fetch one row, returning None if no results.
    Works with both cursor objects and already-fetched results.
    """
    try:
        if cursor_or_result is None:
            return None
        if hasattr(cursor_or_result, 'fetchone'):
            return cursor_or_result.fetchone()
        return cursor_or_result
    except Exception:
        return None


def safe_fetchall(cursor_or_result):
    """
    Safely fetch all rows, returning empty list if no results.
    """
    try:
        if cursor_or_result is None:
            return []
        if hasattr(cursor_or_result, 'fetchall'):
            return cursor_or_result.fetchall()
        return list(cursor_or_result) if cursor_or_result else []
    except Exception:
        return []


def safe_float(value, default=0.0):
    """Safely convert a value to float."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_divide(numerator, denominator, default=0.0):
    """Safely divide two numbers, returning default if denominator is zero."""
    try:
        if denominator is None or denominator == 0:
            return default
        return numerator / denominator
    except (TypeError, ZeroDivisionError):
        return default


def safe_sql_with_column_check(conn, sql, table_name, required_columns, params=None):
    """
    Execute SQL only if all required columns exist in the table.

    Args:
        conn: sqlite3 connection
        sql: SQL query string
        table_name: table to check columns against
        required_columns: list of column names that must exist
        params: optional query parameters

    Returns:
        cursor result or None if columns missing
    """
    cols = get_table_columns(conn, table_name)
    for col in required_columns:
        if col.upper() not in cols:
            logger.debug(f"Column {col} not found in {table_name}, skipping query")
            return None

    try:
        if params:
            return conn.execute(sql, params)
        return conn.execute(sql)
    except Exception as e:
        logger.warning(f"Query failed: {e}")
        return None


def clear_column_cache():
    """Clear the column cache (useful when switching databases)."""
    global _column_cache
    _column_cache = {}
