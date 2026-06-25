## Copyright (c) 2025 Cloudera, Inc. All Rights Reserved.
##
## This file is licensed under the Apache License Version 2.0 (the "License").
## You may not use this file except in compliance with the License.
## You may obtain a copy of the License at http:##www.apache.org/licenses/LICENSE-2.0.
##
## This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
## OF ANY KIND, either express or implied. Refer to the License for the specific
## permissions and limitations governing your use of the file.

import json
import re
from typing import Any

from iceberg_mcp_server.tools.impala_tools import (
    _first_row,
    _is_safe_table_identifier,
    _metadata_table,
    _qualify_table_name,
    _quote_identifier,
    _run_read_query,
    _TABLE_IDENTIFIER_PATTERN,
)

_DEFAULT_METADATA_LIMIT = 100
_DEFAULT_SNAPSHOT_LIMIT = 50
_MAX_METADATA_LIMIT = 1000
_MAX_SNAPSHOT_LIMIT = 500
_MAX_SAMPLE_LIMIT = 1000

_SNAPSHOT_ID_PATTERN = re.compile(r"^\d{1,20}$")
_TIMESTAMP_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2}(\.\d+)?)?$"
)


def _json_response(payload: dict[str, Any]) -> str:
    return json.dumps(payload, default=str)


def _table_error(message: str) -> str:
    return _json_response({"error": message})


def _validate_table(table: str) -> str | None:
    if not _is_safe_table_identifier(table):
        return "Invalid table name. Use [catalog.]database.table with alphanumeric identifiers."
    return None


def _validate_metadata_name(metadata_name: str) -> str | None:
    if not _TABLE_IDENTIFIER_PATTERN.match(metadata_name.strip()):
        return "Invalid metadata table name. Use alphanumeric identifiers (e.g. snapshots, files)."
    return None


def _validate_snapshot_id(snapshot_id: str) -> str | None:
    if not _SNAPSHOT_ID_PATTERN.match(str(snapshot_id).strip()):
        return "Invalid snapshot_id. Use a numeric Iceberg snapshot ID."
    return None


def _validate_timestamp(timestamp: str) -> str | None:
    ts = timestamp.strip()
    if not _TIMESTAMP_PATTERN.match(ts):
        return "Invalid timestamp. Use formats like '2024-07-18 10:12:20' or '2024-07-18'."
    if "'" in ts:
        return "Invalid timestamp."
    return None


def _clamp_limit(limit: int, default: int, maximum: int) -> int:
    if limit <= 0:
        return default
    return min(limit, maximum)


def _base_table_sql(qualified_table: str) -> str:
    return ".".join(_quote_identifier(part) for part in qualified_table.split("."))


def _show_metadata_tables_sql(qualified_table: str) -> str:
    parts = qualified_table.split(".")
    in_clause = ".".join(_quote_identifier(part) for part in parts)
    return f"SHOW METADATA TABLES IN {in_clause}"


def _parse_summary_value(summary: Any) -> Any:
    if summary is None or not isinstance(summary, str):
        return summary
    try:
        return json.loads(summary)
    except json.JSONDecodeError:
        return summary


def _parse_snapshot_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    parsed: list[dict[str, Any]] = []
    for row in rows:
        copy = dict(row)
        if "summary" in copy:
            copy["summary"] = _parse_summary_value(copy["summary"])
        parsed.append(copy)
    return parsed


def _select_columns_sql(columns: str | None) -> str:
    if not columns or not columns.strip():
        return "*"
    parts = [part.strip() for part in columns.split(",")]
    for part in parts:
        if not _TABLE_IDENTIFIER_PATTERN.match(part):
            raise ValueError(f"Invalid column name: {part}")
    return ", ".join(_quote_identifier(part) for part in parts)


def list_metadata_tables(table: str) -> str:
    error = _validate_table(table)
    if error:
        return _table_error(error)

    qualified_table = _qualify_table_name(table)
    result = _run_read_query(_show_metadata_tables_sql(qualified_table))
    if "error" in result:
        return _json_response({"table": qualified_table, "error": result["error"]})

    names = [row.get("name") for row in result.get("rows", []) if row.get("name") is not None]
    return _json_response({"table": qualified_table, "metadata_tables": names})


def describe_metadata_table(table: str, metadata_name: str) -> str:
    error = _validate_table(table)
    if error:
        return _table_error(error)
    meta_error = _validate_metadata_name(metadata_name)
    if meta_error:
        return _table_error(meta_error)

    qualified_table = _qualify_table_name(table)
    metadata_sql = _metadata_table(qualified_table, metadata_name.strip())
    result = _run_read_query(f"DESCRIBE {metadata_sql}")
    if "error" in result:
        return _json_response(
            {
                "table": qualified_table,
                "metadata_table": metadata_name,
                "error": result["error"],
            }
        )

    return _json_response(
        {
            "table": qualified_table,
            "metadata_table": metadata_name,
            "columns": result.get("rows", []),
        }
    )


def query_metadata_table(
    table: str,
    metadata_name: str,
    limit: int = _DEFAULT_METADATA_LIMIT,
    columns: str | None = None,
) -> str:
    error = _validate_table(table)
    if error:
        return _table_error(error)
    meta_error = _validate_metadata_name(metadata_name)
    if meta_error:
        return _table_error(meta_error)

    qualified_table = _qualify_table_name(table)
    limit = _clamp_limit(limit, _DEFAULT_METADATA_LIMIT, _MAX_METADATA_LIMIT)
    metadata_sql = _metadata_table(qualified_table, metadata_name.strip())

    try:
        column_sql = _select_columns_sql(columns)
    except ValueError as exc:
        return _table_error(str(exc))

    result = _run_read_query(
        f"SELECT {column_sql} FROM {metadata_sql} LIMIT {limit}"
    )
    if "error" in result:
        return _json_response(
            {
                "table": qualified_table,
                "metadata_table": metadata_name,
                "error": result["error"],
            }
        )

    rows = result.get("rows", [])
    if metadata_name.strip() == "snapshots":
        rows = _parse_snapshot_rows(rows)

    return _json_response(
        {
            "table": qualified_table,
            "metadata_table": metadata_name,
            "limit": limit,
            "columns": result.get("columns", []),
            "rows": rows,
        }
    )


def list_snapshots(table: str, limit: int = _DEFAULT_SNAPSHOT_LIMIT) -> str:
    error = _validate_table(table)
    if error:
        return _table_error(error)

    qualified_table = _qualify_table_name(table)
    limit = _clamp_limit(limit, _DEFAULT_SNAPSHOT_LIMIT, _MAX_SNAPSHOT_LIMIT)
    snapshots_table = _metadata_table(qualified_table, "snapshots")

    result = _run_read_query(
        f"""
        SELECT
            snapshot_id,
            committed_at,
            operation,
            parent_id,
            summary
        FROM {snapshots_table}
        ORDER BY committed_at DESC
        LIMIT {limit}
        """
    )
    if "error" in result:
        return _json_response({"table": qualified_table, "error": result["error"]})

    return _json_response(
        {
            "table": qualified_table,
            "limit": limit,
            "snapshots": _parse_snapshot_rows(result.get("rows", [])),
        }
    )


def describe_table_history(table: str) -> str:
    """List snapshot history via Impala DESCRIBE HISTORY (alternative to snapshots metadata)."""
    error = _validate_table(table)
    if error:
        return _table_error(error)

    qualified_table = _qualify_table_name(table)
    base_sql = _base_table_sql(qualified_table)
    result = _run_read_query(f"DESCRIBE HISTORY {base_sql}")
    if "error" in result:
        return _json_response({"table": qualified_table, "error": result["error"]})

    return _json_response(
        {
            "table": qualified_table,
            "history": result.get("rows", []),
            "columns": result.get("columns", []),
        }
    )


def get_snapshot_summary(table: str, snapshot_id: str) -> str:
    error = _validate_table(table)
    if error:
        return _table_error(error)
    snap_error = _validate_snapshot_id(snapshot_id)
    if snap_error:
        return _table_error(snap_error)

    qualified_table = _qualify_table_name(table)
    snapshots_table = _metadata_table(qualified_table, "snapshots")
    snapshot_id = str(snapshot_id).strip()

    snapshot_result = _run_read_query(
        f"""
        SELECT
            snapshot_id,
            committed_at,
            operation,
            parent_id,
            manifest_list,
            summary
        FROM {snapshots_table}
        WHERE snapshot_id = {snapshot_id}
        """
    )
    if "error" in snapshot_result:
        return _json_response(
            {"table": qualified_table, "snapshot_id": snapshot_id, "error": snapshot_result["error"]}
        )

    snapshot_row = _first_row(snapshot_result)
    if not snapshot_row:
        return _json_response(
            {
                "table": qualified_table,
                "snapshot_id": snapshot_id,
                "error": "Snapshot not found.",
            }
        )

    snapshot_row = dict(snapshot_row)
    snapshot_row["summary"] = _parse_summary_value(snapshot_row.get("summary"))

    history_table = _metadata_table(qualified_table, "history")
    history_result = _run_read_query(
        f"""
        SELECT made_current_at, is_current_ancestor
        FROM {history_table}
        WHERE snapshot_id = {snapshot_id}
        ORDER BY made_current_at DESC
        LIMIT 1
        """
    )

    payload: dict[str, Any] = {
        "table": qualified_table,
        "snapshot_id": snapshot_id,
        "snapshot": snapshot_row,
    }
    if "error" in history_result:
        payload["history_error"] = history_result["error"]
    else:
        payload["history"] = _first_row(history_result)

    return _json_response(payload)


def list_refs(table: str) -> str:
    error = _validate_table(table)
    if error:
        return _table_error(error)

    qualified_table = _qualify_table_name(table)
    refs_table = _metadata_table(qualified_table, "refs")
    result = _run_read_query(f"SELECT * FROM {refs_table}")
    if "error" in result:
        return _json_response({"table": qualified_table, "error": result["error"]})

    return _json_response(
        {
            "table": qualified_table,
            "refs": result.get("rows", []),
            "columns": result.get("columns", []),
        }
    )


def query_at_snapshot(
    table: str,
    snapshot_id: str,
    limit: int = 100,
    columns: str | None = None,
) -> str:
    error = _validate_table(table)
    if error:
        return _table_error(error)
    snap_error = _validate_snapshot_id(snapshot_id)
    if snap_error:
        return _table_error(snap_error)

    qualified_table = _qualify_table_name(table)
    limit = _clamp_limit(limit, 100, _MAX_SAMPLE_LIMIT)
    snapshot_id = str(snapshot_id).strip()
    base_sql = _base_table_sql(qualified_table)

    try:
        column_sql = _select_columns_sql(columns)
    except ValueError as exc:
        return _table_error(str(exc))

    result = _run_read_query(
        f"""
        SELECT {column_sql}
        FROM {base_sql}
        FOR SYSTEM_VERSION AS OF {snapshot_id}
        LIMIT {limit}
        """
    )
    if "error" in result:
        return _json_response(
            {
                "table": qualified_table,
                "snapshot_id": snapshot_id,
                "error": result["error"],
            }
        )

    return _json_response(
        {
            "table": qualified_table,
            "snapshot_id": snapshot_id,
            "limit": limit,
            "columns": result.get("columns", []),
            "rows": result.get("rows", []),
        }
    )


def query_at_timestamp(
    table: str,
    timestamp: str,
    limit: int = 100,
    columns: str | None = None,
) -> str:
    error = _validate_table(table)
    if error:
        return _table_error(error)
    ts_error = _validate_timestamp(timestamp)
    if ts_error:
        return _table_error(ts_error)

    qualified_table = _qualify_table_name(table)
    limit = _clamp_limit(limit, 100, _MAX_SAMPLE_LIMIT)
    ts = timestamp.strip()
    base_sql = _base_table_sql(qualified_table)

    try:
        column_sql = _select_columns_sql(columns)
    except ValueError as exc:
        return _table_error(str(exc))

    result = _run_read_query(
        f"""
        SELECT {column_sql}
        FROM {base_sql}
        FOR SYSTEM_TIME AS OF '{ts}'
        LIMIT {limit}
        """
    )
    if "error" in result:
        return _json_response(
            {
                "table": qualified_table,
                "timestamp": ts,
                "error": result["error"],
            }
        )

    return _json_response(
        {
            "table": qualified_table,
            "timestamp": ts,
            "limit": limit,
            "columns": result.get("columns", []),
            "rows": result.get("rows", []),
        }
    )


def _summary_diff(
    summary_a: Any,
    summary_b: Any,
) -> dict[str, dict[str, Any]]:
    if not isinstance(summary_a, dict):
        summary_a = {}
    if not isinstance(summary_b, dict):
        summary_b = {}

    all_keys = sorted(set(summary_a.keys()) | set(summary_b.keys()))
    diff: dict[str, dict[str, Any]] = {}
    for key in all_keys:
        val_a = summary_a.get(key)
        val_b = summary_b.get(key)
        if val_a != val_b:
            diff[key] = {"a": val_a, "b": val_b}
    return diff


def diff_snapshots(table: str, snapshot_id_a: str, snapshot_id_b: str) -> str:
    error = _validate_table(table)
    if error:
        return _table_error(error)
    for label, snap_id in (("snapshot_id_a", snapshot_id_a), ("snapshot_id_b", snapshot_id_b)):
        snap_error = _validate_snapshot_id(snap_id)
        if snap_error:
            return _table_error(f"{label}: {snap_error}")

    qualified_table = _qualify_table_name(table)
    id_a = str(snapshot_id_a).strip()
    id_b = str(snapshot_id_b).strip()
    snapshots_table = _metadata_table(qualified_table, "snapshots")

    result = _run_read_query(
        f"""
        SELECT
            snapshot_id,
            committed_at,
            operation,
            parent_id,
            summary
        FROM {snapshots_table}
        WHERE snapshot_id IN ({id_a}, {id_b})
        """
    )
    if "error" in result:
        return _json_response({"table": qualified_table, "error": result["error"]})

    rows = _parse_snapshot_rows(result.get("rows", []))
    by_id = {str(row.get("snapshot_id")): row for row in rows}
    row_a = by_id.get(id_a)
    row_b = by_id.get(id_b)

    if not row_a or not row_b:
        return _json_response(
            {
                "table": qualified_table,
                "error": "One or both snapshot IDs were not found.",
                "snapshot_id_a": id_a,
                "snapshot_id_b": id_b,
                "found_snapshot_ids": list(by_id.keys()),
            }
        )

    summary_a = row_a.get("summary")
    summary_b = row_b.get("summary")

    return _json_response(
        {
            "table": qualified_table,
            "snapshot_a": row_a,
            "snapshot_b": row_b,
            "diff": {
                "operation_changed": row_a.get("operation") != row_b.get("operation"),
                "committed_at_a": row_a.get("committed_at"),
                "committed_at_b": row_b.get("committed_at"),
                "parent_id_a": row_a.get("parent_id"),
                "parent_id_b": row_b.get("parent_id"),
                "summary_diff": _summary_diff(summary_a, summary_b),
            },
        }
    )
