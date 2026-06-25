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
    _run_read_query,
)

_DEFAULT_HOT_PARTITIONS_LIMIT = 20
_MAX_HOT_PARTITIONS_LIMIT = 100
_LARGE_FILE_COUNT_THRESHOLD = 1000
_LARGE_SCAN_BYTES_THRESHOLD = 100 * 1024 * 1024 * 1024  # 100 GB
_SKEW_RATIO_THRESHOLD = 10

_PARTITION_RATIO_PATTERN = re.compile(
    r"partitions?\s*[=:]\s*(\d+)\s*/\s*(\d+)",
    re.IGNORECASE,
)
_INPUT_PARTITIONS_PATTERN = re.compile(
    r"input\s+partitions?\s*[=:]\s*(\d+)",
    re.IGNORECASE,
)


def _json_response(payload: dict[str, Any]) -> str:
    return json.dumps(payload, default=str)


def _table_error(message: str) -> str:
    return _json_response({"error": message})


def _validate_table(table: str) -> str | None:
    if not _is_safe_table_identifier(table):
        return "Invalid table name. Use [catalog.]database.table with alphanumeric identifiers."
    return None


def _validate_readonly_query(query: str) -> str | None:
    stripped = query.strip()
    if not stripped:
        return "Query is empty."
    first_token = stripped.lower().split()[0]
    if first_token not in {"select", "with"}:
        return "Only SELECT or WITH queries are supported for performance analysis."
    return None


def _clamp_limit(limit: int, default: int, maximum: int) -> int:
    if limit <= 0:
        return default
    return min(limit, maximum)


def _explain_plan_text(result: dict[str, Any]) -> str:
    if "error" in result:
        return ""
    lines: list[str] = []
    for row in result.get("rows", []):
        if isinstance(row, dict):
            lines.extend(str(value) for value in row.values() if value is not None)
        else:
            lines.append(str(row))
    return "\n".join(lines)


def _run_explain(query: str) -> dict[str, Any]:
    cleaned = query.strip().rstrip(";")
    return _run_read_query(f"EXPLAIN {cleaned}")


def _analyze_partition_pruning(plan_text: str) -> dict[str, Any]:
    lower = plan_text.lower()
    analysis: dict[str, Any] = {
        "plan_mentions_partitions": "partition" in lower,
        "partition_ratio_matches": [],
        "input_partitions": [],
        "likely_partition_pruning": False,
        "signals": [],
    }

    for match in _PARTITION_RATIO_PATTERN.finditer(plan_text):
        used, total = int(match.group(1)), int(match.group(2))
        analysis["partition_ratio_matches"].append(
            {"partitions_used": used, "partitions_total": total}
        )
        if total > 0 and used < total:
            analysis["likely_partition_pruning"] = True
            analysis["signals"].append(
                f"Plan reads {used} of {total} partitions (possible partition pruning)."
            )
        elif total > 0 and used == total:
            analysis["signals"].append(
                f"Plan reads all {total} partitions (no partition pruning detected)."
            )

    for match in _INPUT_PARTITIONS_PATTERN.finditer(plan_text):
        count = int(match.group(1))
        analysis["input_partitions"].append(count)
        if count == 0:
            analysis["signals"].append("Plan reports zero input partitions.")
        elif count == 1:
            analysis["likely_partition_pruning"] = True
            analysis["signals"].append("Plan reports a single input partition.")

    if "partition key" in lower and not analysis["likely_partition_pruning"]:
        analysis["signals"].append(
            "Partition key appears in plan but pruning could not be confirmed from text."
        )

    if "iceberg" in lower and not analysis["partition_ratio_matches"]:
        analysis["signals"].append(
            "Iceberg scan detected; verify partition predicates if the table is partitioned."
        )

    if not analysis["signals"]:
        analysis["signals"].append(
            "No explicit partition pruning indicators found; review the plan manually."
        )

    return analysis


def _derive_scan_cost_signals(metrics: dict[str, Any]) -> list[str]:
    signals: list[str] = []

    file_count = metrics.get("data_file_count")
    total_bytes = metrics.get("total_scan_bytes")
    partition_count = metrics.get("partition_count")
    avg_file_size = metrics.get("avg_file_size_bytes")
    delete_file_count = metrics.get("delete_file_count")
    max_partition_files = metrics.get("max_partition_file_count")
    avg_partition_files = metrics.get("avg_partition_file_count")

    if file_count is not None and file_count > _LARGE_FILE_COUNT_THRESHOLD:
        signals.append(
            f"High data file count ({file_count}): full scans may be expensive; prefer filters."
        )

    if total_bytes is not None and total_bytes > _LARGE_SCAN_BYTES_THRESHOLD:
        gb = total_bytes / (1024**3)
        signals.append(
            f"Large table footprint (~{gb:.1f} GB data files): full scans are costly."
        )

    if partition_count is not None and partition_count > 1:
        signals.append(
            f"Table has {partition_count} partitions: use partition filters to limit scan scope."
        )
    elif partition_count == 1:
        signals.append("Single partition or unpartitioned table: queries may scan all data files.")

    if (
        avg_file_size is not None
        and file_count is not None
        and avg_file_size < 16 * 1024 * 1024
        and file_count > 100
    ):
        signals.append(
            "Many small files detected: metadata and open costs can dominate query time."
        )

    if delete_file_count is not None and delete_file_count > 0:
        signals.append(
            f"{delete_file_count} delete file(s): merge-on-read adds read overhead."
        )

    if (
        max_partition_files is not None
        and avg_partition_files is not None
        and avg_partition_files > 0
        and max_partition_files / avg_partition_files >= _SKEW_RATIO_THRESHOLD
    ):
        signals.append(
            f"Partition skew detected (max {max_partition_files} vs avg {avg_partition_files:.1f} files)."
        )

    if not signals:
        signals.append("No major scan-cost warnings from current metadata.")

    return signals


def explain_query(query: str) -> str:
    error = _validate_readonly_query(query)
    if error:
        return _table_error(error)

    result = _run_explain(query)
    if "error" in result:
        return _json_response({"error": result["error"]})

    plan_text = _explain_plan_text(result)
    return _json_response(
        {
            "plan": plan_text,
            "plan_rows": result.get("rows", []),
            "columns": result.get("columns", []),
        }
    )


def partition_pruning_check(query: str) -> str:
    error = _validate_readonly_query(query)
    if error:
        return _table_error(error)

    result = _run_explain(query)
    if "error" in result:
        return _json_response({"error": result["error"]})

    plan_text = _explain_plan_text(result)
    analysis = _analyze_partition_pruning(plan_text)

    return _json_response(
        {
            "query": query.strip(),
            "plan": plan_text,
            "analysis": analysis,
        }
    )


def table_scan_cost_hints(table: str) -> str:
    error = _validate_table(table)
    if error:
        return _table_error(error)

    qualified_table = _qualify_table_name(table)
    files_table = _metadata_table(qualified_table, "files")
    partitions_table = _metadata_table(qualified_table, "partitions")

    files_summary = _run_read_query(
        f"""
        SELECT
            COUNT(*) AS data_file_count,
            SUM(record_count) AS total_records,
            SUM(file_size_in_bytes) AS total_scan_bytes,
            AVG(file_size_in_bytes) AS avg_file_size_bytes,
            MIN(file_size_in_bytes) AS min_file_size_bytes,
            MAX(file_size_in_bytes) AS max_file_size_bytes,
            SUM(CASE WHEN content != 0 THEN 1 ELSE 0 END) AS delete_file_count
        FROM {files_table}
        """
    )
    if "error" in files_summary:
        return _json_response({"table": qualified_table, "error": files_summary["error"]})

    partitions_summary = _run_read_query(
        f"""
        SELECT
            COUNT(*) AS partition_count,
            SUM(record_count) AS total_records,
            SUM(file_count) AS total_partition_files,
            AVG(file_count) AS avg_partition_file_count,
            MAX(file_count) AS max_partition_file_count,
            MIN(file_count) AS min_partition_file_count,
            SUM(total_data_file_size_in_bytes) AS total_partition_bytes
        FROM {partitions_table}
        """
    )

    metrics = _first_row(files_summary) or {}
    if "error" not in partitions_summary:
        metrics.update(_first_row(partitions_summary) or {})

    if metrics.get("partition_count") and metrics.get("data_file_count"):
        metrics["avg_files_per_partition"] = (
            metrics["data_file_count"] / metrics["partition_count"]
        )

    cost_signals = _derive_scan_cost_signals(metrics)

    payload: dict[str, Any] = {
        "table": qualified_table,
        "metrics": metrics,
        "cost_signals": cost_signals,
    }
    if "error" in partitions_summary:
        payload["partitions_error"] = partitions_summary["error"]

    return _json_response(payload)


def hot_partitions(table: str, limit: int = _DEFAULT_HOT_PARTITIONS_LIMIT) -> str:
    error = _validate_table(table)
    if error:
        return _table_error(error)

    qualified_table = _qualify_table_name(table)
    limit = _clamp_limit(limit, _DEFAULT_HOT_PARTITIONS_LIMIT, _MAX_HOT_PARTITIONS_LIMIT)
    partitions_table = _metadata_table(qualified_table, "partitions")

    # Use SELECT * because the partition identifier column varies by Impala/Iceberg
    # version (partition struct, partition_value, or absent for unpartitioned tables).
    top_by_files = _run_read_query(
        f"""
        SELECT *
        FROM {partitions_table}
        ORDER BY file_count DESC, record_count DESC
        LIMIT {limit}
        """
    )
    if "error" in top_by_files:
        return _json_response({"table": qualified_table, "error": top_by_files["error"]})

    top_by_records = _run_read_query(
        f"""
        SELECT *
        FROM {partitions_table}
        ORDER BY record_count DESC, file_count DESC
        LIMIT {limit}
        """
    )

    summary = _run_read_query(
        f"""
        SELECT
            COUNT(*) AS partition_count,
            SUM(file_count) AS total_files,
            AVG(file_count) AS avg_file_count,
            MAX(file_count) AS max_file_count,
            STDDEV(file_count) AS stddev_file_count
        FROM {partitions_table}
        """
    )

    payload: dict[str, Any] = {
        "table": qualified_table,
        "limit": limit,
        "summary": _first_row(summary) or {},
        "columns": top_by_files.get("columns", []),
        "top_by_file_count": top_by_files.get("rows", []),
    }
    if "error" in top_by_records:
        payload["top_by_record_count_error"] = top_by_records["error"]
    else:
        payload["top_by_record_count"] = top_by_records.get("rows", [])

    if "error" in summary:
        payload["summary_error"] = summary["error"]

    rows = top_by_files.get("rows", [])
    if rows and payload.get("summary", {}).get("avg_file_count"):
        avg_files = payload["summary"]["avg_file_count"]
        if avg_files and rows[0].get("file_count", 0) / avg_files >= _SKEW_RATIO_THRESHOLD:
            payload["skew_signal"] = (
                f"Hottest partition has {rows[0]['file_count']} files "
                f"vs average {avg_files:.1f}."
            )

    return _json_response(payload)
