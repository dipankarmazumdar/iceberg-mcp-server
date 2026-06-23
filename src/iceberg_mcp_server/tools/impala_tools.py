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
import os
import re
from typing import Any

from dotenv import load_dotenv
from impala.dbapi import connect

load_dotenv()

_TABLE_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_SMALL_FILE_THRESHOLD_BYTES = 16 * 1024 * 1024  # avg below ~16MB suggests real small-file risk
_SMALL_FILE_MANY_FILES_THRESHOLD = 500
_MANY_SNAPSHOTS_THRESHOLD = 100


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _safe_close(conn) -> None:
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        # impyla can raise if the HTTP transport was never opened.
        pass


# Helper to get Impala connection details from env vars
def get_db_connection():
    host = os.getenv("IMPALA_HOST", "coordinator-default-impala.example.com")
    port = int(os.getenv("IMPALA_PORT", "443"))
    user = os.getenv("IMPALA_USER", "username")
    password = os.getenv("IMPALA_PASSWORD", "password")
    database = os.getenv("IMPALA_DATABASE", "default")
    auth_mechanism = os.getenv("IMPALA_AUTH_MECHANISM", "LDAP")
    use_http_transport = _env_bool("IMPALA_USE_HTTP_TRANSPORT", True)
    http_path = os.getenv("IMPALA_HTTP_PATH", "cliservice")
    use_ssl = _env_bool("IMPALA_USE_SSL", True)

    return connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        auth_mechanism=auth_mechanism,
        use_http_transport=use_http_transport,
        http_path=http_path,
        use_ssl=use_ssl,
    )


def _is_safe_table_identifier(table: str) -> bool:
    parts = table.strip().split(".")
    if not 1 <= len(parts) <= 3:
        return False
    return all(_TABLE_IDENTIFIER_PATTERN.match(part) for part in parts)


def _qualify_table_name(table: str) -> str:
    table = table.strip()
    if "." in table:
        return table
    database = os.getenv("IMPALA_DATABASE", "default")
    return f"{database}.{table}"


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier}`"


def _metadata_table(qualified_table: str, metadata_name: str) -> str:
    # Impala reserved words (e.g. files, partitions) must be escaped in metadata paths.
    parts = [*qualified_table.split("."), metadata_name]
    return ".".join(_quote_identifier(part) for part in parts)


def _run_read_query(query: str) -> dict[str, Any]:
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query)
        if not cur.description:
            return {"columns": [], "rows": []}
        columns = [col[0] for col in cur.description]
        rows = [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]
        return {"columns": columns, "rows": rows}
    except Exception as e:
        return {"error": str(e)}
    finally:
        _safe_close(conn)


def _first_row(result: dict[str, Any]) -> dict[str, Any] | None:
    if "error" in result:
        return None
    rows = result.get("rows", [])
    return rows[0] if rows else None


def _derive_health_signals(health: dict[str, Any]) -> list[str]:
    signals: list[str] = []

    snapshots = health.get("snapshots", {})
    if "error" not in snapshots:
        snapshot_count = snapshots.get("summary", {}).get("snapshot_count")
        if snapshot_count is not None and snapshot_count > _MANY_SNAPSHOTS_THRESHOLD:
            signals.append(
                f"High snapshot count ({snapshot_count}): consider expiring old snapshots."
            )

    history = health.get("history", {})
    if "error" not in history:
        non_ancestor_count = history.get("summary", {}).get("non_ancestor_count")
        if non_ancestor_count is not None and non_ancestor_count > 0:
            signals.append(
                f"{non_ancestor_count} history entry(ies) are not current ancestors "
                "(possible rollbacks or divergent commits)."
            )

    files = health.get("files", {})
    if "error" not in files:
        summary = files.get("summary", {})
        file_count = summary.get("file_count")
        avg_file_size = summary.get("avg_file_size_bytes")
        delete_file_count = summary.get("delete_file_count")
        if delete_file_count is not None and delete_file_count > 0:
            signals.append(
                f"{delete_file_count} delete file(s) present: table may use merge-on-read or deletes."
            )
        if (
            file_count is not None
            and avg_file_size is not None
            and (
                avg_file_size < _SMALL_FILE_THRESHOLD_BYTES
                or (
                    avg_file_size < 128 * 1024 * 1024
                    and file_count > _SMALL_FILE_MANY_FILES_THRESHOLD
                )
            )
        ):
            signals.append(
                f"Average data file size is {int(avg_file_size)} bytes across {file_count} files: "
                "possible small-file problem; compaction may help."
            )

    partitions = health.get("partitions", {})
    if "error" not in partitions:
        partition_count = partitions.get("summary", {}).get("partition_count")
        total_records = partitions.get("summary", {}).get("total_records")
        if (
            partition_count is not None
            and total_records is not None
            and partition_count > 100
            and total_records / partition_count < 1000
        ):
            signals.append(
                f"Many partitions ({partition_count}) with low average records per partition: "
                "check partition granularity."
            )

    manifests = health.get("manifests", {})
    if "error" not in manifests:
        deleted_data_files = manifests.get("summary", {}).get("deleted_data_files")
        if deleted_data_files is not None and deleted_data_files > 0:
            signals.append(
                f"{deleted_data_files} data file deletion(s) recorded in current manifests."
            )

    if not signals:
        signals.append("No obvious health issues detected from metadata summaries.")

    return signals


def get_table_health(table: str) -> str:
    if not _is_safe_table_identifier(table):
        return "Error: Invalid table name. Use [catalog.]database.table with alphanumeric identifiers."

    qualified_table = _qualify_table_name(table)
    health: dict[str, Any] = {"table": qualified_table}

    snapshots_table = _metadata_table(qualified_table, "snapshots")
    snapshots_summary = _run_read_query(
        f"""
        SELECT
            COUNT(*) AS snapshot_count,
            MAX(committed_at) AS latest_committed_at
        FROM {snapshots_table}
        """
    )
    if "error" in snapshots_summary:
        health["snapshots"] = {"error": snapshots_summary["error"]}
    else:
        snapshots_recent = _run_read_query(
            f"""
            SELECT
                snapshot_id,
                committed_at,
                operation,
                parent_id,
                summary
            FROM {snapshots_table}
            ORDER BY committed_at DESC
            LIMIT 5
            """
        )
        health["snapshots"] = {
            "summary": _first_row(snapshots_summary) or {},
            "recent": snapshots_recent.get("rows", []),
        }
        if "error" in snapshots_recent:
            health["snapshots"]["recent_error"] = snapshots_recent["error"]

    history_table = _metadata_table(qualified_table, "history")
    history_summary = _run_read_query(
        f"""
        SELECT
            COUNT(*) AS history_entry_count,
            SUM(CASE WHEN NOT is_current_ancestor THEN 1 ELSE 0 END) AS non_ancestor_count
        FROM {history_table}
        """
    )
    if "error" in history_summary:
        health["history"] = {"error": history_summary["error"]}
    else:
        history_recent = _run_read_query(
            f"""
            SELECT
                made_current_at,
                snapshot_id,
                is_current_ancestor
            FROM {history_table}
            ORDER BY made_current_at DESC
            LIMIT 5
            """
        )
        health["history"] = {
            "summary": _first_row(history_summary) or {},
            "recent": history_recent.get("rows", []),
        }
        if "error" in history_recent:
            health["history"]["recent_error"] = history_recent["error"]

    files_table = _metadata_table(qualified_table, "files")
    files_summary = _run_read_query(
        f"""
        SELECT
            COUNT(*) AS file_count,
            SUM(record_count) AS total_records,
            SUM(file_size_in_bytes) AS total_size_bytes,
            AVG(file_size_in_bytes) AS avg_file_size_bytes,
            SUM(CASE WHEN content = 0 THEN 1 ELSE 0 END) AS data_file_count,
            SUM(CASE WHEN content != 0 THEN 1 ELSE 0 END) AS delete_file_count
        FROM {files_table}
        """
    )
    if "error" in files_summary:
        health["files"] = {"error": files_summary["error"]}
    else:
        health["files"] = {"summary": _first_row(files_summary) or {}}

    partitions_table = _metadata_table(qualified_table, "partitions")
    partitions_summary = _run_read_query(
        f"""
        SELECT
            COUNT(*) AS partition_count,
            SUM(record_count) AS total_records,
            SUM(file_count) AS total_files,
            SUM(total_data_file_size_in_bytes) AS total_data_size_bytes
        FROM {partitions_table}
        """
    )
    if "error" in partitions_summary:
        health["partitions"] = {"error": partitions_summary["error"]}
    else:
        health["partitions"] = {"summary": _first_row(partitions_summary) or {}}

    manifests_table = _metadata_table(qualified_table, "manifests")
    manifests_summary = _run_read_query(
        f"""
        SELECT
            COUNT(*) AS manifest_count,
            SUM(added_data_files_count) AS added_data_files,
            SUM(existing_data_files_count) AS existing_data_files,
            SUM(deleted_data_files_count) AS deleted_data_files,
            SUM(added_delete_files_count) AS added_delete_files,
            SUM(existing_delete_files_count) AS existing_delete_files,
            SUM(deleted_delete_files_count) AS deleted_delete_files
        FROM {manifests_table}
        """
    )
    if "error" in manifests_summary:
        health["manifests"] = {"error": manifests_summary["error"]}
    else:
        health["manifests"] = {"summary": _first_row(manifests_summary) or {}}

    metadata_log_table = _metadata_table(qualified_table, "metadata_log_entries")
    metadata_log_summary = _run_read_query(
        f"""
        SELECT COUNT(*) AS metadata_log_entry_count
        FROM {metadata_log_table}
        """
    )
    if "error" in metadata_log_summary:
        health["metadata_log"] = {"error": metadata_log_summary["error"]}
    else:
        health["metadata_log"] = {"summary": _first_row(metadata_log_summary) or {}}

    health["health_signals"] = _derive_health_signals(health)
    return json.dumps(health, default=str)


def execute_query(query: str) -> str:
    conn = None

    # Implement rudimentary SQL injection prevention
    # In this case, we only allow read-only queries
    # This is a very basic check and should be improved for production use
    readonly_prefixes = ["select", "show", "describe", "with"]

    if not query.strip().lower().split()[0] in readonly_prefixes:
        return "Only read-only queries are allowed."

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query)
        if cur.description:
            rows = cur.fetchall()
            result = json.dumps(rows, default=str)
        else:
            conn.commit()
            result = "Query executed successfully."
        cur.close()
        return result
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        _safe_close(conn)


def get_schema() -> str:
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SHOW TABLES")
        tables = cur.fetchall()
        schema = [table[0] for table in tables]
        return json.dumps(schema)
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        _safe_close(conn)
