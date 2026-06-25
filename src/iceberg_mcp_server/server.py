## Copyright (c) 2025 Cloudera, Inc. All Rights Reserved.
##
## This file is licensed under the Apache License Version 2.0 (the "License").
## You may not use this file except in compliance with the License.
## You may obtain a copy of the License at http:##www.apache.org/licenses/LICENSE-2.0.
##
## This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
## OF ANY KIND, either express or implied. Refer to the License for the specific
## permissions and limitations governing your use of the file.

import os
import sys
from fastmcp import FastMCP
from dotenv import load_dotenv

load_dotenv()

from iceberg_mcp_server.tools import impala_tools
from iceberg_mcp_server.tools import iceberg_semantics

mcp = FastMCP(name="Cloudera Iceberg MCP Server via Impala")


# Register functions as MCP tools
@mcp.tool()
def execute_query(query: str) -> str:
    """
    Execute a SQL query on the Impala database and return results as JSON.
    """
    return impala_tools.execute_query(query)


@mcp.tool()
def get_schema() -> str:
    """
    Retrieve the list of table names in the current Impala database.
    """
    return impala_tools.get_schema()


@mcp.tool()
def get_table_health(table: str) -> str:
    """
    Assess Iceberg table health using metadata tables (snapshots, history, files,
    partitions, manifests, metadata_log_entries). Returns structured JSON summaries,
    recent activity, and health signals. Pass the table as name or database.table
    (e.g. flights or airlines_iceberg.flights).
    """
    return impala_tools.get_table_health(table)


@mcp.tool()
def list_metadata_tables(table: str) -> str:
    """
    List Iceberg metadata tables available for a table (SHOW METADATA TABLES).
    Pass table or database.table (e.g. flights or airlines_iceberg.flights).
    """
    return iceberg_semantics.list_metadata_tables(table)


@mcp.tool()
def describe_metadata_table(table: str, metadata_name: str) -> str:
    """
    Describe the schema of an Iceberg metadata table (e.g. snapshots, files, refs).
    """
    return iceberg_semantics.describe_metadata_table(table, metadata_name)


@mcp.tool()
def query_metadata_table(
    table: str,
    metadata_name: str,
    limit: int = 100,
    columns: str | None = None,
) -> str:
    """
    Query an Iceberg metadata table with a safe row limit. Optional comma-separated columns.
    """
    return iceberg_semantics.query_metadata_table(table, metadata_name, limit, columns)


@mcp.tool()
def list_snapshots(table: str, limit: int = 50) -> str:
    """
    List Iceberg snapshots for a table ordered by committed_at (most recent first).
    """
    return iceberg_semantics.list_snapshots(table, limit)


@mcp.tool()
def describe_table_history(table: str) -> str:
    """
    List snapshot history via Impala DESCRIBE HISTORY for an Iceberg table.
    """
    return iceberg_semantics.describe_table_history(table)


@mcp.tool()
def get_snapshot_summary(table: str, snapshot_id: str) -> str:
    """
    Get metadata for a single Iceberg snapshot ID including parsed summary stats.
    """
    return iceberg_semantics.get_snapshot_summary(table, snapshot_id)


@mcp.tool()
def list_refs(table: str) -> str:
    """
    List Iceberg branches and tags from the refs metadata table.
    """
    return iceberg_semantics.list_refs(table)


@mcp.tool()
def query_at_snapshot(
    table: str,
    snapshot_id: str,
    limit: int = 100,
    columns: str | None = None,
) -> str:
    """
    Time-travel read using FOR SYSTEM_VERSION AS OF snapshot_id. Optional column list and limit.
    """
    return iceberg_semantics.query_at_snapshot(table, snapshot_id, limit, columns)


@mcp.tool()
def query_at_timestamp(
    table: str,
    timestamp: str,
    limit: int = 100,
    columns: str | None = None,
) -> str:
    """
    Time-travel read using FOR SYSTEM_TIME AS OF timestamp (e.g. '2024-07-18 10:12:20').
    """
    return iceberg_semantics.query_at_timestamp(table, timestamp, limit, columns)


@mcp.tool()
def diff_snapshots(table: str, snapshot_id_a: str, snapshot_id_b: str) -> str:
    """
    Compare two Iceberg snapshots by ID and return metadata and summary diffs.
    """
    return iceberg_semantics.diff_snapshots(table, snapshot_id_a, snapshot_id_b)


def main():
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    print(f"Starting Iceberg MCP Server via transport: {transport}", file=sys.stderr)
    mcp.run(transport=transport, show_banner=False)

if __name__ == "__main__":
    main()
