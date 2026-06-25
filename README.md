# Iceberg MCP Server (via Impala)

Fork of [cloudera/iceberg-mcp-server](https://github.com/cloudera/iceberg-mcp-server) with Iceberg **table health** tooling and Impala reliability improvements.

Model Context Protocol server for read-only access to Iceberg tables through Apache Impala: schema discovery, SQL queries, and metadata-based health checks.

**Maintained fork:** `https://github.com/dipankarmazumdar/iceberg-mcp-server`

## Tools

- `execute_query(query: str)`: Run a read-only SQL query on Impala and return results as JSON.
- `get_schema()`: List all tables in the current database.
- `get_table_health(table: str)`: Summarize Iceberg table health from metadata tables (`snapshots`, `history`, `files`, `partitions`, `manifests`, `metadata_log_entries`). Pass `table` or `database.table` (for example `flights` or `airlines_iceberg.flights`).

### Iceberg semantics

- `list_metadata_tables(table)`: List metadata tables (`snapshots`, `files`, `refs`, etc.).
- `describe_metadata_table(table, metadata_name)`: Schema of a metadata table.
- `query_metadata_table(table, metadata_name, limit?, columns?)`: Bounded metadata query.
- `list_snapshots(table, limit?)`: Snapshot timeline from metadata.
- `describe_table_history(table)`: Snapshot history via `DESCRIBE HISTORY`.
- `get_snapshot_summary(table, snapshot_id)`: Detail for one snapshot.
- `list_refs(table)`: Branches and tags.
- `query_at_snapshot(table, snapshot_id, limit?, columns?)`: Time travel by snapshot ID.
- `query_at_timestamp(table, timestamp, limit?, columns?)`: Time travel by timestamp.
- `diff_snapshots(table, snapshot_id_a, snapshot_id_b)`: Compare two snapshots.

## Local development

```bash
python3.13 -m venv .venv
source .venv/bin/activate
pip install -e .
cp .env.example .env   # then edit with your Impala settings
```

Quick connectivity test:

```bash
python -c "from iceberg_mcp_server.tools import impala_tools; print(impala_tools.get_schema())"
```

Run with the MCP Inspector (use `fastmcp`, not `mcp dev`):

```bash
fastmcp dev inspector src/iceberg_mcp_server/server.py:mcp --with-editable .
```

## Usage with Cursor

Open this repository in Cursor. Project MCP config lives in `.cursor/mcp.json` (or copy from `mcp.json.example`).

Minimal config:

```json
{
  "mcpServers": {
    "iceberg-mcp-server": {
      "command": "/path/to/iceberg-mcp-server/.venv/bin/python",
      "args": [
        "/path/to/iceberg-mcp-server/src/iceberg_mcp_server/server.py"
      ]
    }
  }
}
```

Use **Agent** mode in chat. Credentials can live in `.env` (loaded by the server) or in the `env` block of the MCP config.

Enable the server under **Cursor Settings → MCP**.

## Usage with Claude Desktop

Add to the `mcpServers` section of your `claude_desktop_config.json`.

### Option 1: Install from this fork (recommended)

```json
{
  "mcpServers": {
    "iceberg-mcp-server": {
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/dipankarmazumdar/iceberg-mcp-server@main",
        "run-server"
      ],
      "env": {
        "IMPALA_HOST": "coordinator-default-impala.example.com",
        "IMPALA_PORT": "443",
        "IMPALA_USER": "username",
        "IMPALA_PASSWORD": "password",
        "IMPALA_DATABASE": "default"
      }
    }
  }
}
```

### Option 2: Local installation (after cloning this repository)

```json
{
  "mcpServers": {
    "iceberg-mcp-server": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/iceberg-mcp-server",
        "run",
        "src/iceberg_mcp_server/server.py"
      ],
      "env": {
        "IMPALA_HOST": "coordinator-default-impala.example.com",
        "IMPALA_PORT": "443",
        "IMPALA_USER": "username",
        "IMPALA_PASSWORD": "password",
        "IMPALA_DATABASE": "default"
      }
    }
  }
}
```

For Option 2, replace `/path/to` with your clone path.

### Upstream (Cloudera, without fork features)

The original project is [cloudera/iceberg-mcp-server](https://github.com/cloudera/iceberg-mcp-server). It does not include `get_table_health` or the Impala metadata fixes in this fork.

## Usage with AI frameworks

The `./examples` folder contains examples for LangChain/LangGraph and OpenAI SDK.

### Transport

Configure via `MCP_TRANSPORT`:

- `stdio` **(default)** — Claude Desktop, Cursor, Inspector
- `http` — network HTTP deployment
- `sse` — Server-Sent Events

---

Based on [cloudera/iceberg-mcp-server](https://github.com/cloudera/iceberg-mcp-server). See `LICENSE` and `NOTICE.txt` for attribution.

*Copyright (c) 2025 Cloudera, Inc. All rights reserved.*
