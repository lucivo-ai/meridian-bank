# Meridian Community Bank

A synthetic PostgreSQL 16 database with ~17.5M rows of realistic banking data, packaged as a Docker image with a built-in MCP server for AI agent access.

## Quick Start

Pull the pre-built image and run:

```bash
docker pull ghcr.io/lucivo-ai/meridian-bank:latest

docker run -d \
  --name meridian-bank \
  -p 5433:5432 \
  -p 8000:8000 \
  ghcr.io/lucivo-ai/meridian-bank:latest
```

The first boot restores a 593MB database dump, which takes a few minutes. Subsequent starts are instant.

## Connection Details

| Interface | Connection String |
|-----------|-------------------|
| Direct SQL | `postgresql://meridian:meridian_dev@localhost:5433/meridian_bank` |
| MCP (AI agents) | `http://localhost:8000/sse` |

**Credentials:** user `meridian`, password `meridian_dev`, database `meridian_bank`

## What's Inside

58 tables across 9 schemas containing approximately 17.5 million rows of interconnected banking data.

### Source Systems (6 schemas)

| Schema | System | Key Tables | Scale |
|--------|--------|------------|-------|
| `core_banking` | Core Banking | customers, accounts, transactions | ~50K customers, ~85K accounts, ~3M transactions |
| `crm` | CRM | contacts, interactions, complaints | Customer relationship records |
| `risk_engine` | Risk Engine | credit_scores, aml_alerts | Credit scoring and AML monitoring |
| `payments` | Payments | instructions, receipts | Payment processing records |
| `treasury` | Treasury | positions, fx_rates | Treasury positions and FX rate history |
| `general_ledger` | General Ledger | journal_entries, chart_of_accounts | Financial accounting records |

### Data Warehouse (3 schemas)

| Schema | Layer | Description |
|--------|-------|-------------|
| `staging` | Staging | Raw extracts from source systems |
| `warehouse` | Core Star Schema | Fact and dimension tables for analytics |
| `reporting` | Reporting Marts | Pre-aggregated views for dashboards |

## Architecture

```
┌──────────────────────────────────────────┐
│       meridian-bank Docker image         │
│                                          │
│  ┌──────────────┐   ┌────────────────┐   │
│  │ PostgreSQL 16 │   │  postgres-mcp  │   │
│  │  :5432        │◄──│  (HTTP :8000)  │   │
│  │  9 schemas    │   │  read-only     │   │
│  │  58 tables    │   └────────────────┘   │
│  │  17.5M rows   │                        │
│  └──────────────┘                         │
└──────────────────────────────────────────┘
```

The Docker image runs two processes via a custom entrypoint:

1. **PostgreSQL 16** serves the database on port 5432
2. **postgres-mcp** (v0.3.0) provides an MCP-compatible API over SSE on port 8000 in read-only mode

On first boot, `pg_restore` loads the database from a pre-baked dump file. A sentinel file prevents re-restoration on subsequent starts.

## MCP Tools

The built-in MCP server exposes these tools for AI agent interaction:

| Tool | Description |
|------|-------------|
| `list_schemas` | List all database schemas |
| `list_objects` | List tables, views, and other objects within a schema |
| `get_object_details` | Get column definitions, constraints, and indexes for a table |
| `execute_sql` | Run read-only SQL queries against the database |
| `explain_query` | Get the query execution plan for a SQL statement |
| `analyze_db_health` | Check table sizes, bloat, index usage, and other health metrics |

## Intentional Data Quality Issues

The dataset includes realistic data quality problems for testing discovery and governance tools:

| Issue | Description |
|-------|-------------|
| Missing postcodes | ~5% of customer addresses have NULL or empty postal codes |
| Zero-amount transactions | Several hundred transactions with `amount = 0.00` |
| Orphaned accounts | Accounts referencing customer IDs that no longer exist |
| GL imbalance | General ledger entries where debits and credits do not balance |
| Stale staging tables | Staging tables with `loaded_at` timestamps months behind source |
| Undocumented lineage | Warehouse tables with no recorded source-to-target mapping |

## Building Locally

To build the image from source, you need Hetzner S3 credentials to fetch the database dump:

```bash
export HETZNER_S3_ACCESS_KEY=your-access-key
export HETZNER_S3_SECRET_KEY=your-secret-key

docker compose up --build
```

The database will be available on `localhost:5433` and the MCP server on `localhost:8000`.

## Data Regeneration

The `generators/` directory contains Python scripts used to produce the synthetic dataset. To regenerate from scratch:

1. Review the generator scripts in `generators/`
2. Run the generators against a clean PostgreSQL instance
3. Export with `pg_dump --format=custom`
4. Upload the dump to S3

See individual generator scripts for configuration options and dependencies.

## Integration with DMP

To add Meridian Bank as a data source alongside [DMP](https://github.com/lucivo-ai/lucivo-dmp), add the following to DMP's `docker-compose.yml`:

```yaml
services:
  meridian-bank:
    image: ghcr.io/lucivo-ai/meridian-bank:latest
    container_name: meridian-bank
    ports:
      - "5433:5432"
      - "8000:8000"
    volumes:
      - meridian-pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U meridian -d meridian_bank"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s

volumes:
  meridian-pgdata:
```

Then configure DMP to connect to `postgresql://meridian:meridian_dev@meridian-bank:5432/meridian_bank` as an ingestion source.
