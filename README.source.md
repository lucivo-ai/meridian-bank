# Meridian Community Bank — Synthetic Test Data Environment

> **Purpose**: Complete synthetic data environment for testing the **Lucivo Data Management Platform (DMP)** — metadata catalog, ontology, lineage, and agentic AI workflows.

## Quick Start

```bash
# 1. Start PostgreSQL
docker-compose up -d

# 2. Install Python dependencies
pip install -r generators/requirements.txt

# 3. Generate everything
python -m generators.generate_all

# 4. Run validation tests
python -m tests.test_referential_integrity
python -m tests.test_data_volumes

# 5. Run agent test scenarios
python -m agent.agent_runner
```

## What's Inside

### The Bank
**Meridian Community Bank** is a fictional UK-based small bank with ~50,000 customers, offering retail banking, SME lending, and basic treasury functions.

### Data Scale
| Layer | Tables | Total Rows |
|-------|--------|-----------|
| 6 Source Systems | 32 | ~5M |
| Staging | 9 | ~3.3M |
| Core (Star Schema) | 10 | ~3.2M |
| Reporting Marts | 7 | ~60K |
| **Total** | **58** | **~11.5M** |

### Source Systems

| System | Schema | Key Tables |
|--------|--------|-----------|
| Core Banking | `core_banking` | customers (50K), accounts (85K), transactions (3M) |
| CRM | `crm` | contacts, interactions (200K), complaints, GDPR consents |
| Risk Engine | `risk` | credit_scores, AML alerts (5K), sanctions screening |
| Payments | `payments` | instructions (500K), receipts (500K), failed payments |
| Treasury | `treasury` | positions, FX rates, liquidity pool |
| General Ledger | `gl` | journal entries (1M), chart of accounts |

### Intentional Data Quality Issues
These are embedded for testing governance and DQ discovery:

| Issue | Location | Detail |
|-------|----------|--------|
| Missing postcodes | `core_banking.addresses` | 500 NULL postcodes |
| Zero-amount transactions | `core_banking.transactions` | 100 transactions with amount = 0 |
| Orphaned accounts | `core_banking.accounts` | 15 accounts with no matching customer |
| GL imbalance | `gl.gl_entries` | Batch BATCH-2024-ERR-001 has £500 discrepancy |
| Stale staging tables | `warehouse_staging` | 3 tables last refreshed 12 days ago |
| Undocumented lineage | `warehouse_reporting` | 2 reports with no upstream lineage |

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Agent Layer (LLM)                     │
│  scenarios.yaml │ tools.py │ agent_runner.py             │
└────────┬────────────────┬────────────────┬──────────────┘
         │                │                │
    ┌────▼────┐    ┌──────▼──────┐   ┌────▼────┐
    │SQL Query│    │  Metadata   │   │Ontology │
    │  Tool   │    │  Search     │   │  Query  │
    └────┬────┘    └──────┬──────┘   └────┬────┘
         │                │                │
    ┌────▼────────────────▼────┐    ┌─────▼──────────┐
    │     PostgreSQL 16        │    │  RDF/Turtle    │
    │  ┌─────────────────────┐ │    │  Ontology      │
    │  │ Source Systems (×6)  │ │    │  + Mappings    │
    │  │ Warehouse (3 layers)│ │    │  + Lineage     │
    │  └─────────────────────┘ │    └────────────────┘
    └──────────────────────────┘
              │
    ┌─────────▼──────────────────┐
    │  DataHub Metadata (JSON)   │
    │  datasets │ lineage │ DQ   │
    │  ownership │ tags │ glossary│
    └────────────────────────────┘
```

## Directory Structure

```
lucivo-test-data/
├── docker-compose.yml
├── config/db_config.yaml
├── schemas/
│   ├── source_systems/     # 6 DDL files
│   └── data_warehouse/     # staging, core, reporting DDL
├── generators/
│   ├── generate_all.py     # Master orchestrator
│   ├── generate_*.py       # 9 data generators
│   ├── config.py           # Generation parameters
│   └── utils/              # Shared utilities
├── metadata/
│   └── datahub/            # 6 JSON metadata files
├── ontology/
│   ├── meridian_banking.ttl    # Core ontology (OWL/RDFS)
│   ├── meridian_mappings.ttl   # Concept → table mappings
│   ├── meridian_lineage.ttl    # Lineage as RDF
│   └── vocabularies/           # FIBO subset, DCAT profile
├── agent/
│   ├── scenarios.yaml      # 20+ test scenarios
│   ├── tools.py            # Tool implementations
│   └── agent_runner.py     # Test execution engine
└── tests/                  # Validation test suite
```

## Agent Test Scenarios

20+ scenarios across 5 categories:

| Category | Examples |
|----------|---------|
| **A: Metadata Discovery** | "What datasets contain PII?", "Who owns AML data?" |
| **B: Data Quality** | "Any stale tables?", "Check GL balance integrity" |
| **C: Ontology Queries** | "Where is 'Customer' stored?", "Map 'Transaction' to physical tables" |
| **D: Cross-Layer** | "High-risk customers with loan balances?", "Compare Q3 vs Q4 AML alerts" |
| **E: Impact Analysis** | "What breaks if we change customers schema?", "Find undocumented lineage" |

## Metadata Org Chart

| Role | Name | Domain |
|------|------|--------|
| Chief Data Officer | Sarah Mitchell | All |
| Head of Retail Data | James Chen | Core Banking, CRM |
| Head of Risk Data | Priya Sharma | Risk & Compliance |
| Head of Finance Data | David Okonkwo | GL, Treasury |
| Payments Data Lead | Emma Williams | Payments |
| DWH Lead | Tom Andersen | Warehouse |
| Data Quality Lead | Fatima Al-Rashid | Cross-cutting |

## Pre-Built Test Data (Recommended)

A pre-built database dump with all 17.5M rows is available on Hetzner S3. This is much faster than generating from scratch.

### Prerequisites

- Docker Desktop running
- DMP stack running (`cd infra/docker && docker-compose up -d`)
- DataHub running (`datahub docker quickstart`)
- Python 3.10+
- AWS CLI configured for Hetzner S3

### Step 1: Download from S3

```bash
aws s3 sync s3://lucivo-bucket/meridian-bank-testdata/ ./meridian-bank-testdata/
```

### Step 2: Start the Test Database

```bash
docker run -d --name meridian_bank_db -p 5433:5432 \
  -e POSTGRES_DB=meridian_bank \
  -e POSTGRES_USER=meridian \
  -e POSTGRES_PASSWORD=meridian_dev \
  postgres:16-alpine

# Wait for it to be ready
until docker exec meridian_bank_db pg_isready -U meridian -d meridian_bank; do sleep 1; done

# Restore the dump (~5 minutes)
docker exec -i meridian_bank_db pg_restore \
  -U meridian -d meridian_bank --no-owner --no-privileges \
  < meridian-bank-testdata/meridian_bank.dump
```

### Step 3: Ingest Schema into DataHub

```bash
pip install 'acryl-datahub[postgres]'

# Ingest table schemas and tags
datahub ingest -c metadata/datahub_ingestion_recipe.yaml
```

This creates 58 datasets across 9 schemas with classification tags.

### Step 4: Ingest Ownership, Lineage & Glossary into DataHub

```bash
pip install acryl-datahub

python3 << 'SCRIPT'
import json
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    CorpUserInfoClass, OwnershipClass, OwnerClass,
    OwnershipTypeClass, UpstreamLineageClass, UpstreamClass,
    DatasetLineageTypeClass, GlossaryTermInfoClass,
    GlossaryNodeInfoClass,
)

emitter = DatahubRestEmitter("http://localhost:8080")

# --- Load metadata files ---
with open("metadata/datahub/ownership.json") as f:
    ownership_data = json.load(f)
with open("metadata/datahub/lineage.json") as f:
    lineage_data = json.load(f)
with open("metadata/datahub/tags_and_glossary.json") as f:
    glossary_data = json.load(f)

# --- 4a. Create CorpUser entities ---
for owner in ownership_data["owners"]:
    email = owner["email"]
    urn = f"urn:li:corpuser:{email}"
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=CorpUserInfoClass(
            active=True,
            displayName=owner["name"],
            fullName=owner["name"],
            title=owner["title"],
            email=email,
        ),
    ))
    print(f"  Created user: {owner['name']}")

# --- 4b. Apply ownership to datasets ---
PLATFORM = "urn:li:dataPlatform:postgres"

for owner in ownership_data["owners"]:
    for dataset_pattern in owner["datasets"]:
        dataset_urn = f"urn:li:dataset:({PLATFORM},meridian_bank.{dataset_pattern},PROD)"

        # Find all owners for this dataset
        dataset_owners = []
        for o in ownership_data["owners"]:
            for dp in o["datasets"]:
                if dp == dataset_pattern:
                    dataset_owners.append(OwnerClass(
                        owner=f"urn:li:corpuser:{o['email']}",
                        type=OwnershipTypeClass.DATA_STEWARD,
                        source={"type": "MANUAL"},
                    ))

        if dataset_owners:
            emitter.emit(MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=OwnershipClass(owners=dataset_owners),
            ))

print("  Ownership applied")

# --- 4c. Create lineage ---
downstream_map = {}
for flow in lineage_data["flows"]:
    for edge in flow["edges"]:
        ds = f"urn:li:dataset:({PLATFORM},meridian_bank.{edge['downstream']},PROD)"
        us = f"urn:li:dataset:({PLATFORM},meridian_bank.{edge['upstream']},PROD)"
        downstream_map.setdefault(ds, []).append(us)

for downstream_urn, upstream_urns in downstream_map.items():
    upstreams = [UpstreamClass(dataset=u, type=DatasetLineageTypeClass.TRANSFORMED) for u in upstream_urns]
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=downstream_urn,
        aspect=UpstreamLineageClass(upstreams=upstreams),
    ))

print(f"  Lineage created for {len(downstream_map)} datasets")

# --- 4d. Create glossary terms ---
node_urn = "urn:li:glossaryNode:meridian_banking"
emitter.emit(MetadataChangeProposalWrapper(
    entityUrn=node_urn,
    aspect=GlossaryNodeInfoClass(
        definition="Banking domain glossary for Meridian Community Bank",
        name="Meridian Banking",
    ),
))

for term in glossary_data["glossary_terms"]:
    term_urn = f"urn:li:glossaryTerm:{term['id']}"
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=term_urn,
        aspect=GlossaryTermInfoClass(
            definition=term["definition"],
            name=term["name"],
            parentNode=node_urn,
        ),
    ))

print(f"  Created {len(glossary_data['glossary_terms'])} glossary terms")
print("Done!")
SCRIPT
```

### Step 5: Import Ontology into Lucivo-DMP

The DMP API must be running on `http://localhost:8000`.

```bash
# Preview the ontology import
curl -s -X POST http://localhost:8000/api/v1/ontology-import/preview \
  -F "file=@ontology/meridian_banking.ttl" \
  -F "format=turtle" | python3 -m json.tool

# Commit the core ontology (52 terms, 71 relationships)
curl -s -X POST http://localhost:8000/api/v1/ontology-import/commit \
  -F "file=@ontology/meridian_banking.ttl" \
  -F "format=turtle" | python3 -m json.tool

# Import supporting ontology files
for ttl in meridian_mappings.ttl meridian_lineage.ttl; do
  curl -s -X POST http://localhost:8000/api/v1/ontology-import/commit \
    -F "file=@ontology/$ttl" \
    -F "format=turtle" | python3 -m json.tool
done

# FIBO alignment
curl -s -X POST http://localhost:8000/api/v1/ontology-import/commit \
  -F "file=@ontology/vocabularies/fibo_subset.ttl" \
  -F "format=turtle" | python3 -m json.tool
```

### Step 6: Sync DataHub Assets into DMP

```bash
python3 << 'SCRIPT'
import requests

DATAHUB = "http://localhost:8080/api/graphql"
DMP = "http://localhost:8000/api/v1/assets"

query = """{ search(input: { type: DATASET, query: "meridian_bank", start: 0, count: 100 }) {
  searchResults { entity { urn ... on Dataset {
    name properties { name description }
    schemaMetadata { fields { fieldPath nativeDataType } }
    tags { tags { tag { urn properties { name } } } }
  }}}
}}"""

resp = requests.post(DATAHUB, json={"query": query}).json()
datasets = resp["data"]["search"]["searchResults"]

for ds in datasets:
    entity = ds["entity"]
    props = entity.get("properties") or {}
    schema = entity.get("schemaMetadata") or {}
    tags_data = entity.get("tags") or {}

    fields = [{"name": f["fieldPath"], "type": f.get("nativeDataType", "")}
              for f in schema.get("fields", [])]
    tags = [t["tag"]["properties"]["name"]
            for t in tags_data.get("tags", []) if t.get("tag", {}).get("properties")]

    asset = {
        "name": props.get("name", entity["name"]),
        "description": props.get("description", ""),
        "platform": "postgres",
        "qualified_name": entity["name"],
        "asset_type": "dataset",
        "schema_fields": fields,
        "tags": tags,
        "external_url": entity["urn"],
    }
    r = requests.post(DMP, json=asset)
    status = "ok" if r.status_code in (200, 201) else f"err {r.status_code}"
    print(f"  {entity['name']}: {status}")

print(f"Done! {len(datasets)} assets synced to DMP")
SCRIPT
```

### Verification

```bash
# Check database
docker exec meridian_bank_db psql -U meridian -d meridian_bank \
  -c "SELECT count(*) FROM core_banking.customers;"
# Expected: 50000

# Check DataHub (58 datasets)
curl -s -X POST http://localhost:8080/api/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query": "{ search(input: { type: DATASET, query: \"meridian_bank\", start: 0, count: 1 }) { total } }"}' \
  | python3 -c "import sys,json; print(f'DataHub datasets: {json.load(sys.stdin)[\"data\"][\"search\"][\"total\"]}')"

# Check DMP ontology
curl -s http://localhost:8000/api/v1/glossary/terms?limit=5 | python3 -m json.tool
```

### What's Included

| Component | Details |
|-----------|---------|
| **Database** | 17.5M rows, 58 tables, 9 schemas |
| **DataHub metadata** | Schema + tags, ownership (7 users), lineage (7 flows, 23 edges), glossary (15 terms) |
| **Ontology** | 33 OWL classes, 11 object properties, 8 datatype properties, FIBO alignment |
| **Intentional gaps** | `rpt_product_performance` and `rpt_arrears_ageing` have undocumented lineage (for agent testing) |

## Generating from Scratch

If you prefer to generate data from scratch instead of using the pre-built dump:

## Requirements

- Docker & Docker Compose
- Python 3.10+
- ~2-5 GB disk space for database
- Generation time: ~10-20 minutes

## License

This is synthetic test data created for Lucivo.ai DMP demonstrations. All data is fictitious.
