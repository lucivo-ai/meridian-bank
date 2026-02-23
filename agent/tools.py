"""
Meridian Community Bank — Agent Tool Definitions
Provides three core tools for the agentic AI to query the test data environment.
"""
import json
import os
import re
from pathlib import Path
from sqlalchemy import text, create_engine
from rdflib import Graph

# ── Configuration ─────────────────────────────────────────────

BASE_DIR = Path(__file__).parent.parent
METADATA_DIR = BASE_DIR / 'metadata' / 'datahub'
ONTOLOGY_DIR = BASE_DIR / 'ontology'

DB_URL = os.environ.get(
    'MERIDIAN_DB_URL',
    'postgresql://meridian:meridian_dev@localhost:5432/meridian_bank'
)

# ── Tool Definitions (for LLM) ───────────────────────────────

TOOL_DEFINITIONS = [
    {
        "name": "sql_query",
        "description": (
            "Execute a read-only SQL query against the Meridian Bank PostgreSQL database. "
            "Available schemas: core_banking, crm, risk, payments, treasury, gl, "
            "warehouse_staging, warehouse_core, warehouse_reporting. "
            "Use for retrieving actual data, counts, aggregations, and data quality checks."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "SQL SELECT statement. Must be read-only (SELECT only, no INSERT/UPDATE/DELETE)."
                }
            },
            "required": ["query"]
        }
    },
    {
        "name": "metadata_search",
        "description": (
            "Search the DataHub metadata catalog for Meridian Bank. "
            "Find datasets, column metadata, owners, tags (PII, Sensitive, Financial, Regulatory, Derived, Reference), "
            "glossary terms, data quality assertions (passing and failing), and lineage information. "
            "Use for discovering what data exists, who owns it, its quality status, and how data flows between systems."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "search_term": {
                    "type": "string",
                    "description": "Free text search across dataset names, descriptions, tags, glossary terms, and owners."
                },
                "filter_tags": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Filter by classification tags: PII, Sensitive, Financial, Regulatory, Derived, Reference"
                },
                "filter_owner": {
                    "type": "string",
                    "description": "Filter by data owner name"
                },
                "filter_domain": {
                    "type": "string",
                    "description": "Filter by domain: Retail Banking, CRM, Risk & Compliance, Payments, Treasury, Finance, Data Warehouse"
                },
                "include_lineage": {
                    "type": "boolean",
                    "description": "Include upstream/downstream lineage information"
                },
                "include_quality": {
                    "type": "boolean",
                    "description": "Include data quality assertion results"
                }
            },
            "required": ["search_term"]
        }
    },
    {
        "name": "ontology_query",
        "description": (
            "Query the Meridian Banking Ontology using SPARQL. "
            "Discover business concepts (Party, Account, Transaction, Product, RiskAssessment, RegulatoryReport), "
            "their relationships (hasAccount, hasProduct, hasTransaction, hasRiskAssessment, feedsReport), "
            "and mappings from business concepts to physical database tables. "
            "The ontology is aligned with FIBO (Financial Industry Business Ontology)."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "sparql": {
                    "type": "string",
                    "description": "SPARQL query. Prefixes available: meridian:, map:, lin:, rdfs:, owl:, skos:"
                }
            },
            "required": ["sparql"]
        }
    }
]


# ── Tool Implementations ─────────────────────────────────────

def execute_sql_query(query: str) -> dict:
    """Execute a read-only SQL query against the Meridian Bank database."""
    # Safety check
    query_upper = query.strip().upper()
    if not query_upper.startswith('SELECT') and not query_upper.startswith('WITH'):
        return {"error": "Only SELECT queries are permitted."}

    dangerous = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'TRUNCATE', 'CREATE']
    for keyword in dangerous:
        if re.search(rf'\b{keyword}\b', query_upper):
            return {"error": f"Query contains forbidden keyword: {keyword}"}

    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            result = conn.execute(text(query))
            columns = list(result.keys())
            rows = [dict(zip(columns, row)) for row in result.fetchmany(500)]
            total = len(rows)

            # Convert non-serializable types
            for row in rows:
                for k, v in row.items():
                    if hasattr(v, 'isoformat'):
                        row[k] = v.isoformat()
                    elif isinstance(v, (bytes, memoryview)):
                        row[k] = str(v)
                    elif v is not None and not isinstance(v, (str, int, float, bool, list)):
                        row[k] = str(v)

            return {
                "columns": columns,
                "rows": rows,
                "row_count": total,
                "truncated": total >= 500
            }
    except Exception as e:
        return {"error": str(e)}


def search_metadata(search_term: str, filter_tags: list = None,
                    filter_owner: str = None, filter_domain: str = None,
                    include_lineage: bool = False, include_quality: bool = False) -> dict:
    """Search the DataHub metadata catalog."""
    results = {
        "datasets": [],
        "glossary_matches": [],
        "lineage": [],
        "quality_assertions": []
    }

    search_lower = search_term.lower()

    # Search datasets
    with open(METADATA_DIR / 'datasets.json') as f:
        datasets = json.load(f)['datasets']

    for ds in datasets:
        score = 0
        if search_lower in ds['name'].lower():
            score += 10
        if search_lower in ds.get('description', '').lower():
            score += 5
        if any(search_lower in tag.lower() for tag in ds.get('tags', [])):
            score += 8

        if filter_tags:
            if not any(tag in ds.get('tags', []) for tag in filter_tags):
                continue
        if filter_domain and filter_domain.lower() != ds.get('domain', '').lower():
            continue
        if score > 0 or filter_tags:
            results['datasets'].append({**ds, '_relevance': score})

    # Filter by owner
    if filter_owner:
        with open(METADATA_DIR / 'ownership.json') as f:
            owners = json.load(f)['owners']
        owned_datasets = set()
        for owner in owners:
            if filter_owner.lower() in owner['name'].lower():
                for ds_name in owner['datasets']:
                    if ds_name == '*':
                        owned_datasets = {d['name'] for d in results['datasets']}
                        break
                    owned_datasets.add(ds_name)
        results['datasets'] = [d for d in results['datasets']
                               if d['name'] in owned_datasets or any(
                                   d['name'].startswith(p.replace('.*', '.'))
                                   for p in owned_datasets if '*' in p)]

    # Search glossary
    with open(METADATA_DIR / 'tags_and_glossary.json') as f:
        glossary_data = json.load(f)
    for term in glossary_data.get('glossary_terms', []):
        if search_lower in term['term'].lower() or search_lower in term['definition'].lower():
            results['glossary_matches'].append(term)

    # Include lineage
    if include_lineage:
        with open(METADATA_DIR / 'lineage.json') as f:
            lineage_data = json.load(f)
        for flow in lineage_data.get('lineage_edges', []):
            for edge in flow['edges']:
                if (search_lower in edge.get('upstream', '').lower() or
                    search_lower in edge.get('downstream', '').lower()):
                    results['lineage'].append({
                        'flow_name': flow['name'],
                        **edge
                    })
        # Include gaps
        for gap in lineage_data.get('undocumented_lineage_gaps', []):
            if search_lower in gap.get('dataset', '').lower() or search_lower in gap.get('note', '').lower():
                results['lineage'].append({'gap': gap})

    # Include quality
    if include_quality:
        with open(METADATA_DIR / 'data_quality.json') as f:
            dq_data = json.load(f)
        for assertion in dq_data.get('assertions', []):
            if (search_lower in assertion.get('dataset', '').lower() or
                search_lower in assertion.get('name', '').lower() or
                (assertion.get('status') == 'FAIL' and 'fail' in search_lower) or
                (assertion.get('status') == 'FAIL' and 'quality' in search_lower) or
                (assertion.get('status') == 'FAIL' and 'issue' in search_lower)):
                results['quality_assertions'].append(assertion)

    # Sort by relevance
    results['datasets'].sort(key=lambda x: x.get('_relevance', 0), reverse=True)

    # Add ownership info
    with open(METADATA_DIR / 'ownership.json') as f:
        owners = json.load(f)['owners']
    for ds in results['datasets']:
        ds_owners = []
        for owner in owners:
            if ds['name'] in owner.get('datasets', []) or '*' in owner.get('datasets', []):
                ds_owners.append(f"{owner['name']} ({owner['role']})")
            elif any(ds['name'].startswith(p.replace('.*', '.')) for p in owner.get('datasets', []) if '*' in p):
                ds_owners.append(f"{owner['name']} ({owner['role']})")
        ds['owners'] = ds_owners

    return results


def query_ontology(sparql: str) -> dict:
    """Execute a SPARQL query against the Meridian Banking Ontology."""
    try:
        g = Graph()
        g.parse(ONTOLOGY_DIR / 'meridian_banking.ttl', format='turtle')
        g.parse(ONTOLOGY_DIR / 'meridian_mappings.ttl', format='turtle')
        g.parse(ONTOLOGY_DIR / 'meridian_lineage.ttl', format='turtle')

        results = g.query(sparql)

        rows = []
        for row in results:
            row_dict = {}
            for i, var in enumerate(results.vars):
                val = row[i]
                row_dict[str(var)] = str(val) if val else None
            rows.append(row_dict)

        return {
            "variables": [str(v) for v in results.vars],
            "rows": rows,
            "row_count": len(rows)
        }
    except Exception as e:
        return {"error": str(e)}


# ── Tool Dispatcher ───────────────────────────────────────────

def dispatch_tool(tool_name: str, arguments: dict) -> dict:
    """Dispatch a tool call to the appropriate implementation."""
    if tool_name == 'sql_query':
        return execute_sql_query(arguments['query'])
    elif tool_name == 'metadata_search':
        return search_metadata(**arguments)
    elif tool_name == 'ontology_query':
        return query_ontology(arguments['sparql'])
    else:
        return {"error": f"Unknown tool: {tool_name}"}
