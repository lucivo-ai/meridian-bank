#!/usr/bin/env python3
"""
Meridian Community Bank — Master Data Generation Orchestrator
Runs all generators in dependency order to build the complete test data environment.

Usage:
    python generate_all.py              # Full generation
    python generate_all.py --schema-only  # Create schemas only (no data)
    python generate_all.py --step N     # Run from step N onwards
"""
import sys
import os
import time
import argparse

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from generators.utils.relationships import get_engine, execute_sql_file
from sqlalchemy import text


def setup_database():
    """Create all schemas and tables from DDL files."""
    print("=" * 60)
    print("🏦 MERIDIAN COMMUNITY BANK — Data Environment Setup")
    print("=" * 60)

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    schemas_dir = os.path.join(base_dir, 'schemas')

    engine = get_engine()

    # Test connection
    try:
        with engine.connect() as conn:
            version = conn.execute(text("SELECT version()")).scalar()
            print(f"\n✓ Connected to PostgreSQL: {version[:50]}...")
    except Exception as e:
        print(f"\n✗ Database connection failed: {e}")
        print("  Make sure PostgreSQL is running (docker-compose up -d)")
        sys.exit(1)

    # Drop and recreate schemas for clean run
    print("\n🗑️  Dropping existing schemas...")
    with engine.connect() as conn:
        for schema in ['warehouse_reporting', 'warehouse_core', 'warehouse_staging',
                       'payments', 'treasury', 'gl', 'risk', 'crm', 'core_banking']:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        conn.commit()
    print("  ✓ Clean slate")

    # Execute DDL files in order
    print("\n📐 Creating schemas and tables...")
    ddl_order = [
        ('Source: Core Banking', 'source_systems/core_banking.sql'),
        ('Source: CRM', 'source_systems/crm.sql'),
        ('Source: Risk Engine', 'source_systems/risk_engine.sql'),
        ('Source: Payments', 'source_systems/payments.sql'),
        ('Source: Treasury', 'source_systems/treasury.sql'),
        ('Source: General Ledger', 'source_systems/gl.sql'),
        ('Warehouse: Staging', 'data_warehouse/staging.sql'),
        ('Warehouse: Core', 'data_warehouse/core.sql'),
        ('Warehouse: Reporting', 'data_warehouse/reporting.sql'),
    ]

    for label, path in ddl_order:
        full_path = os.path.join(schemas_dir, path)
        execute_sql_file(full_path)
        print(f"  ✓ {label}")

    print("\n✅ Database schema created successfully")


def run_generators(start_step=1):
    """Run data generators in dependency order."""
    steps = [
        (1, "Reference Data", "generators.generate_reference_data"),
        (2, "Customers & Addresses", "generators.generate_customers"),
        (3, "Accounts", "generators.generate_accounts"),
        (4, "Risk & Compliance", "generators.generate_risk_data"),
        (5, "Transactions", "generators.generate_transactions"),
        (6, "General Ledger", "generators.generate_gl_entries"),
        (7, "Treasury", "generators.generate_treasury"),
        (8, "CRM Data", "generators.generate_crm_data"),
        (9, "Warehouse (Staging → Core → Reporting)", "generators.generate_warehouse"),
    ]

    print("\n" + "=" * 60)
    print("📊 DATA GENERATION")
    print("=" * 60)

    total_start = time.time()

    for step_num, label, module_name in steps:
        if step_num < start_step:
            print(f"\n⏭️  Step {step_num}: {label} (skipped)")
            continue

        step_start = time.time()
        print(f"\n{'─' * 60}")
        print(f"Step {step_num}/{len(steps)}: {label}")
        print(f"{'─' * 60}")

        try:
            module = __import__(module_name, fromlist=['run'])
            module.run()
            elapsed = time.time() - step_start
            print(f"⏱️  Step {step_num} completed in {elapsed:.1f}s")
        except Exception as e:
            print(f"\n✗ Step {step_num} failed: {e}")
            import traceback
            traceback.print_exc()
            print("\nYou can resume from this step with: python generate_all.py --step", step_num)
            sys.exit(1)

    total_elapsed = time.time() - total_start
    print_summary(total_elapsed)


def print_summary(elapsed):
    """Print final summary of generated data."""
    engine = get_engine()

    print("\n" + "=" * 60)
    print("📋 GENERATION SUMMARY")
    print("=" * 60)

    schemas = {
        'core_banking': ['products', 'customers', 'addresses', 'accounts',
                         'transactions', 'standing_orders', 'direct_debits'],
        'crm': ['contacts', 'interactions', 'complaints', 'marketing_consents', 'segments'],
        'risk': ['credit_scores', 'credit_applications', 'aml_alerts', 'aml_cases',
                 'sanctions_screening', 'risk_assessments', 'regulatory_reports'],
        'payments': ['payment_schemes', 'payment_instructions', 'payment_receipts', 'failed_payments'],
        'treasury': ['positions', 'fx_rates', 'interbank_lending', 'liquidity_pool'],
        'gl': ['chart_of_accounts', 'cost_centres', 'gl_entries', 'gl_balances'],
        'warehouse_staging': ['stg_customers', 'stg_accounts', 'stg_transactions',
                              'stg_contacts', 'stg_interactions', 'stg_credit_scores',
                              'stg_aml_alerts', 'stg_risk_assessments', 'stg_gl_entries'],
        'warehouse_core': ['dim_date', 'dim_customer', 'dim_account', 'dim_product',
                          'dim_branch', 'dim_geography',
                          'fact_transactions', 'fact_gl_entries', 'bridge_customer_account'],
        'warehouse_reporting': ['rpt_customer_360', 'rpt_daily_pnl', 'rpt_liquidity_coverage',
                                'rpt_aml_summary', 'rpt_product_performance',
                                'rpt_regulatory_capital', 'rpt_arrears_ageing'],
    }

    total_rows = 0
    with engine.connect() as conn:
        for schema, tables in schemas.items():
            print(f"\n  {schema}:")
            for table in tables:
                try:
                    count = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table}")).scalar()
                    total_rows += count
                    print(f"    {table:.<40} {count:>12,}")
                except Exception:
                    print(f"    {table:.<40} {'(empty)':>12}")

    # Database size
    with engine.connect() as conn:
        size = conn.execute(text(
            "SELECT pg_size_pretty(pg_database_size('meridian_bank'))"
        )).scalar()

    print(f"\n{'─' * 60}")
    print(f"  Total rows: {total_rows:,}")
    print(f"  Database size: {size}")
    print(f"  Total time: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
    print(f"{'─' * 60}")
    print("\n✅ Meridian Community Bank test data environment is ready!")
    print("   Next steps:")
    print("   1. Generate metadata: python -m metadata.generate_metadata")
    print("   2. Run agent tests:   python -m agent.agent_runner")


def main():
    parser = argparse.ArgumentParser(description='Generate Meridian Bank test data')
    parser.add_argument('--schema-only', action='store_true', help='Create schemas only')
    parser.add_argument('--step', type=int, default=1, help='Start from step N')
    parser.add_argument('--no-schema', action='store_true', help='Skip schema creation')
    args = parser.parse_args()

    if not args.no_schema:
        setup_database()

    if not args.schema_only:
        run_generators(start_step=args.step)


if __name__ == '__main__':
    main()
