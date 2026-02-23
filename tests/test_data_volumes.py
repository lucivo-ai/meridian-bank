"""
Test that expected data volumes are met across all tables.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from generators.utils.relationships import get_engine


def test_all():
    engine = get_engine()
    results = []

    # (schema.table, min_expected, max_expected)
    volume_checks = [
        # Source systems
        ("core_banking.products", 20, 25),
        ("core_banking.customers", 49000, 51000),
        ("core_banking.addresses", 50000, 80000),
        ("core_banking.accounts", 75000, 95000),
        ("core_banking.transactions", 2500000, 3500000),
        ("core_banking.standing_orders", 20000, 60000),
        ("core_banking.direct_debits", 30000, 80000),
        ("crm.contacts", 49000, 51000),
        ("crm.interactions", 150000, 250000),
        ("crm.complaints", 1500, 3000),
        ("crm.marketing_consents", 200000, 400000),
        ("crm.segments", 49000, 51000),
        ("risk.credit_scores", 49000, 51000),
        ("risk.credit_applications", 10000, 20000),
        ("risk.aml_alerts", 3000, 7000),
        ("risk.aml_cases", 30, 800),
        ("risk.sanctions_screening", 49000, 51000),
        ("risk.risk_assessments", 49000, 51000),
        ("risk.regulatory_reports", 50, 300),
        ("payments.payment_schemes", 8, 12),
        ("payments.payment_instructions", 200000, 700000),
        ("payments.payment_receipts", 200000, 700000),
        ("payments.failed_payments", 5000, 25000),
        ("treasury.positions", 100, 1000),
        ("treasury.fx_rates", 500, 10000),
        ("treasury.interbank_lending", 50, 500),
        ("treasury.liquidity_pool", 20, 200),
        ("gl.chart_of_accounts", 50, 70),
        ("gl.cost_centres", 15, 25),
        ("gl.gl_entries", 500000, 1500000),
        ("gl.gl_balances", 100, 10000),

        # Warehouse - staging
        ("warehouse_staging.stg_customers", 49000, 51000),
        ("warehouse_staging.stg_accounts", 75000, 95000),
        ("warehouse_staging.stg_transactions", 2500000, 3500000),

        # Warehouse - core
        ("warehouse_core.dim_date", 2000, 3000),
        ("warehouse_core.dim_customer", 49000, 51000),
        ("warehouse_core.dim_account", 75000, 95000),
        ("warehouse_core.dim_product", 20, 25),
        ("warehouse_core.fact_transactions", 2000000, 3500000),
        ("warehouse_core.fact_gl_entries", 500000, 1500000),

        # Warehouse - reporting
        ("warehouse_reporting.rpt_customer_360", 49000, 51000),
        ("warehouse_reporting.rpt_daily_pnl", 100, 50000),
        ("warehouse_reporting.rpt_aml_summary", 3, 12),
        ("warehouse_reporting.rpt_liquidity_coverage", 3, 12),
        ("warehouse_reporting.rpt_regulatory_capital", 3, 12),
    ]

    with engine.connect() as conn:
        for table, min_exp, max_exp in volume_checks:
            try:
                actual = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                passed = min_exp <= actual <= max_exp
                status = "✅" if passed else "❌"
                results.append((table, passed, actual, min_exp, max_exp))
                print(f"  {status} {table:.<50} {actual:>10,}  (expected {min_exp:,}-{max_exp:,})")
            except Exception as e:
                results.append((table, False, 0, min_exp, max_exp))
                print(f"  ❌ {table:.<50} ERROR: {e}")

    passed_count = sum(1 for _, p, *_ in results if p)
    print(f"\n  {passed_count}/{len(results)} volume checks passed")
    return all(p for _, p, *_ in results)


if __name__ == '__main__':
    print("\n📊 Data Volume Tests\n")
    success = test_all()
    sys.exit(0 if success else 1)
