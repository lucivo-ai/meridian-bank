"""
Test referential integrity across the Meridian Bank data environment.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from generators.utils.relationships import get_engine


def test_all():
    engine = get_engine()
    results = []

    checks = [
        ("Accounts → Customers (with 15 expected orphans)",
         "SELECT COUNT(*) FROM core_banking.accounts a LEFT JOIN core_banking.customers c ON a.customer_id = c.customer_id WHERE c.customer_id IS NULL",
         15, "eq"),

        ("Transactions → Accounts",
         "SELECT COUNT(*) FROM core_banking.transactions t LEFT JOIN core_banking.accounts a ON t.account_id = a.account_id WHERE a.account_id IS NULL",
         0, "eq"),

        ("Addresses → Customers",
         "SELECT COUNT(*) FROM core_banking.addresses a LEFT JOIN core_banking.customers c ON a.customer_id = c.customer_id WHERE c.customer_id IS NULL",
         0, "eq"),

        ("CRM Contacts → Customers",
         "SELECT COUNT(*) FROM crm.contacts c LEFT JOIN core_banking.customers cu ON c.customer_id = cu.customer_id WHERE cu.customer_id IS NULL",
         0, "eq"),

        ("CRM Interactions → Contacts",
         "SELECT COUNT(*) FROM crm.interactions i LEFT JOIN crm.contacts c ON i.contact_id = c.contact_id WHERE c.contact_id IS NULL",
         0, "eq"),

        ("Credit Scores → Customers",
         "SELECT COUNT(*) FROM risk.credit_scores s LEFT JOIN core_banking.customers c ON s.customer_id = c.customer_id WHERE c.customer_id IS NULL",
         0, "eq"),

        ("AML Alerts → Customers",
         "SELECT COUNT(*) FROM risk.aml_alerts a LEFT JOIN core_banking.customers c ON a.customer_id = c.customer_id WHERE c.customer_id IS NULL",
         0, "eq"),

        ("GL Entries balance (expect 1 imbalanced batch)",
         "SELECT COUNT(*) FROM (SELECT batch_id, ABS(SUM(debit_amount) - SUM(credit_amount)) as diff FROM gl.gl_entries GROUP BY batch_id HAVING ABS(SUM(debit_amount) - SUM(credit_amount)) > 0.01) sub",
         1, "eq"),

        ("Fact Transactions → Dim Customer",
         "SELECT COUNT(*) FROM warehouse_core.fact_transactions ft LEFT JOIN warehouse_core.dim_customer dc ON ft.customer_key = dc.customer_key WHERE dc.customer_key IS NULL",
         0, "eq"),

        ("Fact Transactions → Dim Account",
         "SELECT COUNT(*) FROM warehouse_core.fact_transactions ft LEFT JOIN warehouse_core.dim_account da ON ft.account_key = da.account_key WHERE da.account_key IS NULL",
         0, "eq"),
    ]

    with engine.connect() as conn:
        for name, sql, expected, op in checks:
            actual = conn.execute(text(sql)).scalar()
            if op == "eq":
                passed = actual == expected
            elif op == "lte":
                passed = actual <= expected
            else:
                passed = actual == expected

            status = "✅" if passed else "❌"
            results.append((name, passed, actual, expected))
            print(f"  {status} {name}: got {actual}, expected {expected}")

    passed_count = sum(1 for _, p, _, _ in results if p)
    print(f"\n  {passed_count}/{len(results)} checks passed")
    return all(p for _, p, _, _ in results)


if __name__ == '__main__':
    print("\n🔗 Referential Integrity Tests\n")
    success = test_all()
    sys.exit(0 if success else 1)
