"""
Generate reference/lookup data:
- Products, Chart of Accounts, Cost Centres, Payment Schemes, Branches, Date dimension
"""
import numpy as np
from datetime import date, timedelta
from generators.config import (
    PRODUCTS, CHART_OF_ACCOUNTS, COST_CENTRES, PAYMENT_SCHEMES, BRANCHES, SEED
)
from generators.utils.relationships import bulk_insert, registry, get_engine
from sqlalchemy import text


def generate_products():
    """Insert product catalogue."""
    records = []
    for i, (code, name, cat, rate, cur, min_bal, launched) in enumerate(PRODUCTS, 1):
        records.append({
            'product_code': code,
            'name': name,
            'category': cat,
            'interest_rate': rate,
            'currency': cur,
            'min_balance': min_bal,
            'is_active': True,
            'launched_date': launched,
            'description': f'{name} — Meridian Community Bank',
        })
    bulk_insert('core_banking.products', records,
                ['product_code', 'name', 'category', 'interest_rate', 'currency',
                 'min_balance', 'is_active', 'launched_date', 'description'])

    # Retrieve generated product_ids and register
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT product_id, product_code, category FROM core_banking.products")).fetchall()
    product_ids = [r[0] for r in rows]
    registry.register('product_ids', product_ids)

    # Also store by category for account generation
    by_cat = {}
    for pid, code, cat in rows:
        by_cat.setdefault(cat, []).append(pid)
    registry.register('products_by_category', [by_cat])  # Store as single-element list of dict

    print(f"  ✓ Products: {len(records)} inserted")
    return product_ids


def generate_chart_of_accounts():
    """Insert chart of accounts."""
    records = []
    for code, name, atype, subtype, parent, level in CHART_OF_ACCOUNTS:
        records.append({
            'account_code': code,
            'account_name': name,
            'account_type': atype,
            'account_subtype': subtype,
            'parent_code': parent,
            'hierarchy_level': level,
            'is_posting_account': level >= 2,
            'is_active': True,
        })
    bulk_insert('gl.chart_of_accounts', records,
                ['account_code', 'account_name', 'account_type', 'account_subtype',
                 'parent_code', 'hierarchy_level', 'is_posting_account', 'is_active'])

    gl_codes = [r['account_code'] for r in records if r['is_posting_account']]
    registry.register('gl_posting_codes', gl_codes)
    print(f"  ✓ Chart of Accounts: {len(records)} accounts ({len(gl_codes)} posting)")


def generate_cost_centres():
    """Insert cost centres."""
    records = []
    for code, name, dept, mgr in COST_CENTRES:
        records.append({
            'cost_centre_code': code,
            'cost_centre_name': name,
            'department': dept,
            'manager': mgr,
            'is_active': True,
        })
    bulk_insert('gl.cost_centres', records,
                ['cost_centre_code', 'cost_centre_name', 'department', 'manager', 'is_active'])

    cc_codes = [r['cost_centre_code'] for r in records]
    registry.register('cost_centre_codes', cc_codes)
    print(f"  ✓ Cost Centres: {len(records)} inserted")


def generate_payment_schemes():
    """Insert payment scheme reference data."""
    records = []
    for code, name, stype, max_amt, settle, hours in PAYMENT_SCHEMES:
        records.append({
            'scheme_code': code,
            'scheme_name': name,
            'scheme_type': stype,
            'max_amount': max_amt,
            'settlement_cycle': settle,
            'operating_hours': hours,
            'is_active': True,
        })
    bulk_insert('payments.payment_schemes', records,
                ['scheme_code', 'scheme_name', 'scheme_type', 'max_amount',
                 'settlement_cycle', 'operating_hours', 'is_active'])

    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT scheme_id, scheme_code FROM payments.payment_schemes")).fetchall()
    scheme_map = {code: sid for sid, code in rows}
    registry.register('scheme_ids', list(scheme_map.values()))
    registry.register('scheme_map', [scheme_map])
    print(f"  ✓ Payment Schemes: {len(records)} inserted")


def generate_branches():
    """Insert branch dimension."""
    records = []
    for code, name, region, city, postcode, btype in BRANCHES:
        records.append({
            'branch_code': code,
            'branch_name': name,
            'region': region,
            'city': city,
            'postcode': postcode,
            'branch_type': btype,
            'is_active': True,
        })
    bulk_insert('warehouse_core.dim_branch', records,
                ['branch_code', 'branch_name', 'region', 'city', 'postcode', 'branch_type', 'is_active'])
    print(f"  ✓ Branches: {len(records)} inserted")


def generate_date_dimension():
    """Generate date dimension from 2020-01-01 to 2026-12-31."""
    # UK bank holidays (simplified — major ones)
    bank_holidays = {
        # 2024
        date(2024, 1, 1), date(2024, 3, 29), date(2024, 4, 1),
        date(2024, 5, 6), date(2024, 5, 27), date(2024, 8, 26),
        date(2024, 12, 25), date(2024, 12, 26),
        # 2025
        date(2025, 1, 1), date(2025, 4, 18), date(2025, 4, 21),
        date(2025, 5, 5), date(2025, 5, 26), date(2025, 8, 25),
        date(2025, 12, 25), date(2025, 12, 26),
    }

    start = date(2020, 1, 1)
    end = date(2026, 12, 31)
    records = []
    d = start
    while d <= end:
        is_month_end = (d + timedelta(days=1)).month != d.month
        is_quarter_end = is_month_end and d.month in (3, 6, 9, 12)
        is_year_end = d.month == 12 and d.day == 31
        fiscal_year = d.year if d.month >= 4 else d.year - 1
        fiscal_quarter = ((d.month - 4) % 12) // 3 + 1

        records.append({
            'date_key': int(d.strftime('%Y%m%d')),
            'full_date': d.isoformat(),
            'day_of_week': d.isoweekday(),
            'day_name': d.strftime('%A'),
            'day_of_month': d.day,
            'day_of_year': d.timetuple().tm_yday,
            'week_of_year': d.isocalendar()[1],
            'iso_week': d.isocalendar()[1],
            'month_number': d.month,
            'month_name': d.strftime('%B'),
            'month_short': d.strftime('%b'),
            'quarter': (d.month - 1) // 3 + 1,
            'quarter_name': f'Q{(d.month - 1) // 3 + 1}',
            'year': d.year,
            'fiscal_year': fiscal_year,
            'fiscal_quarter': fiscal_quarter,
            'is_weekend': d.isoweekday() >= 6,
            'is_bank_holiday': d in bank_holidays,
            'is_month_end': is_month_end,
            'is_quarter_end': is_quarter_end,
            'is_year_end': is_year_end,
        })
        d += timedelta(days=1)

    bulk_insert('warehouse_core.dim_date', records,
                list(records[0].keys()))
    print(f"  ✓ Date Dimension: {len(records)} days (2020-2026)")


def run():
    """Generate all reference data."""
    print("\n📋 Generating reference data...")
    generate_products()
    generate_chart_of_accounts()
    generate_cost_centres()
    generate_payment_schemes()
    generate_branches()
    generate_date_dimension()
    print("✅ Reference data complete\n")


if __name__ == '__main__':
    run()
