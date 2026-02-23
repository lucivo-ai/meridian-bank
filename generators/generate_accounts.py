"""
Generate accounts for Meridian Community Bank customers.
~85,000 accounts across all product types with realistic distributions.
"""
import numpy as np
from datetime import date, timedelta
from tqdm import tqdm
from sqlalchemy import text
from generators.config import (
    SEED, ACTIVE_ACCOUNT_RATIO, ARREARS_RATIO, ORPHANED_ACCOUNTS
)
from generators.utils.relationships import bulk_insert, registry, get_engine
from generators.utils.faker_extensions import generate_sort_code, generate_account_number


def generate_accounts():
    """Generate account records linked to customers and products."""
    rng = np.random.default_rng(SEED + 10)

    personal_ids = registry.get_ids('personal_customer_ids')
    business_ids = registry.get_ids('business_customer_ids')

    # Get product IDs by category
    engine = get_engine()
    with engine.connect() as conn:
        products = conn.execute(text(
            "SELECT product_id, category FROM core_banking.products"
        )).fetchall()
    products_by_cat = {}
    for pid, cat in products:
        products_by_cat.setdefault(cat, []).append(pid)

    records = []
    used_account_numbers = set()

    def make_unique_account(rng):
        while True:
            sc = generate_sort_code(rng)
            an = generate_account_number(rng)
            key = f"{an}-{sc}"
            if key not in used_account_numbers:
                used_account_numbers.add(key)
                return an, sc

    # ── Personal customers ────────────────────────────────
    # Each gets: 1 current account (100%), savings (60%), loan/mortgage (20%), card (30%)
    print(f"  Generating accounts for {len(personal_ids)} personal customers...")
    for cid in tqdm(personal_ids, desc="  Personal accounts", leave=False):
        onboarded_offset = rng.integers(0, 3650)
        base_date = date(2015, 1, 1) + timedelta(days=int(onboarded_offset))
        if base_date > date(2024, 12, 31):
            base_date = date(2024, 12, 31)

        # Current account (always)
        an, sc = make_unique_account(rng)
        status = 'active' if rng.random() < ACTIVE_ACCOUNT_RATIO else rng.choice(['dormant', 'closed'])
        records.append(_make_account(int(cid), rng.choice(products_by_cat['current_account']),
                                      an, sc, status, base_date, rng))

        # Savings (60%)
        if rng.random() < 0.60:
            an, sc = make_unique_account(rng)
            cat = rng.choice(['savings'] * 3)
            records.append(_make_account(int(cid), rng.choice(products_by_cat['savings']),
                                          an, sc, 'active', base_date + timedelta(days=int(rng.integers(0, 365))), rng))

        # Loan or mortgage (20%)
        if rng.random() < 0.20:
            an, sc = make_unique_account(rng)
            cat = rng.choice(['personal_loan', 'mortgage'], p=[0.6, 0.4])
            status = 'active'
            if rng.random() < ARREARS_RATIO:
                status = rng.choice(['in_arrears', 'default'], p=[0.8, 0.2])
            records.append(_make_account(int(cid), rng.choice(products_by_cat[cat]),
                                          an, sc, status, base_date + timedelta(days=int(rng.integers(30, 1000))), rng))

        # Credit card (30%)
        if rng.random() < 0.30:
            an, sc = make_unique_account(rng)
            records.append(_make_account(int(cid), rng.choice(products_by_cat['credit_card']),
                                          an, sc, 'active', base_date + timedelta(days=int(rng.integers(0, 730))), rng))

    # ── Business customers ────────────────────────────────
    # Each gets: 1 business current (100%), business savings (50%), business loan (30%)
    print(f"  Generating accounts for {len(business_ids)} business customers...")
    for cid in tqdm(business_ids, desc="  Business accounts", leave=False):
        onboarded_offset = rng.integers(0, 3650)
        base_date = date(2015, 1, 1) + timedelta(days=int(onboarded_offset))
        if base_date > date(2024, 12, 31):
            base_date = date(2024, 12, 31)

        # Business current (always)
        an, sc = make_unique_account(rng)
        records.append(_make_account(int(cid), rng.choice(products_by_cat['business_current']),
                                      an, sc, 'active', base_date, rng))

        # Business savings (50%)
        if rng.random() < 0.50:
            an, sc = make_unique_account(rng)
            records.append(_make_account(int(cid), rng.choice(products_by_cat['business_savings']),
                                          an, sc, 'active', base_date + timedelta(days=int(rng.integers(0, 365))), rng))

        # Business loan (30%)
        if rng.random() < 0.30:
            an, sc = make_unique_account(rng)
            status = 'active'
            if rng.random() < ARREARS_RATIO:
                status = 'in_arrears'
            records.append(_make_account(int(cid), rng.choice(products_by_cat['business_loan']),
                                          an, sc, status, base_date + timedelta(days=int(rng.integers(30, 730))), rng))

    # ── Orphaned accounts (intentional DQ issue) ──────────
    max_customer_id = int(max(personal_ids.max(), business_ids.max()))
    for i in range(ORPHANED_ACCOUNTS):
        an, sc = make_unique_account(rng)
        fake_cid = max_customer_id + 1000 + i  # Non-existent customer
        records.append({
            'customer_id': fake_cid,
            'product_id': int(rng.choice(products_by_cat['current_account'])),
            'account_number': an,
            'sort_code': sc,
            'account_name': f'Orphaned Account {i+1}',
            'status': 'active',
            'currency': 'GBP',
            'credit_limit': None,
            'overdraft_limit': None,
            'opened_date': '2023-06-15',
            'closed_date': None,
            'last_transaction_date': None,
        })

    # Need to temporarily disable FK constraint for orphaned accounts
    with engine.connect() as conn:
        conn.execute(text("ALTER TABLE core_banking.accounts DROP CONSTRAINT IF EXISTS accounts_customer_id_fkey"))
        conn.commit()

    cols = ['customer_id', 'product_id', 'account_number', 'sort_code', 'account_name',
            'status', 'currency', 'credit_limit', 'overdraft_limit', 'opened_date',
            'closed_date', 'last_transaction_date']
    print(f"  Inserting {len(records)} accounts...")
    bulk_insert('core_banking.accounts', records, cols)

    # Re-add FK (won't validate existing data)
    with engine.connect() as conn:
        conn.execute(text("""
            ALTER TABLE core_banking.accounts
            ADD CONSTRAINT accounts_customer_id_fkey
            FOREIGN KEY (customer_id) REFERENCES core_banking.customers(customer_id) NOT VALID
        """))
        conn.commit()

    # Retrieve and register account IDs
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT account_id, customer_id, product_id, status FROM core_banking.accounts ORDER BY account_id"
        )).fetchall()

    all_ids = [r[0] for r in rows]
    active_ids = [r[0] for r in rows if r[1] in ['active', 'in_arrears']]
    account_customer_map = {r[0]: r[1] for r in rows}

    registry.register('account_ids', all_ids)
    registry.register('active_account_ids', active_ids)
    registry.register('account_customer_map', [account_customer_map])

    print(f"  ✓ Accounts: {len(all_ids)} total ({len(active_ids)} active, {ORPHANED_ACCOUNTS} orphaned)")
    return all_ids


def _make_account(customer_id, product_id, account_number, sort_code, status, opened_date, rng):
    """Helper to create an account record."""
    if opened_date > date(2024, 12, 31):
        opened_date = date(2024, 12, 31)

    closed_date = None
    if status == 'closed':
        closed_date = (opened_date + timedelta(days=int(rng.integers(90, 2000)))).isoformat()

    credit_limit = None
    overdraft_limit = None
    # Credit cards get a limit
    if product_id in range(15, 17):  # Rough range for credit cards
        credit_limit = float(rng.choice([1000, 2000, 3000, 5000, 7500, 10000, 15000]))
    # Current accounts may have overdraft
    if rng.random() < 0.3:
        overdraft_limit = float(rng.choice([250, 500, 1000, 1500, 2000, 3000]))

    return {
        'customer_id': customer_id,
        'product_id': int(product_id),
        'account_number': account_number,
        'sort_code': sort_code,
        'account_name': None,
        'status': status,
        'currency': 'GBP',
        'credit_limit': credit_limit,
        'overdraft_limit': overdraft_limit,
        'opened_date': opened_date.isoformat(),
        'closed_date': closed_date,
        'last_transaction_date': None,
    }


def run():
    """Generate all account data."""
    print("\n🏦 Generating account data...")
    generate_accounts()
    print("✅ Account data complete\n")


if __name__ == '__main__':
    run()
