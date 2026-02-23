"""
Generate Payments data: standing orders, direct debits, payment instructions, receipts, failed payments.
"""
import numpy as np
from datetime import date, timedelta
from tqdm import tqdm
from sqlalchemy import text
from generators.config import SEED
from generators.utils.relationships import bulk_insert, registry, get_engine
from generators.utils.faker_extensions import (
    generate_account_number, generate_sort_code, RETAIL_COUNTERPARTIES
)


def generate_standing_orders():
    """Generate standing orders for current account holders."""
    rng = np.random.default_rng(SEED + 70)
    engine = get_engine()

    with engine.connect() as conn:
        accounts = conn.execute(text("""
            SELECT a.account_id FROM core_banking.accounts a
            JOIN core_banking.products p ON a.product_id = p.product_id
            WHERE p.category IN ('current_account', 'business_current') AND a.status = 'active'
        """)).fetchall()

    account_ids = [r[0] for r in accounts]
    records = []
    payees = ['Landlord', 'Savings Transfer', 'Charity Donation', 'Gym Membership',
              'Insurance Premium', 'Child Maintenance', 'Parent Support']

    for aid in tqdm(account_ids, desc="  Standing orders", leave=False):
        n = max(0, int(rng.poisson(1.5)))
        for _ in range(n):
            start = date(2023, 1, 1) + timedelta(days=int(rng.integers(0, 730)))
            records.append({
                'account_id': aid,
                'payee_name': rng.choice(payees),
                'payee_account': generate_account_number(rng),
                'payee_sort_code': generate_sort_code(rng),
                'amount': round(float(rng.choice([25, 50, 100, 150, 200, 300, 500, 750, 1000])), 2),
                'currency': 'GBP',
                'frequency': rng.choice(['monthly', 'weekly', 'quarterly'], p=[0.70, 0.15, 0.15]),
                'start_date': start.isoformat(),
                'end_date': None if rng.random() > 0.2 else (start + timedelta(days=int(rng.integers(180, 730)))).isoformat(),
                'next_payment_date': '2025-01-15',
                'reference': f'SO-{rng.integers(10000, 99999)}',
                'status': rng.choice(['active', 'active', 'active', 'cancelled']),
            })

    cols = list(records[0].keys())
    bulk_insert('core_banking.standing_orders', records, cols)
    print(f"  ✓ Standing Orders: {len(records)} generated")


def generate_direct_debits():
    """Generate direct debit mandates."""
    rng = np.random.default_rng(SEED + 71)
    engine = get_engine()

    with engine.connect() as conn:
        accounts = conn.execute(text("""
            SELECT a.account_id FROM core_banking.accounts a
            JOIN core_banking.products p ON a.product_id = p.product_id
            WHERE p.category IN ('current_account', 'business_current') AND a.status = 'active'
        """)).fetchall()

    account_ids = [r[0] for r in accounts]
    originators = [
        ('British Gas', 'SUN-001'), ('EDF Energy', 'SUN-002'), ('Thames Water', 'SUN-003'),
        ('Sky TV', 'SUN-004'), ('BT', 'SUN-005'), ('Council Tax', 'SUN-006'),
        ('HMRC', 'SUN-007'), ('Netflix', 'SUN-008'), ('Spotify', 'SUN-009'),
        ('Virgin Media', 'SUN-010'), ('Admiral Insurance', 'SUN-011'),
        ('Aviva', 'SUN-012'), ('PureGym', 'SUN-013'),
    ]

    records = []
    for aid in tqdm(account_ids, desc="  Direct debits", leave=False):
        n = max(0, int(rng.poisson(3)))
        selected = rng.choice(len(originators), size=min(n, len(originators)), replace=False)
        for idx in selected:
            name, sun = originators[idx]
            mandate = date(2020, 1, 1) + timedelta(days=int(rng.integers(0, 1800)))
            records.append({
                'account_id': aid,
                'originator_name': name,
                'originator_id': sun,
                'reference': f'DD-{rng.integers(100000, 999999)}',
                'mandate_date': mandate.isoformat(),
                'first_collection': (mandate + timedelta(days=int(rng.integers(14, 45)))).isoformat(),
                'last_collection': '2024-12-15',
                'status': rng.choice(['active', 'active', 'active', 'cancelled', 'suspended'], p=[0.60, 0.20, 0.05, 0.10, 0.05]),
            })

    cols = list(records[0].keys())
    bulk_insert('core_banking.direct_debits', records, cols)
    print(f"  ✓ Direct Debits: {len(records)} generated")


def generate_payment_flows():
    """Generate payment instructions and receipts."""
    rng = np.random.default_rng(SEED + 72)
    engine = get_engine()

    with engine.connect() as conn:
        scheme_map = {r[1]: r[0] for r in conn.execute(text(
            "SELECT scheme_id, scheme_code FROM payments.payment_schemes"
        )).fetchall()}

        accounts = conn.execute(text(
            "SELECT account_id FROM core_banking.accounts WHERE status = 'active'"
        )).fetchall()

    account_ids = [r[0] for r in accounts]
    schemes = list(scheme_map.keys())
    scheme_weights = np.array([0.30, 0.20, 0.15, 0.05, 0.02, 0.10, 0.08, 0.05, 0.03, 0.02])
    scheme_weights = scheme_weights / scheme_weights.sum()

    # Payment Instructions (outbound) — ~500K
    print("  Generating payment instructions...")
    pi_records = []
    n_instructions = 500000
    sampled_accounts = rng.choice(account_ids, size=n_instructions)
    sampled_schemes = rng.choice(schemes, size=n_instructions, p=scheme_weights)

    for i in tqdm(range(n_instructions), desc="  Payment instructions", leave=False):
        inst_date = date(2024, 7, 1) + timedelta(days=int(rng.integers(0, 183)))
        scheme_code = sampled_schemes[i]

        pi_records.append({
            'account_id': int(sampled_accounts[i]),
            'scheme_id': scheme_map[scheme_code],
            'instruction_date': f'{inst_date.isoformat()} {rng.integers(8,18):02d}:{rng.integers(0,59):02d}:00',
            'amount': round(float(rng.lognormal(5, 1.2)), 2),
            'currency': 'GBP',
            'beneficiary_name': rng.choice(RETAIL_COUNTERPARTIES),
            'beneficiary_account': generate_account_number(rng),
            'beneficiary_sort_code': generate_sort_code(rng),
            'reference': f'PAY-{rng.integers(100000, 999999)}',
            'payment_type': rng.choice(['single', 'bulk', 'standing_order'], p=[0.70, 0.15, 0.15]),
            'priority': rng.choice(['normal', 'urgent'], p=[0.92, 0.08]),
            'status': rng.choice(['settled', 'settled', 'settled', 'sent', 'rejected'], p=[0.80, 0.08, 0.05, 0.05, 0.02]),
            'settlement_date': (inst_date + timedelta(days=int(rng.integers(0, 3)))).isoformat(),
        })

        if len(pi_records) >= 50000:
            cols = list(pi_records[0].keys())
            bulk_insert('payments.payment_instructions', pi_records, cols)
            pi_records = []

    if pi_records:
        bulk_insert('payments.payment_instructions', pi_records, list(pi_records[0].keys()))

    # Payment Receipts (inbound) — ~500K
    print("  Generating payment receipts...")
    pr_records = []
    n_receipts = 500000
    sampled_accounts = rng.choice(account_ids, size=n_receipts)
    sampled_schemes = rng.choice(schemes, size=n_receipts, p=scheme_weights)

    for i in tqdm(range(n_receipts), desc="  Payment receipts", leave=False):
        rcpt_date = date(2024, 7, 1) + timedelta(days=int(rng.integers(0, 183)))
        scheme_code = sampled_schemes[i]

        pr_records.append({
            'account_id': int(sampled_accounts[i]),
            'scheme_id': scheme_map[scheme_code],
            'receipt_date': f'{rcpt_date.isoformat()} {rng.integers(8,18):02d}:{rng.integers(0,59):02d}:00',
            'amount': round(float(rng.lognormal(5, 1.2)), 2),
            'currency': 'GBP',
            'sender_name': rng.choice(RETAIL_COUNTERPARTIES),
            'sender_account': generate_account_number(rng),
            'sender_sort_code': generate_sort_code(rng),
            'reference': f'RCV-{rng.integers(100000, 999999)}',
            'status': rng.choice(['applied', 'applied', 'applied', 'received'], p=[0.85, 0.05, 0.05, 0.05]),
        })

        if len(pr_records) >= 50000:
            cols = list(pr_records[0].keys())
            bulk_insert('payments.payment_receipts', pr_records, cols)
            pr_records = []

    if pr_records:
        bulk_insert('payments.payment_receipts', pr_records, list(pr_records[0].keys()))

    # Failed payments — ~2% of instructions
    print("  Generating failed payments...")
    with engine.connect() as conn:
        failed_instructions = conn.execute(text(
            "SELECT instruction_id, amount FROM payments.payment_instructions WHERE status = 'rejected' LIMIT 5000"
        )).fetchall()

    fp_records = []
    reasons = ['insufficient_funds', 'invalid_account', 'invalid_sort_code',
               'account_closed', 'amount_limit_exceeded', 'technical_error']
    reason_weights = np.array([0.40, 0.15, 0.10, 0.10, 0.10, 0.15])

    for inst_id, amount in failed_instructions:
        fp_records.append({
            'instruction_id': inst_id,
            'failure_date': f'2024-{rng.integers(7,12):02d}-{rng.integers(1,28):02d} 14:00:00',
            'failure_reason': rng.choice(reasons, p=reason_weights),
            'original_amount': float(amount),
            'currency': 'GBP',
            'resolution_status': rng.choice(['unresolved', 'retried', 'reversed'], p=[0.30, 0.40, 0.30]),
        })

    if fp_records:
        cols = ['instruction_id', 'failure_date', 'failure_reason', 'original_amount',
                'currency', 'resolution_status']
        bulk_insert('payments.failed_payments', fp_records, cols)

    print(f"  ✓ Payment Instructions: {n_instructions:,}")
    print(f"  ✓ Payment Receipts: {n_receipts:,}")
    print(f"  ✓ Failed Payments: {len(fp_records)}")


def run():
    print("\n💸 Generating Payments data...")
    generate_standing_orders()
    generate_direct_debits()
    generate_payment_flows()
    print("✅ Payments data complete\n")


if __name__ == '__main__':
    run()
