"""
Generate transactions for Meridian Community Bank.
~3M transactions across 6 months with realistic patterns and distributions.
This is the largest generator — uses batched inserts for performance.
"""
import numpy as np
from datetime import date, timedelta, datetime
from tqdm import tqdm
from sqlalchemy import text
from generators.config import (
    SEED, TXN_DATE_START, TXN_DATE_END, AVG_TXN_PER_ACCOUNT_MONTH,
    ZERO_AMOUNT_TXNS, BATCH_SIZE
)
from generators.utils.relationships import bulk_insert, registry, get_engine
from generators.utils.faker_extensions import (
    get_counterparty, generate_account_number, generate_sort_code,
    RETAIL_COUNTERPARTIES
)


# Transaction type distributions by account category
TXN_TYPE_WEIGHTS = {
    'current_account': {
        'types': ['direct_debit', 'standing_order', 'faster_payment', 'card_payment',
                  'atm_withdrawal', 'salary', 'transfer_out', 'transfer_in', 'bacs', 'fee'],
        'weights': [0.15, 0.08, 0.20, 0.25, 0.05, 0.08, 0.05, 0.05, 0.07, 0.02],
    },
    'savings': {
        'types': ['transfer_in', 'transfer_out', 'interest', 'faster_payment'],
        'weights': [0.40, 0.35, 0.15, 0.10],
    },
    'personal_loan': {
        'types': ['loan_repayment', 'interest', 'fee'],
        'weights': [0.70, 0.20, 0.10],
    },
    'mortgage': {
        'types': ['mortgage_payment', 'interest', 'fee'],
        'weights': [0.70, 0.20, 0.10],
    },
    'credit_card': {
        'types': ['card_payment', 'faster_payment', 'interest', 'fee', 'transfer_in'],
        'weights': [0.55, 0.10, 0.15, 0.05, 0.15],
    },
    'business_current': {
        'types': ['direct_debit', 'faster_payment', 'bacs', 'chaps', 'card_payment',
                  'salary', 'transfer_out', 'transfer_in', 'fee'],
        'weights': [0.12, 0.20, 0.15, 0.05, 0.15, 0.10, 0.08, 0.10, 0.05],
    },
    'business_loan': {
        'types': ['loan_repayment', 'interest', 'fee'],
        'weights': [0.70, 0.20, 0.10],
    },
    'business_savings': {
        'types': ['transfer_in', 'transfer_out', 'interest'],
        'weights': [0.45, 0.40, 0.15],
    },
}

# Amount distributions by transaction type (mean, std for lognormal)
AMOUNT_PARAMS = {
    'direct_debit': (4.0, 0.8),      # ~£55 median
    'standing_order': (5.0, 0.5),     # ~£150 median
    'faster_payment': (4.5, 1.0),     # ~£90 median, wide spread
    'card_payment': (3.0, 0.9),       # ~£20 median
    'atm_withdrawal': (3.3, 0.3),     # ~£27, tight (multiples of £10)
    'salary': (7.5, 0.4),             # ~£1800 median
    'transfer_out': (5.0, 1.2),       # ~£150, wide
    'transfer_in': (5.0, 1.2),        # ~£150, wide
    'bacs': (5.5, 1.0),              # ~£245 median
    'chaps': (9.0, 1.5),             # ~£8100 median, very wide
    'interest': (2.0, 1.0),           # ~£7 median
    'fee': (1.5, 0.5),               # ~£4.5 median
    'loan_repayment': (5.8, 0.3),     # ~£330 median
    'mortgage_payment': (6.7, 0.3),   # ~£812 median
}

CHANNELS = ['online', 'mobile', 'branch', 'atm', 'phone', 'api', 'batch']
CHANNEL_WEIGHTS = np.array([0.25, 0.35, 0.05, 0.05, 0.03, 0.12, 0.15])


def generate_transactions():
    """Generate transaction records for all active accounts."""
    rng = np.random.default_rng(SEED + 20)

    engine = get_engine()

    # Get active accounts with their product categories
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT a.account_id, a.customer_id, a.status, p.category,
                   c.type as customer_type
            FROM core_banking.accounts a
            JOIN core_banking.products p ON a.product_id = p.product_id
            LEFT JOIN core_banking.customers c ON a.customer_id = c.customer_id
            WHERE a.status IN ('active', 'in_arrears')
            ORDER BY a.account_id
        """)).fetchall()

    accounts = [(r[0], r[1], r[2], r[3], r[4] or 'personal') for r in rows]
    print(f"  Generating transactions for {len(accounts)} active accounts...")

    # Calculate date range
    n_days = (TXN_DATE_END - TXN_DATE_START).days + 1
    dates = [TXN_DATE_START + timedelta(days=d) for d in range(n_days)]

    all_records = []
    total_txns = 0
    zero_amount_inserted = 0

    for account_id, customer_id, status, product_cat, cust_type in tqdm(accounts, desc="  Transactions", leave=False):
        # Determine number of transactions for this account
        if product_cat in ('savings', 'business_savings'):
            avg_monthly = 3
        elif product_cat in ('personal_loan', 'mortgage', 'business_loan'):
            avg_monthly = 2
        elif product_cat == 'credit_card':
            avg_monthly = 15
        else:
            avg_monthly = AVG_TXN_PER_ACCOUNT_MONTH

        n_txns = max(1, int(rng.poisson(avg_monthly * 6)))  # 6 months

        if n_txns == 0:
            continue

        # Get transaction type config
        type_config = TXN_TYPE_WEIGHTS.get(product_cat, TXN_TYPE_WEIGHTS['current_account'])
        txn_types = rng.choice(type_config['types'], size=n_txns, p=type_config['weights'])
        txn_dates = sorted(rng.choice(dates, size=n_txns))

        is_business = cust_type == 'business'

        for i in range(n_txns):
            txn_type = txn_types[i]
            txn_date = txn_dates[i]

            # Generate amount
            mean, std = AMOUNT_PARAMS.get(txn_type, (4.0, 0.8))
            amount = round(float(rng.lognormal(mean, std)), 2)
            amount = min(amount, 500000)  # Cap at £500k

            # ATM: round to £10
            if txn_type == 'atm_withdrawal':
                amount = round(amount / 10) * 10
                amount = max(10, min(amount, 500))

            # Intentional zero amount DQ issue
            if zero_amount_inserted < ZERO_AMOUNT_TXNS and rng.random() < 0.0001:
                amount = 0.0
                zero_amount_inserted += 1

            # Sign: debits are negative
            is_credit = txn_type in ('salary', 'transfer_in', 'interest')
            if not is_credit:
                amount = -amount

            # Channel
            if txn_type in ('direct_debit', 'standing_order', 'bacs', 'interest', 'fee'):
                channel = 'batch'
            elif txn_type == 'atm_withdrawal':
                channel = 'atm'
            elif txn_type in ('card_payment',):
                channel = rng.choice(['mobile', 'online', 'branch'], p=[0.4, 0.3, 0.3])
            elif txn_type == 'salary':
                channel = 'api' if rng.random() > 0.5 else 'batch'
            elif txn_type in ('chaps',):
                channel = 'api'
            else:
                channel = rng.choice(CHANNELS, p=CHANNEL_WEIGHTS)

            # Timestamp
            hour = int(rng.normal(13, 4))
            hour = max(0, min(23, hour))
            minute = int(rng.integers(0, 60))
            txn_ts = datetime(txn_date.year, txn_date.month, txn_date.day, hour, minute)

            # Status
            status_val = 'completed'
            if rng.random() < 0.005:
                status_val = rng.choice(['failed', 'reversed', 'disputed'])

            counterparty = get_counterparty(txn_type, is_business, rng)

            all_records.append({
                'account_id': int(account_id),
                'txn_date': txn_date.isoformat(),
                'txn_timestamp': txn_ts.isoformat(),
                'value_date': txn_date.isoformat(),
                'amount': amount,
                'currency': 'GBP',
                'txn_type': txn_type,
                'description': f'{txn_type.replace("_"," ").title()} - {counterparty}',
                'counterparty_name': counterparty,
                'counterparty_account': generate_account_number(rng) if rng.random() > 0.3 else None,
                'counterparty_sort_code': generate_sort_code(rng) if rng.random() > 0.3 else None,
                'channel': channel,
                'reference': f'REF{rng.integers(100000, 999999)}',
                'status': status_val,
                'balance_after': None,  # Will be calculated later or left null
            })

            total_txns += 1

        # Batch insert periodically to manage memory
        if len(all_records) >= 50000:
            _insert_txn_batch(all_records)
            all_records = []

    # Insert remaining
    if all_records:
        _insert_txn_batch(all_records)

    # Register transaction count
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM core_banking.transactions")).scalar()

    print(f"  ✓ Transactions: {count:,} inserted ({zero_amount_inserted} zero-amount DQ issues)")


def _insert_txn_batch(records):
    """Insert a batch of transaction records."""
    cols = ['account_id', 'txn_date', 'txn_timestamp', 'value_date', 'amount',
            'currency', 'txn_type', 'description', 'counterparty_name',
            'counterparty_account', 'counterparty_sort_code', 'channel',
            'reference', 'status', 'balance_after']
    bulk_insert('core_banking.transactions', records, cols)


def run():
    """Generate all transaction data."""
    print("\n💳 Generating transaction data...")
    generate_transactions()
    print("✅ Transaction data complete\n")


if __name__ == '__main__':
    run()
