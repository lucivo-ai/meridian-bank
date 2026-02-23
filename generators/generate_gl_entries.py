"""
Generate General Ledger entries derived from transactions and banking operations.
Includes an intentional GL imbalance for DQ testing.
"""
import numpy as np
from datetime import date, timedelta
from tqdm import tqdm
from sqlalchemy import text
from generators.config import SEED, GL_IMBALANCE_BATCH
from generators.utils.relationships import bulk_insert, registry, get_engine


def generate_gl_entries():
    """Generate GL journal entries from banking activities."""
    rng = np.random.default_rng(SEED + 40)
    engine = get_engine()

    # Get posting accounts and cost centres
    with engine.connect() as conn:
        gl_codes = [r[0] for r in conn.execute(text(
            "SELECT account_code FROM gl.chart_of_accounts WHERE is_posting_account = TRUE"
        )).fetchall()]
        cc_codes = [r[0] for r in conn.execute(text(
            "SELECT cost_centre_code FROM gl.cost_centres"
        )).fetchall()]

    # GL account mappings for transaction types
    txn_gl_map = {
        'salary': ('2110', '4210'),       # Deposit liability ↔ Fee income
        'direct_debit': ('2110', '1120'),  # Deposit out ↔ Nostro
        'standing_order': ('2110', '1120'),
        'faster_payment': ('2110', '1120'),
        'card_payment': ('2110', '4220'),  # Deposit ↔ Interchange fee
        'interest': ('5110', '2120'),      # Interest expense ↔ Savings liability
        'fee': ('4210', '2110'),           # Fee income ↔ Deposit
        'loan_repayment': ('1210', '2110'),  # Loan asset ↔ Deposit
        'mortgage_payment': ('1220', '2110'),
        'transfer_in': ('2110', '1120'),
        'transfer_out': ('1120', '2110'),
        'bacs': ('2110', '1120'),
        'chaps': ('2110', '1120'),
        'atm_withdrawal': ('2110', '1130'),  # Deposit ↔ ATM cash
    }

    # Generate daily GL entries for 6 months
    records = []
    journal_counter = 0
    start_date = date(2024, 7, 1)
    end_date = date(2024, 12, 31)

    n_days = (end_date - start_date).days + 1
    print(f"  Generating GL entries for {n_days} days...")

    for day_offset in tqdm(range(n_days), desc="  GL entries", leave=False):
        current_date = start_date + timedelta(days=day_offset)
        date_str = current_date.isoformat()

        # ~150-250 journals per day (simplified from real txn volume)
        n_journals = rng.integers(150, 250)
        batch_id = f'BATCH-{current_date.strftime("%Y%m%d")}'

        for j in range(n_journals):
            journal_counter += 1
            journal_id = f'JNL-{journal_counter:08d}'
            cc = rng.choice(cc_codes)

            # Pick a transaction type and get GL codes
            txn_type = rng.choice(list(txn_gl_map.keys()))
            debit_code, credit_code = txn_gl_map[txn_type]

            amount = round(float(rng.lognormal(5.0, 1.2)), 2)
            amount = min(amount, 100000)

            source_ref = f'TXN-{rng.integers(100000, 999999)}'

            # Debit entry
            records.append({
                'journal_id': journal_id,
                'batch_id': batch_id,
                'entry_date': date_str,
                'posting_date': date_str,
                'account_code': debit_code,
                'cost_centre_code': cc,
                'debit_amount': amount,
                'credit_amount': 0,
                'currency': 'GBP',
                'description': f'{txn_type.replace("_"," ").title()} posting',
                'source_system': 'core_banking',
                'source_reference': source_ref,
                'is_manual': False,
                'posted_by': 'SYSTEM',
            })

            # Credit entry
            records.append({
                'journal_id': journal_id,
                'batch_id': batch_id,
                'entry_date': date_str,
                'posting_date': date_str,
                'account_code': credit_code,
                'cost_centre_code': cc,
                'debit_amount': 0,
                'credit_amount': amount,
                'currency': 'GBP',
                'description': f'{txn_type.replace("_"," ").title()} posting',
                'source_system': 'core_banking',
                'source_reference': source_ref,
                'is_manual': False,
                'posted_by': 'SYSTEM',
            })

        # Insert in chunks
        if len(records) >= 10000:
            _insert_gl_batch(records)
            records = []

    # ── Intentional GL imbalance ──────────────────────────
    # Add an unbalanced journal entry for DQ testing
    journal_counter += 1
    records.append({
        'journal_id': f'JNL-{journal_counter:08d}',
        'batch_id': GL_IMBALANCE_BATCH,
        'entry_date': '2024-11-15',
        'posting_date': '2024-11-15',
        'account_code': '4210',
        'cost_centre_code': 'CC-FIN',
        'debit_amount': 15000.00,
        'credit_amount': 0,
        'currency': 'GBP',
        'description': 'IMBALANCED ENTRY — Fee adjustment (DQ test)',
        'source_system': 'manual',
        'source_reference': 'MANUAL-ERR-001',
        'is_manual': True,
        'posted_by': 'FIN-003',
    })
    journal_counter += 1
    records.append({
        'journal_id': f'JNL-{journal_counter:08d}',
        'batch_id': GL_IMBALANCE_BATCH,
        'entry_date': '2024-11-15',
        'posting_date': '2024-11-15',
        'account_code': '2110',
        'cost_centre_code': 'CC-FIN',
        'debit_amount': 0,
        'credit_amount': 14500.00,  # £500 imbalance!
        'currency': 'GBP',
        'description': 'IMBALANCED ENTRY — Fee adjustment (DQ test)',
        'source_system': 'manual',
        'source_reference': 'MANUAL-ERR-001',
        'is_manual': True,
        'posted_by': 'FIN-003',
    })

    if records:
        _insert_gl_batch(records)

    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM gl.gl_entries")).scalar()
    print(f"  ✓ GL Entries: {count:,} inserted (includes 1 imbalanced journal: {GL_IMBALANCE_BATCH})")


def _insert_gl_batch(records):
    """Insert GL entry batch."""
    cols = ['journal_id', 'batch_id', 'entry_date', 'posting_date', 'account_code',
            'cost_centre_code', 'debit_amount', 'credit_amount', 'currency',
            'description', 'source_system', 'source_reference', 'is_manual', 'posted_by']
    bulk_insert('gl.gl_entries', records, cols)


def generate_gl_balances():
    """Generate period-end GL balance snapshots."""
    rng = np.random.default_rng(SEED + 41)
    engine = get_engine()

    # Get monthly summaries from GL entries
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT account_code, cost_centre_code,
                   DATE_TRUNC('month', entry_date) + INTERVAL '1 month' - INTERVAL '1 day' as period_end,
                   SUM(debit_amount) as total_debits,
                   SUM(credit_amount) as total_credits
            FROM gl.gl_entries
            GROUP BY account_code, cost_centre_code, DATE_TRUNC('month', entry_date)
            ORDER BY period_end, account_code
        """)).fetchall()

    records = []
    running_balances = {}  # (account_code, cc) -> balance

    for acct, cc, period_end, debits, credits in rows:
        key = (acct, cc)
        opening = running_balances.get(key, 0)
        closing = opening + float(debits) - float(credits)
        running_balances[key] = closing

        records.append({
            'period_end_date': period_end.date().isoformat() if hasattr(period_end, 'date') else str(period_end)[:10],
            'account_code': acct,
            'cost_centre_code': cc,
            'opening_balance': round(opening, 2),
            'period_debits': round(float(debits), 2),
            'period_credits': round(float(credits), 2),
            'closing_balance': round(closing, 2),
            'currency': 'GBP',
        })

    if records:
        cols = list(records[0].keys())
        bulk_insert('gl.gl_balances', records, cols)
    print(f"  ✓ GL Balances: {len(records)} period-end snapshots")


def run():
    """Generate all GL data."""
    print("\n📒 Generating General Ledger data...")
    generate_gl_entries()
    generate_gl_balances()
    print("✅ GL data complete\n")


if __name__ == '__main__':
    run()
