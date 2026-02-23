"""
Generate Treasury data — positions, FX rates, interbank lending, liquidity pool.
"""
import numpy as np
from datetime import date, timedelta
from generators.config import SEED
from generators.utils.relationships import bulk_insert


def generate_treasury_positions():
    """Generate treasury book positions."""
    rng = np.random.default_rng(SEED + 50)
    records = []

    instruments = [
        ('gilt', 'banking_book', 'UK Gilt {}yr', (10, 50)),
        ('corporate_bond', 'banking_book', 'Corp Bond {}', (5, 30)),
        ('money_market', 'banking_book', 'MM Deposit {}', (1, 20)),
        ('certificate_of_deposit', 'liquidity_buffer', 'CD {}', (1, 10)),
        ('repo', 'banking_book', 'Repo {}', (5, 50)),
        ('interest_rate_swap', 'banking_book', 'IRS {}', (10, 100)),
    ]

    counterparties = ['Barclays', 'HSBC', 'Lloyds', 'NatWest', 'Standard Chartered',
                      'Goldman Sachs', 'JP Morgan', 'Deutsche Bank', 'BNP Paribas']

    for month in range(7, 13):  # Jul-Dec 2024
        pos_date = date(2024, month, 28)
        for inst_type, book, name_tmpl, (n_min, n_max) in instruments:
            n_positions = rng.integers(n_min, n_max)
            for i in range(n_positions):
                notional = round(float(rng.lognormal(14, 1.5)), 2)
                notional = min(notional, 500_000_000)

                records.append({
                    'position_date': pos_date.isoformat(),
                    'instrument_type': inst_type,
                    'instrument_ref': f'{inst_type[:3].upper()}-{rng.integers(1000,9999)}',
                    'counterparty': rng.choice(counterparties),
                    'notional_amount': notional,
                    'currency': rng.choice(['GBP', 'GBP', 'GBP', 'USD', 'EUR']),
                    'market_value': round(notional * rng.uniform(0.85, 1.15), 2),
                    'book': book,
                    'maturity_date': (pos_date + timedelta(days=int(rng.integers(30, 3650)))).isoformat(),
                    'yield_rate': round(float(rng.uniform(0.01, 0.08)), 6),
                })

    cols = list(records[0].keys())
    bulk_insert('treasury.positions', records, cols)
    print(f"  ✓ Treasury Positions: {len(records)} generated")


def generate_fx_rates():
    """Generate daily FX rates."""
    rng = np.random.default_rng(SEED + 51)
    currencies = ['USD', 'EUR', 'JPY', 'CHF', 'AUD', 'CAD', 'SEK', 'NOK', 'DKK', 'PLN']
    base_rates = {
        'USD': 1.27, 'EUR': 1.16, 'JPY': 188.5, 'CHF': 1.12,
        'AUD': 1.93, 'CAD': 1.72, 'SEK': 13.2, 'NOK': 13.5,
        'DKK': 8.65, 'PLN': 5.05,
    }

    records = []
    start = date(2024, 7, 1)
    n_days = 184

    for day in range(n_days):
        d = start + timedelta(days=day)
        if d.isoweekday() > 5:
            continue  # Skip weekends

        for ccy, base in base_rates.items():
            drift = rng.normal(0, 0.003)
            base_rates[ccy] = base * (1 + drift)
            records.append({
                'rate_date': d.isoformat(),
                'base_currency': 'GBP',
                'quote_currency': ccy,
                'spot_rate': round(base_rates[ccy], 6),
                'source': 'ECB',
            })

    cols = list(records[0].keys())
    bulk_insert('treasury.fx_rates', records, cols)
    print(f"  ✓ FX Rates: {len(records)} generated")


def generate_interbank():
    """Generate interbank lending/borrowing positions."""
    rng = np.random.default_rng(SEED + 52)
    counterparties = ['Barclays', 'HSBC', 'Lloyds', 'NatWest', 'Santander UK']
    records = []

    for month in range(7, 13):
        n = rng.integers(10, 25)
        for _ in range(n):
            trade_date = date(2024, month, int(rng.integers(1, 28)))
            term = int(rng.choice([7, 14, 30, 60, 90, 180, 365]))
            records.append({
                'trade_date': trade_date.isoformat(),
                'settlement_date': (trade_date + timedelta(days=2)).isoformat(),
                'maturity_date': (trade_date + timedelta(days=term)).isoformat(),
                'direction': rng.choice(['lend', 'borrow'], p=[0.55, 0.45]),
                'counterparty': rng.choice(counterparties),
                'principal_amount': round(float(rng.choice([5, 10, 15, 20, 25, 50]) * 1_000_000), 2),
                'currency': 'GBP',
                'interest_rate': round(float(rng.uniform(0.04, 0.06)), 6),
                'status': 'active' if month >= 11 else rng.choice(['active', 'matured'], p=[0.3, 0.7]),
            })

    cols = ['trade_date', 'settlement_date', 'maturity_date', 'direction',
            'counterparty', 'principal_amount', 'currency', 'interest_rate', 'status']
    bulk_insert('treasury.interbank_lending', records, cols)
    print(f"  ✓ Interbank Lending: {len(records)} generated")


def generate_liquidity_pool():
    """Generate liquidity buffer composition."""
    rng = np.random.default_rng(SEED + 53)
    records = []

    asset_classes = [
        ('cash_central_bank', 0, 200_000_000, 500_000_000),
        ('level_1_hqla', 0, 500_000_000, 1_500_000_000),
        ('level_2a_hqla', 0.15, 100_000_000, 400_000_000),
        ('level_2b_hqla', 0.50, 50_000_000, 150_000_000),
        ('other_liquid_assets', 0, 20_000_000, 80_000_000),
    ]

    for month in range(7, 13):
        report_date = date(2024, month, 28)
        for asset_class, haircut, val_min, val_max in asset_classes:
            nominal = round(float(rng.uniform(val_min, val_max)), 2)
            market_val = round(nominal * rng.uniform(0.95, 1.05), 2)
            adjusted = round(market_val * (1 - haircut), 2)

            records.append({
                'report_date': report_date.isoformat(),
                'asset_class': asset_class,
                'instrument_type': f'{asset_class} instruments',
                'nominal_value': nominal,
                'market_value': market_val,
                'haircut_pct': haircut * 100,
                'adjusted_value': adjusted,
                'currency': 'GBP',
            })

    cols = list(records[0].keys())
    bulk_insert('treasury.liquidity_pool', records, cols)
    print(f"  ✓ Liquidity Pool: {len(records)} generated")


def run():
    print("\n🏛️  Generating Treasury data...")
    generate_treasury_positions()
    generate_fx_rates()
    generate_interbank()
    generate_liquidity_pool()
    print("✅ Treasury data complete\n")


if __name__ == '__main__':
    run()
