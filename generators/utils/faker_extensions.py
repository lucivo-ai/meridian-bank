"""
UK-specific fake data generators for Meridian Community Bank.
Generates realistic UK sort codes, account numbers, NI numbers, and postcodes.
"""
import numpy as np
from faker import Faker

fake = Faker('en_GB')

# Meridian sort code prefixes (fictional but realistic format)
MERIDIAN_SORT_CODES = [
    '200100', '200101', '200102', '200103', '200104',  # London
    '200200', '200201',  # Manchester
    '200300', '200301',  # Birmingham
    '200400',  # Edinburgh
    '200500',  # Bristol
    '200600',  # Leeds
    '200700',  # Cardiff
    '200800',  # Digital
]


def generate_sort_code(rng: np.random.Generator) -> str:
    """Generate a Meridian sort code."""
    return rng.choice(MERIDIAN_SORT_CODES)


def generate_account_number(rng: np.random.Generator) -> str:
    """Generate an 8-digit UK account number."""
    return str(rng.integers(10000000, 99999999))


def generate_ni_number(rng: np.random.Generator) -> str:
    """Generate a UK National Insurance number (format: AB123456C)."""
    prefix_letters = 'ABCEGHJKLMNPRSTWXYZ'
    suffix_letters = 'ABCD'
    p1 = rng.choice(list(prefix_letters))
    p2 = rng.choice(list(prefix_letters))
    digits = str(rng.integers(100000, 999999))
    suffix = rng.choice(list(suffix_letters))
    return f"{p1}{p2}{digits}{suffix}"


def generate_uk_postcode() -> str:
    """Generate a realistic UK postcode using Faker."""
    return fake.postcode()


def generate_uk_phone() -> str:
    """Generate a UK mobile number."""
    return fake.phone_number()


def generate_uk_address() -> dict:
    """Generate a complete UK address."""
    return {
        'line1': fake.street_address(),
        'line2': fake.secondary_address() if np.random.random() > 0.6 else None,
        'city': fake.city(),
        'county': fake.county() if np.random.random() > 0.3 else None,
        'postcode': fake.postcode(),
        'country': 'United Kingdom',
    }


# ── Counterparty names for transactions ──────────────────────
RETAIL_COUNTERPARTIES = [
    'Tesco Stores', 'Sainsbury\'s', 'ASDA', 'Morrisons', 'Aldi', 'Lidl',
    'Marks & Spencer', 'Waitrose', 'Co-op Food',
    'Amazon.co.uk', 'ASOS.com', 'John Lewis', 'Argos', 'Currys',
    'Shell', 'BP', 'Esso', 'Texaco',
    'Costa Coffee', 'Starbucks', 'Greggs', 'Pret A Manger', 'Nandos',
    'Deliveroo', 'Just Eat', 'Uber Eats',
    'Netflix', 'Spotify', 'Apple', 'Google',
    'Sky', 'BT', 'Virgin Media', 'EE', 'Three', 'Vodafone',
    'British Gas', 'EDF Energy', 'SSE', 'Octopus Energy',
    'Thames Water', 'Severn Trent', 'United Utilities',
    'Council Tax', 'HMRC', 'DVLA',
    'Aviva', 'Direct Line', 'Admiral', 'AA',
    'PureGym', 'David Lloyd', 'Gymshark',
    'TfL', 'Trainline', 'National Rail',
    'NHS', 'Boots Pharmacy',
]

BUSINESS_COUNTERPARTIES = [
    'HMRC VAT', 'HMRC Corporation Tax', 'HMRC PAYE',
    'Companies House', 'Business Rates',
    'AWS', 'Microsoft Azure', 'Google Cloud',
    'Royal Mail', 'DHL', 'FedEx',
    'Sage Accounting', 'Xero', 'QuickBooks',
    'Barclaycard Merchant', 'Worldpay', 'Stripe',
]

SALARY_PAYERS = [
    'Meridian Bank Payroll', 'Tesco PLC Payroll', 'NHS Trust Payroll',
    'Barclays Payroll', 'BT Group Payroll', 'Unilever Payroll',
    'GSK Payroll', 'Rolls Royce Payroll', 'BAE Systems Payroll',
    'Lloyds Banking Payroll', 'JCB Payroll', 'Astra Zeneca Payroll',
    'Royal Mail Payroll', 'Network Rail Payroll', 'BBC Payroll',
]


def get_counterparty(txn_type: str, is_business: bool, rng: np.random.Generator) -> str:
    """Get a realistic counterparty name based on transaction type."""
    if txn_type == 'salary':
        return rng.choice(SALARY_PAYERS)
    elif is_business:
        pool = BUSINESS_COUNTERPARTIES + RETAIL_COUNTERPARTIES[:10]
    else:
        pool = RETAIL_COUNTERPARTIES
    return rng.choice(pool)
