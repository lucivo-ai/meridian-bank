"""
Generate customers and addresses for Meridian Community Bank.
~50,000 customers (85% personal, 15% business) with UK-realistic data.
"""
import numpy as np
from datetime import date, timedelta
from faker import Faker
from tqdm import tqdm
from generators.config import (
    SEED, CUSTOMER_COUNT, PERSONAL_RATIO, CUSTOMER_SEGMENTS,
    MISSING_POSTCODE_COUNT, AML_FLAG_RATIO
)
from generators.utils.relationships import bulk_insert, registry
from generators.utils.faker_extensions import (
    generate_ni_number, generate_uk_postcode, generate_uk_phone
)

fake = Faker('en_GB')
Faker.seed(SEED)


def generate_customers():
    """Generate customer master records."""
    rng = np.random.default_rng(SEED)
    n_personal = int(CUSTOMER_COUNT * PERSONAL_RATIO)
    n_business = CUSTOMER_COUNT - n_personal

    records = []
    customer_ids = []
    customer_types = []

    # Age distribution: UK-realistic
    # 18-25: 15%, 26-35: 25%, 36-50: 30%, 51-65: 20%, 66+: 10%
    age_weights = [0.15, 0.25, 0.30, 0.20, 0.10]
    age_ranges = [(18, 25), (26, 35), (36, 50), (51, 65), (66, 85)]

    # KYC status distribution
    kyc_statuses = ['verified', 'verified', 'verified', 'verified', 'verified',
                    'verified', 'verified', 'enhanced_due_diligence', 'pending', 'expired']

    # Risk rating (most are standard)
    risk_weights = np.array([0.25, 0.55, 0.12, 0.05, 0.02, 0.01])
    risk_ratings = ['low', 'standard', 'medium', 'high', 'pep', 'sanctioned']

    # Segments for personal
    personal_segments = ['mass_market', 'mass_affluent', 'high_net_worth',
                        'young_professional', 'student', 'retired']
    personal_seg_weights = np.array([0.45, 0.25, 0.05, 0.12, 0.05, 0.08])

    print(f"  Generating {n_personal} personal customers...")
    for i in tqdm(range(n_personal), desc="  Personal customers", leave=False):
        # Age
        age_bracket = rng.choice(len(age_ranges), p=age_weights)
        age = rng.integers(age_ranges[age_bracket][0], age_ranges[age_bracket][1] + 1)
        dob = date(2024, 7, 1) - timedelta(days=int(age * 365 + rng.integers(0, 365)))

        # Onboarding date: spread over last 10 years
        onboarded = date(2015, 1, 1) + timedelta(days=int(rng.integers(0, 3650)))
        if onboarded > date(2024, 12, 31):
            onboarded = date(2024, 12, 31)

        gender = rng.choice(['male', 'female'])
        if gender == 'male':
            first = fake.first_name_male()
            title = rng.choice(['Mr', 'Mr', 'Mr', 'Dr'])
        else:
            first = fake.first_name_female()
            title = rng.choice(['Ms', 'Mrs', 'Miss', 'Dr'])
        last = fake.last_name()

        risk = rng.choice(risk_ratings, p=risk_weights)
        kyc = rng.choice(kyc_statuses)
        segment = rng.choice(personal_segments, p=personal_seg_weights)

        # ~8% of customers are inactive/closed
        is_active = rng.random() > 0.08
        closed_date = None
        if not is_active:
            closed_date = (onboarded + timedelta(days=int(rng.integers(180, 3000)))).isoformat()

        records.append({
            'customer_ref': f'MCB-{10000001 + i}',
            'type': 'personal',
            'title': title,
            'first_name': first,
            'last_name': last,
            'full_name': f'{title} {first} {last}',
            'date_of_birth': dob.isoformat(),
            'gender': gender,
            'nationality': 'British' if rng.random() > 0.15 else rng.choice([
                'Irish', 'Polish', 'Indian', 'Pakistani', 'Nigerian', 'Romanian',
                'Italian', 'Portuguese', 'French', 'German'
            ]),
            'ni_number': generate_ni_number(rng),
            'email': f'{first.lower()}.{last.lower()}{rng.integers(1,999)}@{fake.free_email_domain()}',
            'phone_mobile': generate_uk_phone(),
            'phone_home': generate_uk_phone() if rng.random() > 0.6 else None,
            'company_name': None,
            'company_reg_number': None,
            'sic_code': None,
            'kyc_status': kyc,
            'kyc_verified_date': (onboarded + timedelta(days=int(rng.integers(1, 30)))).isoformat() if kyc == 'verified' else None,
            'risk_rating': risk,
            'customer_segment': segment,
            'is_active': is_active,
            'onboarded_date': onboarded.isoformat(),
            'closed_date': closed_date,
        })

    # Business segments
    business_segments = ['small_business', 'growing_business']
    business_seg_weights = np.array([0.70, 0.30])
    sic_codes = ['62020', '47110', '56101', '41201', '69201', '86210',
                 '96020', '55100', '49410', '01110', '74909', '82990']

    print(f"  Generating {n_business} business customers...")
    for i in tqdm(range(n_business), desc="  Business customers", leave=False):
        company = fake.company()
        onboarded = date(2015, 1, 1) + timedelta(days=int(rng.integers(0, 3650)))
        if onboarded > date(2024, 12, 31):
            onboarded = date(2024, 12, 31)

        risk = rng.choice(risk_ratings[:4], p=np.array([0.20, 0.50, 0.20, 0.10]))
        is_active = rng.random() > 0.10

        records.append({
            'customer_ref': f'MCB-{10000001 + n_personal + i}',
            'type': 'business',
            'title': None,
            'first_name': None,
            'last_name': None,
            'full_name': company,
            'date_of_birth': None,
            'gender': None,
            'nationality': None,
            'ni_number': None,
            'email': f'info@{company.lower().replace(" ", "").replace(",","").replace("&","")[:20]}.co.uk',
            'phone_mobile': generate_uk_phone(),
            'phone_home': generate_uk_phone() if rng.random() > 0.4 else None,
            'company_name': company,
            'company_reg_number': f'{rng.integers(1000000, 99999999):08d}',
            'sic_code': rng.choice(sic_codes),
            'kyc_status': rng.choice(['verified', 'verified', 'verified', 'enhanced_due_diligence']),
            'kyc_verified_date': (onboarded + timedelta(days=int(rng.integers(1, 30)))).isoformat(),
            'risk_rating': risk,
            'customer_segment': rng.choice(business_segments, p=business_seg_weights),
            'is_active': is_active,
            'onboarded_date': onboarded.isoformat(),
            'closed_date': None if is_active else (onboarded + timedelta(days=int(rng.integers(180, 2000)))).isoformat(),
        })

    cols = list(records[0].keys())
    print(f"  Inserting {len(records)} customers...")
    bulk_insert('core_banking.customers', records, cols)

    # Retrieve IDs
    from sqlalchemy import text
    engine = registry._store  # hacky but works
    from generators.utils.relationships import get_engine
    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT customer_id, type, is_active FROM core_banking.customers ORDER BY customer_id"
        )).fetchall()

    all_ids = [r[0] for r in rows]
    personal_ids = [r[0] for r in rows if r[1] == 'personal']
    business_ids = [r[0] for r in rows if r[1] == 'business']
    active_ids = [r[0] for r in rows if r[2]]

    registry.register('customer_ids', all_ids)
    registry.register('personal_customer_ids', personal_ids)
    registry.register('business_customer_ids', business_ids)
    registry.register('active_customer_ids', active_ids)

    print(f"  ✓ Customers: {len(all_ids)} total ({len(personal_ids)} personal, {len(business_ids)} business)")
    return all_ids


def generate_addresses():
    """Generate addresses for all customers."""
    rng = np.random.default_rng(SEED + 1)
    customer_ids = registry.get_ids('customer_ids')

    records = []
    # Track which customers get missing postcodes (for DQ testing)
    missing_postcode_customers = set(rng.choice(customer_ids, size=MISSING_POSTCODE_COUNT, replace=False))

    print(f"  Generating addresses for {len(customer_ids)} customers...")
    for cid in tqdm(customer_ids, desc="  Addresses", leave=False):
        # Primary address
        addr = {
            'customer_id': int(cid),
            'address_type': 'home',
            'line1': fake.street_address(),
            'line2': fake.secondary_address() if rng.random() > 0.6 else None,
            'line3': None,
            'city': fake.city(),
            'county': fake.county() if rng.random() > 0.3 else None,
            'postcode': None if cid in missing_postcode_customers else fake.postcode(),
            'country': 'United Kingdom',
            'is_primary': True,
            'valid_from': '2015-01-01',
            'valid_to': None,
        }
        records.append(addr)

        # ~30% have a correspondence address too
        if rng.random() > 0.7:
            records.append({
                **addr,
                'address_type': 'correspondence',
                'line1': fake.street_address(),
                'city': fake.city(),
                'postcode': fake.postcode(),
                'is_primary': False,
            })

    cols = ['customer_id', 'address_type', 'line1', 'line2', 'line3',
            'city', 'county', 'postcode', 'country', 'is_primary', 'valid_from', 'valid_to']
    bulk_insert('core_banking.addresses', records, cols)
    print(f"  ✓ Addresses: {len(records)} inserted ({MISSING_POSTCODE_COUNT} with missing postcode)")


def run():
    """Generate all customer data."""
    print("\n👥 Generating customer data...")
    generate_customers()
    generate_addresses()
    print("✅ Customer data complete\n")


if __name__ == '__main__':
    run()
