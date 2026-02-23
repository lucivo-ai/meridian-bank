"""
Generate CRM data: contacts, interactions, complaints, marketing consents, segments.
"""
import numpy as np
from datetime import date, timedelta
from faker import Faker
from tqdm import tqdm
from generators.config import SEED, COMPLAINT_RATIO, CUSTOMER_SEGMENTS
from generators.utils.relationships import bulk_insert, registry, get_engine
from generators.utils.faker_extensions import generate_uk_phone

fake = Faker('en_GB')
Faker.seed(SEED)


def generate_contacts():
    """Generate CRM contact records linked to customers."""
    rng = np.random.default_rng(SEED + 60)
    engine = get_engine()

    from sqlalchemy import text
    with engine.connect() as conn:
        customers = conn.execute(text(
            "SELECT customer_id, full_name, email, phone_mobile FROM core_banking.customers ORDER BY customer_id"
        )).fetchall()

    records = []
    branches = ['London City', 'London West End', 'Manchester', 'Birmingham',
                'Edinburgh', 'Bristol', 'Leeds', 'Cardiff', 'Digital Hub']
    channels = ['email', 'phone', 'sms', 'post', 'app']
    rm_names = [f'RM-{i:03d}' for i in range(1, 30)]

    for cid, name, email, phone in tqdm(customers, desc="  CRM contacts", leave=False):
        records.append({
            'customer_id': cid,
            'contact_name': name,
            'email_primary': email,
            'email_secondary': fake.email() if rng.random() > 0.8 else None,
            'phone_primary': phone,
            'phone_secondary': generate_uk_phone() if rng.random() > 0.7 else None,
            'preferred_channel': rng.choice(channels, p=[0.35, 0.20, 0.15, 0.05, 0.25]),
            'language_pref': 'en',
            'relationship_manager': rng.choice(rm_names),
            'assigned_branch': rng.choice(branches),
        })

    cols = ['customer_id', 'contact_name', 'email_primary', 'email_secondary',
            'phone_primary', 'phone_secondary', 'preferred_channel', 'language_pref',
            'relationship_manager', 'assigned_branch']
    bulk_insert('crm.contacts', records, cols)

    # Register contact IDs
    with engine.connect() as conn:
        contact_rows = conn.execute(text("SELECT contact_id, customer_id FROM crm.contacts")).fetchall()
    contact_map = {r[1]: r[0] for r in contact_rows}
    registry.register('contact_map', [contact_map])
    print(f"  ✓ CRM Contacts: {len(records)} generated")


def generate_interactions():
    """Generate customer interaction history."""
    rng = np.random.default_rng(SEED + 61)
    engine = get_engine()

    from sqlalchemy import text
    with engine.connect() as conn:
        contacts = conn.execute(text(
            "SELECT contact_id, customer_id FROM crm.contacts"
        )).fetchall()

    channels = ['phone_inbound', 'phone_outbound', 'email_inbound', 'email_outbound',
                'branch_visit', 'webchat', 'app_message', 'letter']
    chan_weights = np.array([0.20, 0.10, 0.15, 0.10, 0.08, 0.15, 0.17, 0.05])
    categories = ['enquiry', 'service_request', 'product_enquiry', 'account_maintenance',
                  'complaint', 'feedback', 'outbound_campaign']
    cat_weights = np.array([0.30, 0.20, 0.15, 0.15, 0.05, 0.05, 0.10])

    records = []
    # Average 4 interactions per customer
    for contact_id, customer_id in tqdm(contacts, desc="  Interactions", leave=False):
        n = max(0, int(rng.poisson(4)))
        for _ in range(n):
            int_date = date(2024, 1, 1) + timedelta(days=int(rng.integers(0, 365)))
            records.append({
                'contact_id': contact_id,
                'customer_id': customer_id,
                'interaction_date': f'{int_date.isoformat()} {rng.integers(8,18):02d}:{rng.integers(0,59):02d}:00',
                'channel': rng.choice(channels, p=chan_weights),
                'category': rng.choice(categories, p=cat_weights),
                'subject': fake.sentence(nb_words=6),
                'resolved': bool(rng.random() > 0.15),
                'handled_by': f'AGENT-{rng.integers(1, 50):03d}',
                'duration_seconds': int(rng.lognormal(5, 0.8)) if rng.random() > 0.3 else None,
                'sentiment_score': round(float(rng.uniform(-0.5, 1.0)), 2),
            })

    cols = ['contact_id', 'customer_id', 'interaction_date', 'channel', 'category',
            'subject', 'resolved', 'handled_by', 'duration_seconds', 'sentiment_score']

    # Batch insert
    for i in range(0, len(records), 50000):
        bulk_insert('crm.interactions', records[i:i+50000], cols)

    print(f"  ✓ CRM Interactions: {len(records):,} generated")


def generate_complaints():
    """Generate formal complaints."""
    rng = np.random.default_rng(SEED + 62)
    customer_ids = registry.get_ids('active_customer_ids')
    n_complaints = int(len(customer_ids) * COMPLAINT_RATIO)
    complainants = rng.choice(customer_ids, size=n_complaints, replace=True)

    categories = ['charges_fees', 'service_quality', 'product_mis_sell', 'fraud',
                  'payment_issue', 'lending_decision', 'other']
    cat_weights = np.array([0.25, 0.20, 0.10, 0.10, 0.15, 0.10, 0.10])

    records = []
    for cid in complainants:
        comp_date = date(2024, 1, 1) + timedelta(days=int(rng.integers(0, 365)))
        status = rng.choice(['open', 'investigating', 'resolved', 'closed', 'referred_to_fos'],
                             p=[0.10, 0.15, 0.40, 0.30, 0.05])
        resolved = status in ('resolved', 'closed', 'referred_to_fos')

        records.append({
            'customer_id': int(cid),
            'complaint_date': comp_date.isoformat(),
            'category': rng.choice(categories, p=cat_weights),
            'severity': rng.choice(['low', 'medium', 'high', 'critical'], p=[0.30, 0.40, 0.25, 0.05]),
            'description': fake.paragraph(nb_sentences=2),
            'root_cause': rng.choice(['process_failure', 'system_error', 'staff_error', 'policy_gap', None]),
            'status': status,
            'resolution_date': (comp_date + timedelta(days=int(rng.integers(1, 60)))).isoformat() if resolved else None,
            'resolution_notes': fake.sentence() if resolved else None,
            'compensation_amount': round(float(rng.lognormal(3, 1)), 2) if resolved and rng.random() > 0.6 else 0,
            'fos_referral': status == 'referred_to_fos',
            'assigned_to': f'COMP-{rng.integers(1, 15):03d}',
        })

    cols = list(records[0].keys())
    bulk_insert('crm.complaints', records, cols)
    print(f"  ✓ Complaints: {len(records)} generated")


def generate_marketing_consents():
    """Generate GDPR consent records."""
    rng = np.random.default_rng(SEED + 63)
    customer_ids = registry.get_ids('customer_ids')

    consent_types = ['email_marketing', 'sms_marketing', 'phone_marketing',
                     'post_marketing', 'third_party_sharing', 'profiling', 'analytics']

    records = []
    for cid in tqdm(customer_ids, desc="  Consents", leave=False):
        for ct in consent_types:
            consented = bool(rng.random() > 0.35)
            consent_date = date(2018, 5, 25) + timedelta(days=int(rng.integers(0, 2400)))  # Post-GDPR

            records.append({
                'customer_id': int(cid),
                'consent_type': ct,
                'is_consented': consented,
                'consent_date': f'{consent_date.isoformat()} 12:00:00',
                'withdrawal_date': (consent_date + timedelta(days=int(rng.integers(30, 730)))).isoformat() + ' 12:00:00' if not consented and rng.random() > 0.5 else None,
                'consent_source': rng.choice(['onboarding', 'online_update', 'branch', 'campaign_response']),
                'lawful_basis': 'consent' if consented else rng.choice(['consent', 'legitimate_interest']),
            })

    cols = list(records[0].keys())
    for i in range(0, len(records), 50000):
        bulk_insert('crm.marketing_consents', records[i:i+50000], cols)
    print(f"  ✓ Marketing Consents: {len(records):,} generated")


def generate_segments():
    """Generate customer segmentation assignments."""
    rng = np.random.default_rng(SEED + 64)
    engine = get_engine()

    from sqlalchemy import text
    with engine.connect() as conn:
        customers = conn.execute(text(
            "SELECT customer_id, customer_segment FROM core_banking.customers WHERE is_active = TRUE"
        )).fetchall()

    segment_names = {
        'mass_market': 'Mass Market',
        'mass_affluent': 'Mass Affluent',
        'high_net_worth': 'High Net Worth',
        'young_professional': 'Young Professional',
        'student': 'Student',
        'retired': 'Retired',
        'small_business': 'Small Business',
        'growing_business': 'Growing Business',
    }

    records = []
    for cid, seg in customers:
        if seg:
            records.append({
                'customer_id': cid,
                'segment_code': seg,
                'segment_name': segment_names.get(seg, seg),
                'assigned_date': '2024-01-15',
                'score': round(float(rng.uniform(0, 100)), 2),
                'is_current': True,
                'model_version': 'SEG_V2.1',
            })

    cols = list(records[0].keys())
    bulk_insert('crm.segments', records, cols)
    print(f"  ✓ Segments: {len(records)} generated")


def run():
    print("\n📞 Generating CRM data...")
    generate_contacts()
    generate_interactions()
    generate_complaints()
    generate_marketing_consents()
    generate_segments()
    print("✅ CRM data complete\n")


if __name__ == '__main__':
    run()
