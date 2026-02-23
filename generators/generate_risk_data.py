"""
Generate risk and compliance data:
- Credit scores, applications, AML alerts/cases, sanctions screening, risk assessments
"""
import numpy as np
import json
from datetime import date, timedelta
from tqdm import tqdm
from sqlalchemy import text
from generators.config import SEED, AML_FLAG_RATIO, CUSTOMER_COUNT
from generators.utils.relationships import bulk_insert, registry, get_engine


def generate_credit_scores():
    """Generate credit scores for all active customers."""
    rng = np.random.default_rng(SEED + 30)
    customer_ids = registry.get_ids('active_customer_ids')

    records = []
    score_bands = {
        (0, 299): 'very_poor',
        (300, 499): 'poor',
        (500, 649): 'fair',
        (650, 799): 'good',
        (800, 999): 'excellent',
    }

    for cid in tqdm(customer_ids, desc="  Credit scores", leave=False):
        # Score follows normal distribution centered on 650
        score = int(rng.normal(650, 150))
        score = max(0, min(999, score))

        band = 'fair'
        for (lo, hi), b in score_bands.items():
            if lo <= score <= hi:
                band = b
                break

        factors = []
        if score < 500:
            factors = ['missed_payments', 'high_utilisation', 'short_credit_history']
        elif score < 700:
            factors = ['moderate_utilisation', 'limited_credit_mix']
        else:
            factors = ['low_utilisation', 'long_credit_history', 'diverse_credit_mix']

        records.append({
            'customer_id': int(cid),
            'score_date': '2024-12-01',
            'score_value': score,
            'score_band': band,
            'model_name': 'MCB_SCORE_V3',
            'model_version': '3.2.1',
            'factors': json.dumps(factors),
            'is_current': True,
        })

    cols = ['customer_id', 'score_date', 'score_value', 'score_band',
            'model_name', 'model_version', 'factors', 'is_current']
    bulk_insert('risk.credit_scores', records, cols)
    print(f"  ✓ Credit Scores: {len(records)} generated")


def generate_credit_applications():
    """Generate loan/mortgage applications."""
    rng = np.random.default_rng(SEED + 31)
    customer_ids = registry.get_ids('active_customer_ids')

    # ~30% of customers have applied for credit
    applicants = rng.choice(customer_ids, size=int(len(customer_ids) * 0.30), replace=False)

    engine = get_engine()
    with engine.connect() as conn:
        products = conn.execute(text(
            "SELECT product_id FROM core_banking.products WHERE category IN ('personal_loan', 'mortgage', 'business_loan')"
        )).fetchall()
    loan_product_ids = [r[0] for r in products]

    records = []
    decisions = ['approved', 'approved', 'approved', 'declined', 'declined', 'referred', 'withdrawn']
    purposes = ['home_improvement', 'car_purchase', 'debt_consolidation', 'business_expansion',
                'property_purchase', 'education', 'medical', 'other']
    employment = ['employed', 'self_employed', 'retired', 'student', 'unemployed']

    for cid in tqdm(applicants, desc="  Credit applications", leave=False):
        app_date = date(2024, 1, 1) + timedelta(days=int(rng.integers(0, 365)))
        amount = round(float(rng.lognormal(9.0, 1.2)), 2)  # ~£8k median
        amount = min(amount, 2000000)

        decision = rng.choice(decisions)
        approved_amount = round(amount * rng.uniform(0.7, 1.0), 2) if decision == 'approved' else None

        records.append({
            'customer_id': int(cid),
            'product_id': int(rng.choice(loan_product_ids)),
            'application_date': app_date.isoformat(),
            'requested_amount': amount,
            'approved_amount': approved_amount,
            'term_months': int(rng.choice([12, 24, 36, 48, 60, 120, 180, 240, 300])),
            'interest_rate': round(float(rng.uniform(0.03, 0.15)), 4),
            'purpose': rng.choice(purposes),
            'employment_status': rng.choice(employment, p=[0.65, 0.15, 0.10, 0.05, 0.05]),
            'annual_income': round(float(rng.lognormal(10.3, 0.6)), 2),
            'credit_score_at_application': int(rng.normal(650, 150)),
            'decision': decision,
            'decision_date': (app_date + timedelta(days=int(rng.integers(1, 14)))).isoformat(),
            'decision_reason': 'Automated approval' if decision == 'approved' else 'Policy criteria not met',
            'affordability_ratio': round(float(rng.uniform(0.15, 0.55)), 2),
            'underwriter': f'UW-{rng.integers(1, 20):03d}',
        })

    cols = list(records[0].keys())
    bulk_insert('risk.credit_applications', records, cols)
    print(f"  ✓ Credit Applications: {len(records)} generated")


def generate_aml_alerts():
    """Generate AML transaction monitoring alerts."""
    rng = np.random.default_rng(SEED + 32)
    customer_ids = registry.get_ids('active_customer_ids')

    # ~2% flagged + some random alerts
    n_alerts = int(CUSTOMER_COUNT * AML_FLAG_RATIO * 5)  # Multiple alerts per flagged customer
    flagged_customers = rng.choice(customer_ids, size=int(CUSTOMER_COUNT * AML_FLAG_RATIO), replace=False)

    alert_types = ['unusual_activity', 'large_cash', 'structuring', 'rapid_movement',
                   'high_risk_jurisdiction', 'sanctions_hit', 'pep_activity', 'dormant_reactivation']
    alert_weights = np.array([0.30, 0.20, 0.15, 0.10, 0.08, 0.05, 0.07, 0.05])
    statuses = ['open', 'investigating', 'escalated', 'sar_filed', 'false_positive', 'closed']
    status_weights = np.array([0.10, 0.15, 0.05, 0.05, 0.35, 0.30])

    records = []
    for i in range(n_alerts):
        cid = int(rng.choice(flagged_customers))
        alert_date = date(2024, 7, 1) + timedelta(days=int(rng.integers(0, 183)))
        status = rng.choice(statuses, p=status_weights)

        records.append({
            'customer_id': cid,
            'alert_date': f'{alert_date.isoformat()} {rng.integers(8,18):02d}:{rng.integers(0,59):02d}:00',
            'alert_type': rng.choice(alert_types, p=alert_weights),
            'rule_id': f'AML-R{rng.integers(1, 50):03d}',
            'rule_name': f'Rule {rng.integers(1, 50)}',
            'trigger_amount': round(float(rng.lognormal(8, 1.5)), 2),
            'trigger_details': 'Automated alert from transaction monitoring system',
            'risk_score': round(float(rng.uniform(10, 95)), 2),
            'status': status,
            'assigned_to': f'MLRO-{rng.integers(1, 8):03d}',
            'resolution_date': (alert_date + timedelta(days=int(rng.integers(1, 60)))).isoformat() + ' 17:00:00' if status in ('false_positive', 'closed', 'sar_filed') else None,
            'resolution_notes': 'Reviewed and resolved' if status in ('false_positive', 'closed') else None,
        })

    cols = ['customer_id', 'alert_date', 'alert_type', 'rule_id', 'rule_name',
            'trigger_amount', 'trigger_details', 'risk_score', 'status',
            'assigned_to', 'resolution_date', 'resolution_notes']
    bulk_insert('risk.aml_alerts', records, cols)
    print(f"  ✓ AML Alerts: {len(records)} generated")
    return flagged_customers


def generate_aml_cases(flagged_customers):
    """Generate AML investigation cases."""
    rng = np.random.default_rng(SEED + 33)

    # ~10% of flagged customers get a formal case
    n_cases = max(50, int(len(flagged_customers) * 0.5))
    case_customers = rng.choice(flagged_customers, size=min(n_cases, len(flagged_customers)), replace=False)

    records = []
    for i, cid in enumerate(case_customers):
        opened = date(2024, 7, 1) + timedelta(days=int(rng.integers(0, 183)))
        status = rng.choice(
            ['open', 'investigating', 'pending_sar', 'sar_filed', 'closed_no_action', 'closed_action_taken'],
            p=[0.15, 0.20, 0.05, 0.10, 0.35, 0.15]
        )

        records.append({
            'case_ref': f'AML-2024-{i+1:04d}',
            'customer_id': int(cid),
            'opened_date': opened.isoformat(),
            'case_type': rng.choice(['investigation', 'enhanced_monitoring', 'sar', 'referral'],
                                     p=[0.50, 0.25, 0.15, 0.10]),
            'priority': rng.choice(['low', 'medium', 'high', 'critical'], p=[0.20, 0.40, 0.30, 0.10]),
            'description': 'Suspicious transaction pattern identified by monitoring system',
            'total_suspicious_amount': round(float(rng.lognormal(9, 1.5)), 2),
            'status': status,
            'sar_reference': f'SAR-2024-{rng.integers(1000, 9999)}' if status in ('sar_filed', 'pending_sar') else None,
            'assigned_to': f'MLRO-{rng.integers(1, 5):03d}',
            'closed_date': (opened + timedelta(days=int(rng.integers(14, 90)))).isoformat() if 'closed' in status else None,
            'outcome_notes': 'Case resolved' if 'closed' in status else None,
        })

    cols = list(records[0].keys())
    bulk_insert('risk.aml_cases', records, cols)
    print(f"  ✓ AML Cases: {len(records)} generated")


def generate_sanctions_screening():
    """Generate sanctions screening records for all active customers."""
    rng = np.random.default_rng(SEED + 34)
    customer_ids = registry.get_ids('active_customer_ids')

    lists_checked = ['OFSI', 'EU Sanctions', 'UN Sanctions', 'OFAC SDN']
    records = []

    for cid in tqdm(customer_ids, desc="  Sanctions screening", leave=False):
        screen_date = date(2024, 1, 1) + timedelta(days=int(rng.integers(0, 365)))
        match_found = rng.random() < 0.003  # 0.3% match rate

        records.append({
            'customer_id': int(cid),
            'screening_date': f'{screen_date.isoformat()} 02:00:00',
            'screening_type': rng.choice(['periodic', 'batch', 'onboarding'], p=[0.60, 0.30, 0.10]),
            'list_checked': rng.choice(lists_checked),
            'match_found': match_found,
            'match_score': round(float(rng.uniform(70, 99)), 2) if match_found else None,
            'match_details': 'Potential name match — requires manual review' if match_found else None,
            'status': rng.choice(['potential_match', 'false_positive'], p=[0.3, 0.7]) if match_found else 'clear',
            'reviewed_by': f'COMP-{rng.integers(1, 10):03d}' if match_found else None,
            'review_date': (screen_date + timedelta(days=int(rng.integers(1, 5)))).isoformat() + ' 14:00:00' if match_found else None,
        })

    cols = list(records[0].keys())
    bulk_insert('risk.sanctions_screening', records, cols)
    print(f"  ✓ Sanctions Screening: {len(records)} records")


def generate_risk_assessments():
    """Generate CDD/EDD risk assessments."""
    rng = np.random.default_rng(SEED + 35)
    customer_ids = registry.get_ids('active_customer_ids')

    records = []
    risk_levels = ['low', 'medium', 'high', 'very_high']
    risk_weights = np.array([0.35, 0.40, 0.20, 0.05])

    for cid in tqdm(customer_ids, desc="  Risk assessments", leave=False):
        assess_date = date(2024, 1, 1) + timedelta(days=int(rng.integers(0, 365)))
        overall = rng.choice(risk_levels, p=risk_weights)
        is_edd = overall in ('high', 'very_high')

        records.append({
            'customer_id': int(cid),
            'assessment_date': assess_date.isoformat(),
            'assessment_type': 'enhanced_edd' if is_edd else 'standard_cdd',
            'overall_risk': overall,
            'country_risk': rng.choice(risk_levels[:3], p=[0.60, 0.30, 0.10]),
            'product_risk': rng.choice(risk_levels[:3], p=[0.50, 0.35, 0.15]),
            'channel_risk': rng.choice(['low', 'medium'], p=[0.70, 0.30]),
            'occupation_risk': rng.choice(risk_levels[:3], p=[0.55, 0.30, 0.15]),
            'source_of_funds': rng.choice(['employment', 'business', 'investments', 'inheritance', 'pension']),
            'source_of_wealth': rng.choice(['salary', 'business_profits', 'property', 'investments', 'inheritance']),
            'pep_status': bool(rng.random() < 0.02),
            'adverse_media': bool(rng.random() < 0.01),
            'next_review_date': (assess_date + timedelta(days=365 if not is_edd else 180)).isoformat(),
            'assessed_by': f'COMP-{rng.integers(1, 10):03d}',
            'notes': None,
        })

    cols = list(records[0].keys())
    bulk_insert('risk.risk_assessments', records, cols)
    print(f"  ✓ Risk Assessments: {len(records)} generated")


def generate_regulatory_reports():
    """Generate regulatory report metadata."""
    rng = np.random.default_rng(SEED + 36)

    report_defs = [
        ('COREP', 'Common Reporting — Own Funds', 'PRA', 'quarterly'),
        ('FINREP', 'Financial Reporting', 'PRA', 'quarterly'),
        ('LCR', 'Liquidity Coverage Ratio', 'PRA', 'monthly'),
        ('NSFR', 'Net Stable Funding Ratio', 'PRA', 'quarterly'),
        ('ALMM', 'Additional Liquidity Monitoring', 'PRA', 'monthly'),
        ('MLAR', 'Mortgage Lending & Admin Return', 'FCA', 'quarterly'),
        ('CCR', 'Client Money & Assets Report', 'FCA', 'monthly'),
        ('STR', 'Suspicious Transaction Report', 'FCA', 'ad_hoc'),
        ('GABRIEL', 'FCA Regulatory Returns', 'FCA', 'quarterly'),
        ('BOESTAT', 'Bank of England Statistical Returns', 'BoE', 'monthly'),
    ]

    records = []
    for report_code, report_name, regulator, freq in report_defs:
        # Generate reports for last 12 months
        if freq == 'monthly':
            periods = [date(2024, m, 1) for m in range(1, 13)]
        elif freq == 'quarterly':
            periods = [date(2024, m, 1) for m in [3, 6, 9, 12]]
        elif freq == 'ad_hoc':
            n = rng.integers(2, 8)
            periods = [date(2024, 1, 1) + timedelta(days=int(rng.integers(0, 365))) for _ in range(n)]
        else:
            periods = [date(2024, 12, 31)]

        for period in periods:
            deadline = period + timedelta(days=int(rng.integers(20, 45)))
            submitted = rng.random() > 0.05  # 95% submitted
            status = 'submitted' if submitted else rng.choice(['draft', 'in_review'])

            records.append({
                'report_code': report_code,
                'report_name': report_name,
                'regulator': regulator,
                'frequency': freq,
                'reporting_period': period.isoformat(),
                'submission_deadline': deadline.isoformat(),
                'actual_submission': (deadline - timedelta(days=int(rng.integers(1, 10)))).isoformat() + ' 16:00:00' if submitted else None,
                'status': status,
                'prepared_by': f'FIN-{rng.integers(1, 5):03d}',
                'approved_by': 'David Okonkwo' if submitted else None,
                'notes': None,
            })

    cols = list(records[0].keys())
    bulk_insert('risk.regulatory_reports', records, cols)
    print(f"  ✓ Regulatory Reports: {len(records)} generated")


def run():
    """Generate all risk and compliance data."""
    print("\n🛡️  Generating risk & compliance data...")
    generate_credit_scores()
    generate_credit_applications()
    flagged = generate_aml_alerts()
    generate_aml_cases(flagged)
    generate_sanctions_screening()
    generate_risk_assessments()
    generate_regulatory_reports()
    print("✅ Risk & compliance data complete\n")


if __name__ == '__main__':
    run()
