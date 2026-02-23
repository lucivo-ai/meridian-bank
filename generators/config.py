"""
Meridian Community Bank — Data Generation Configuration
Central configuration for all generators. Loads from db_config.yaml with defaults.
"""
import os
import yaml
from datetime import date, datetime

# Load YAML config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config', 'db_config.yaml')

def load_config():
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)

_cfg = load_config()

# ── Database ──────────────────────────────────────────────────
DB_HOST = _cfg['database']['host']
DB_PORT = _cfg['database']['port']
DB_NAME = _cfg['database']['name']
DB_USER = _cfg['database']['user']
DB_PASS = _cfg['database']['password']
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ── Generation parameters ────────────────────────────────────
SEED = _cfg['generation']['seed']
CUSTOMER_COUNT = _cfg['generation']['customer_count']
TRANSACTION_MONTHS = _cfg['generation']['transaction_months']
TXN_DATE_START = date.fromisoformat(_cfg['generation']['transaction_date_start'])
TXN_DATE_END = date.fromisoformat(_cfg['generation']['transaction_date_end'])
AVG_TXN_PER_ACCOUNT_MONTH = _cfg['generation']['avg_transactions_per_account_per_month']
WAREHOUSE_BATCH_DATE = date.fromisoformat(_cfg['generation']['warehouse_batch_date'])

# ── Distribution controls ────────────────────────────────────
PERSONAL_RATIO = _cfg['generation']['personal_vs_business_ratio']
ACTIVE_ACCOUNT_RATIO = _cfg['generation']['active_account_ratio']
ARREARS_RATIO = _cfg['generation']['arrears_ratio']
AML_FLAG_RATIO = _cfg['generation']['aml_flag_ratio']
COMPLAINT_RATIO = _cfg['generation']['complaint_ratio']

# ── Intentional data quality issues ──────────────────────────
MISSING_POSTCODE_COUNT = _cfg['generation']['missing_postcode_count']
ZERO_AMOUNT_TXNS = _cfg['generation']['zero_amount_transactions']
ORPHANED_ACCOUNTS = _cfg['generation']['orphaned_accounts']
STALE_TABLE_COUNT = _cfg['generation']['stale_table_count']
GL_IMBALANCE_BATCH = _cfg['generation']['gl_imbalance_batch']

# ── Derived counts ───────────────────────────────────────────
PERSONAL_CUSTOMERS = int(CUSTOMER_COUNT * PERSONAL_RATIO)
BUSINESS_CUSTOMERS = CUSTOMER_COUNT - PERSONAL_CUSTOMERS

# Accounts: personal avg 1.7, business avg 2.5
ESTIMATED_ACCOUNTS = int(PERSONAL_CUSTOMERS * 1.7 + BUSINESS_CUSTOMERS * 2.5)

# ── Batch sizes for bulk inserts ─────────────────────────────
BATCH_SIZE = 5000

# ── UK-specific constants ────────────────────────────────────
UK_REGIONS = [
    'London', 'South East', 'South West', 'East of England',
    'West Midlands', 'East Midlands', 'Yorkshire and the Humber',
    'North West', 'North East', 'Scotland', 'Wales', 'Northern Ireland'
]

CUSTOMER_SEGMENTS = [
    'mass_market', 'mass_affluent', 'high_net_worth',
    'young_professional', 'student', 'retired',
    'small_business', 'growing_business'
]

# ── Product definitions ──────────────────────────────────────
PRODUCTS = [
    # (code, name, category, rate, currency, min_balance, launched)
    ('CA-STD-001', 'Meridian Current Account', 'current_account', 0.0, 'GBP', 0, '2015-01-01'),
    ('CA-PRM-001', 'Meridian Premium Current', 'current_account', 0.005, 'GBP', 5000, '2018-03-01'),
    ('CA-STU-001', 'Student Current Account', 'current_account', 0.0, 'GBP', 0, '2016-09-01'),
    ('SA-ISA-001', 'Meridian Cash ISA', 'savings', 0.042, 'GBP', 1, '2015-01-01'),
    ('SA-EAS-001', 'Easy Saver', 'savings', 0.031, 'GBP', 1, '2015-01-01'),
    ('SA-FIX-001', 'Fixed Rate Saver 1yr', 'savings', 0.048, 'GBP', 1000, '2020-01-01'),
    ('SA-FIX-002', 'Fixed Rate Saver 2yr', 'savings', 0.051, 'GBP', 1000, '2020-01-01'),
    ('SA-NOT-001', 'Notice Saver 90 Day', 'savings', 0.044, 'GBP', 500, '2019-06-01'),
    ('PL-UNS-001', 'Personal Loan', 'personal_loan', 0.069, 'GBP', 0, '2015-01-01'),
    ('PL-UNS-002', 'Personal Loan Plus', 'personal_loan', 0.049, 'GBP', 0, '2020-01-01'),
    ('MG-RES-001', 'Residential Mortgage 2yr Fix', 'mortgage', 0.0449, 'GBP', 0, '2015-01-01'),
    ('MG-RES-002', 'Residential Mortgage 5yr Fix', 'mortgage', 0.0479, 'GBP', 0, '2015-01-01'),
    ('MG-BTL-001', 'Buy to Let Mortgage', 'mortgage', 0.0549, 'GBP', 0, '2017-01-01'),
    ('MG-RES-003', 'Tracker Mortgage', 'mortgage', 0.0429, 'GBP', 0, '2015-01-01'),
    ('CC-STD-001', 'Meridian Credit Card', 'credit_card', 0.199, 'GBP', 0, '2016-01-01'),
    ('CC-RWD-001', 'Rewards Credit Card', 'credit_card', 0.229, 'GBP', 0, '2019-01-01'),
    ('BC-STD-001', 'Business Current Account', 'business_current', 0.0, 'GBP', 0, '2015-01-01'),
    ('BC-PRM-001', 'Business Premium Account', 'business_current', 0.005, 'GBP', 10000, '2018-01-01'),
    ('BL-SME-001', 'SME Business Loan', 'business_loan', 0.079, 'GBP', 0, '2015-01-01'),
    ('BL-GRO-001', 'Growth Finance Loan', 'business_loan', 0.065, 'GBP', 0, '2021-01-01'),
    ('BS-SME-001', 'Business Savings Account', 'business_savings', 0.035, 'GBP', 1, '2015-01-01'),
]

# ── Chart of Accounts structure ──────────────────────────────
# (code, name, type, subtype, parent, level)
CHART_OF_ACCOUNTS = [
    # Level 0 — Top level
    ('1000', 'Assets', 'asset', None, None, 0),
    ('2000', 'Liabilities', 'liability', None, None, 0),
    ('3000', 'Equity', 'equity', None, None, 0),
    ('4000', 'Revenue', 'revenue', None, None, 0),
    ('5000', 'Expenses', 'expense', None, None, 0),
    # Level 1 — Asset breakdown
    ('1100', 'Cash and Balances', 'asset', 'cash', '1000', 1),
    ('1200', 'Loans and Advances', 'asset', 'loans', '1000', 1),
    ('1300', 'Investment Securities', 'asset', 'investments', '1000', 1),
    ('1400', 'Fixed Assets', 'asset', 'fixed', '1000', 1),
    ('1500', 'Other Assets', 'asset', 'other', '1000', 1),
    # Level 2 — Cash detail
    ('1110', 'Cash at Bank of England', 'asset', 'cash', '1100', 2),
    ('1120', 'Nostro Accounts', 'asset', 'cash', '1100', 2),
    ('1130', 'ATM Holdings', 'asset', 'cash', '1100', 2),
    # Level 2 — Loans detail
    ('1210', 'Personal Loans', 'asset', 'loans', '1200', 2),
    ('1220', 'Mortgages', 'asset', 'loans', '1200', 2),
    ('1230', 'Business Loans', 'asset', 'loans', '1200', 2),
    ('1240', 'Credit Card Receivables', 'asset', 'loans', '1200', 2),
    ('1250', 'Overdrafts', 'asset', 'loans', '1200', 2),
    ('1260', 'Loan Loss Provisions', 'asset', 'provisions', '1200', 2),
    # Level 2 — Investments
    ('1310', 'Government Bonds (Gilts)', 'asset', 'investments', '1300', 2),
    ('1320', 'Corporate Bonds', 'asset', 'investments', '1300', 2),
    ('1330', 'Money Market Instruments', 'asset', 'investments', '1300', 2),
    # Level 1 — Liability breakdown
    ('2100', 'Customer Deposits', 'liability', 'deposits', '2000', 1),
    ('2200', 'Wholesale Funding', 'liability', 'funding', '2000', 1),
    ('2300', 'Other Liabilities', 'liability', 'other', '2000', 1),
    # Level 2 — Deposits
    ('2110', 'Current Account Deposits', 'liability', 'deposits', '2100', 2),
    ('2120', 'Savings Deposits', 'liability', 'deposits', '2100', 2),
    ('2130', 'Fixed Term Deposits', 'liability', 'deposits', '2100', 2),
    ('2140', 'Business Deposits', 'liability', 'deposits', '2100', 2),
    # Level 2 — Funding
    ('2210', 'Interbank Borrowings', 'liability', 'funding', '2200', 2),
    ('2220', 'Repo Agreements', 'liability', 'funding', '2200', 2),
    # Level 1 — Equity
    ('3100', 'Share Capital', 'equity', None, '3000', 1),
    ('3200', 'Retained Earnings', 'equity', None, '3000', 1),
    ('3300', 'Reserves', 'equity', None, '3000', 1),
    # Level 1 — Revenue
    ('4100', 'Interest Income', 'revenue', 'interest', '4000', 1),
    ('4200', 'Fee and Commission Income', 'revenue', 'fees', '4000', 1),
    ('4300', 'Trading Income', 'revenue', 'trading', '4000', 1),
    ('4400', 'Other Income', 'revenue', 'other', '4000', 1),
    # Level 2 — Interest income detail
    ('4110', 'Interest on Loans', 'revenue', 'interest', '4100', 2),
    ('4120', 'Interest on Mortgages', 'revenue', 'interest', '4100', 2),
    ('4130', 'Interest on Investments', 'revenue', 'interest', '4100', 2),
    ('4140', 'Interest on Interbank', 'revenue', 'interest', '4100', 2),
    # Level 2 — Fee income detail
    ('4210', 'Account Fees', 'revenue', 'fees', '4200', 2),
    ('4220', 'Card Interchange Fees', 'revenue', 'fees', '4200', 2),
    ('4230', 'Payment Fees', 'revenue', 'fees', '4200', 2),
    ('4240', 'Lending Arrangement Fees', 'revenue', 'fees', '4200', 2),
    # Level 1 — Expenses
    ('5100', 'Interest Expense', 'expense', 'interest', '5000', 1),
    ('5200', 'Staff Costs', 'expense', 'staff', '5000', 1),
    ('5300', 'Premises and Equipment', 'expense', 'premises', '5000', 1),
    ('5400', 'Technology Costs', 'expense', 'technology', '5000', 1),
    ('5500', 'Marketing', 'expense', 'marketing', '5000', 1),
    ('5600', 'Regulatory and Compliance', 'expense', 'regulatory', '5000', 1),
    ('5700', 'Depreciation', 'expense', 'depreciation', '5000', 1),
    ('5800', 'Impairment Charges', 'expense', 'impairment', '5000', 1),
    ('5900', 'Other Operating Expenses', 'expense', 'other', '5000', 1),
    # Level 2 — Interest expense
    ('5110', 'Interest on Deposits', 'expense', 'interest', '5100', 2),
    ('5120', 'Interest on Wholesale Funding', 'expense', 'interest', '5100', 2),
    ('5130', 'Interest on Subordinated Debt', 'expense', 'interest', '5100', 2),
]

# ── Cost Centres ─────────────────────────────────────────────
COST_CENTRES = [
    ('CC-EXC', 'Executive Office', 'Executive', 'CEO'),
    ('CC-RET', 'Retail Banking', 'Retail', 'Head of Retail'),
    ('CC-BUS', 'Business Banking', 'Business', 'Head of Business'),
    ('CC-TRE', 'Treasury', 'Treasury', 'Head of Treasury'),
    ('CC-RIS', 'Risk Management', 'Risk', 'CRO'),
    ('CC-COM', 'Compliance', 'Compliance', 'Head of Compliance'),
    ('CC-FIN', 'Finance', 'Finance', 'CFO'),
    ('CC-HRM', 'Human Resources', 'HR', 'Head of HR'),
    ('CC-TEC', 'Technology', 'IT', 'CTO'),
    ('CC-OPS', 'Operations', 'Operations', 'COO'),
    ('CC-MKT', 'Marketing', 'Marketing', 'Head of Marketing'),
    ('CC-LEG', 'Legal', 'Legal', 'General Counsel'),
    ('CC-AUD', 'Internal Audit', 'Audit', 'Head of Audit'),
    ('CC-CRD', 'Credit Operations', 'Credit', 'Head of Credit'),
    ('CC-PAY', 'Payments', 'Payments', 'Head of Payments'),
    ('CC-BR1', 'London Branch', 'Branch', 'Branch Manager London'),
    ('CC-BR2', 'Manchester Branch', 'Branch', 'Branch Manager Manchester'),
    ('CC-BR3', 'Birmingham Branch', 'Branch', 'Branch Manager Birmingham'),
    ('CC-BR4', 'Edinburgh Branch', 'Branch', 'Branch Manager Edinburgh'),
    ('CC-BR5', 'Bristol Branch', 'Branch', 'Branch Manager Bristol'),
]

# ── Payment Schemes ──────────────────────────────────────────
PAYMENT_SCHEMES = [
    ('FPS', 'Faster Payments', 'real_time', 250000, 'Near instant', '24/7'),
    ('BACS', 'BACS Direct Credit', 'batch', None, '3 working days', 'Working days'),
    ('DD', 'Direct Debit', 'batch', None, '3 working days', 'Working days'),
    ('CHAPS', 'CHAPS', 'high_value', None, 'Same day', '06:00-18:00'),
    ('SWIFT', 'SWIFT International', 'international', None, '1-5 working days', 'Working days'),
    ('MC', 'Mastercard', 'real_time', None, 'Real-time auth', '24/7'),
    ('VISA', 'Visa', 'real_time', None, 'Real-time auth', '24/7'),
    ('SO', 'Standing Order', 'batch', None, 'Scheduled', 'As scheduled'),
    ('LINK', 'LINK ATM Network', 'real_time', 500, 'Instant', '24/7'),
    ('SEPA', 'SEPA Credit Transfer', 'batch', None, '1-2 working days', 'Working days'),
]

# ── Branches ─────────────────────────────────────────────────
BRANCHES = [
    ('BR-LON-01', 'London City', 'London', 'London', 'EC2V 8AS', 'full_service'),
    ('BR-LON-02', 'London West End', 'London', 'London', 'W1D 3QR', 'full_service'),
    ('BR-MAN-01', 'Manchester Deansgate', 'North West', 'Manchester', 'M3 4LQ', 'full_service'),
    ('BR-BIR-01', 'Birmingham City', 'West Midlands', 'Birmingham', 'B2 5DB', 'full_service'),
    ('BR-EDI-01', 'Edinburgh George St', 'Scotland', 'Edinburgh', 'EH2 2PQ', 'full_service'),
    ('BR-BRI-01', 'Bristol Harbourside', 'South West', 'Bristol', 'BS1 5DB', 'full_service'),
    ('BR-LEE-01', 'Leeds City Square', 'Yorkshire and the Humber', 'Leeds', 'LS1 5AB', 'full_service'),
    ('BR-CAR-01', 'Cardiff Queen St', 'Wales', 'Cardiff', 'CF10 2BU', 'full_service'),
    ('BR-DIG-01', 'Digital Hub', 'London', 'London', 'EC1V 9NR', 'digital_hub'),
    ('BR-HQ-01', 'Head Office', 'London', 'London', 'EC2N 1HQ', 'head_office'),
]
