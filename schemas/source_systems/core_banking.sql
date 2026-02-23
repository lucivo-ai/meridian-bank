-- ============================================================
-- MERIDIAN COMMUNITY BANK — Core Banking System
-- Schema: core_banking
-- ============================================================

CREATE SCHEMA IF NOT EXISTS core_banking;

-- ------------------------------------------------------------
-- Products catalogue
-- ------------------------------------------------------------
CREATE TABLE core_banking.products (
    product_id          SERIAL PRIMARY KEY,
    product_code        VARCHAR(20) NOT NULL UNIQUE,
    name                VARCHAR(100) NOT NULL,
    category            VARCHAR(30) NOT NULL CHECK (category IN (
                            'current_account', 'savings', 'personal_loan',
                            'mortgage', 'credit_card', 'business_current',
                            'business_loan', 'business_savings'
                        )),
    interest_rate       NUMERIC(6,4),               -- Annual rate
    currency            CHAR(3) NOT NULL DEFAULT 'GBP',
    min_balance         NUMERIC(15,2) DEFAULT 0,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    launched_date       DATE NOT NULL,
    description         TEXT,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE core_banking.products IS 'Product catalogue for all banking products offered by Meridian Community Bank';
COMMENT ON COLUMN core_banking.products.product_code IS 'Unique product identifier code used across systems';
COMMENT ON COLUMN core_banking.products.category IS 'Product category classification';
COMMENT ON COLUMN core_banking.products.interest_rate IS 'Annual interest rate (APR for lending, AER for deposits)';

-- ------------------------------------------------------------
-- Customers
-- ------------------------------------------------------------
CREATE TABLE core_banking.customers (
    customer_id         SERIAL PRIMARY KEY,
    customer_ref        VARCHAR(20) NOT NULL UNIQUE,  -- MCB-XXXXXXXX
    type                VARCHAR(10) NOT NULL CHECK (type IN ('personal', 'business')),
    title               VARCHAR(10),
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    full_name           VARCHAR(200) NOT NULL,
    date_of_birth       DATE,
    gender              VARCHAR(10),
    nationality         VARCHAR(50),
    ni_number           VARCHAR(15),                  -- National Insurance number (PII)
    email               VARCHAR(200),
    phone_mobile        VARCHAR(20),
    phone_home          VARCHAR(20),
    -- Business fields
    company_name        VARCHAR(200),
    company_reg_number  VARCHAR(20),
    sic_code            VARCHAR(10),
    -- KYC and status
    kyc_status          VARCHAR(30) NOT NULL DEFAULT 'pending' CHECK (kyc_status IN (
                            'pending', 'verified', 'enhanced_due_diligence', 'expired', 'failed'
                        )),
    kyc_verified_date   DATE,
    risk_rating         VARCHAR(10) DEFAULT 'standard' CHECK (risk_rating IN (
                            'low', 'standard', 'medium', 'high', 'pep', 'sanctioned'
                        )),
    customer_segment    VARCHAR(30),
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    onboarded_date      DATE NOT NULL,
    closed_date         DATE,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE core_banking.customers IS 'Customer master data — all personal and business customers of Meridian Community Bank';
COMMENT ON COLUMN core_banking.customers.customer_ref IS 'External customer reference number (MCB-XXXXXXXX format)';
COMMENT ON COLUMN core_banking.customers.ni_number IS 'UK National Insurance number — PII, restricted access';
COMMENT ON COLUMN core_banking.customers.kyc_status IS 'Know Your Customer verification status';
COMMENT ON COLUMN core_banking.customers.risk_rating IS 'Customer risk classification for AML/CTF purposes';

CREATE INDEX idx_customers_type ON core_banking.customers(type);
CREATE INDEX idx_customers_risk ON core_banking.customers(risk_rating);
CREATE INDEX idx_customers_segment ON core_banking.customers(customer_segment);
CREATE INDEX idx_customers_kyc ON core_banking.customers(kyc_status);

-- ------------------------------------------------------------
-- Addresses
-- ------------------------------------------------------------
CREATE TABLE core_banking.addresses (
    address_id          SERIAL PRIMARY KEY,
    customer_id         INTEGER NOT NULL REFERENCES core_banking.customers(customer_id),
    address_type        VARCHAR(20) NOT NULL CHECK (address_type IN (
                            'home', 'business', 'correspondence', 'previous'
                        )),
    line1               VARCHAR(200) NOT NULL,
    line2               VARCHAR(200),
    line3               VARCHAR(200),
    city                VARCHAR(100) NOT NULL,
    county              VARCHAR(100),
    postcode            VARCHAR(10),                  -- Deliberately nullable for DQ testing
    country             VARCHAR(50) NOT NULL DEFAULT 'United Kingdom',
    is_primary          BOOLEAN NOT NULL DEFAULT FALSE,
    valid_from          DATE NOT NULL,
    valid_to            DATE,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE core_banking.addresses IS 'Customer addresses — PII data, supports multiple address types per customer';
COMMENT ON COLUMN core_banking.addresses.postcode IS 'UK postcode — intentionally nullable to test data quality scenarios';

CREATE INDEX idx_addresses_customer ON core_banking.addresses(customer_id);
CREATE INDEX idx_addresses_postcode ON core_banking.addresses(postcode);

-- ------------------------------------------------------------
-- Accounts
-- ------------------------------------------------------------
CREATE TABLE core_banking.accounts (
    account_id          SERIAL PRIMARY KEY,
    customer_id         INTEGER NOT NULL REFERENCES core_banking.customers(customer_id),
    product_id          INTEGER NOT NULL REFERENCES core_banking.products(product_id),
    account_number      VARCHAR(8) NOT NULL,          -- UK 8-digit account number
    sort_code           VARCHAR(6) NOT NULL,           -- UK 6-digit sort code
    iban                VARCHAR(34),
    account_name        VARCHAR(200),
    status              VARCHAR(15) NOT NULL DEFAULT 'active' CHECK (status IN (
                            'active', 'dormant', 'frozen', 'closed', 'in_arrears', 'default'
                        )),
    currency            CHAR(3) NOT NULL DEFAULT 'GBP',
    credit_limit        NUMERIC(15,2),                -- For credit cards / overdrafts
    overdraft_limit     NUMERIC(15,2),
    opened_date         DATE NOT NULL,
    closed_date         DATE,
    last_transaction_date DATE,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_account_sortcode UNIQUE (account_number, sort_code)
);
COMMENT ON TABLE core_banking.accounts IS 'Customer accounts across all product types — Sensitive financial data';
COMMENT ON COLUMN core_banking.accounts.sort_code IS 'UK bank sort code (6 digits) — Sensitive';
COMMENT ON COLUMN core_banking.accounts.account_number IS 'UK account number (8 digits) — Sensitive';

CREATE INDEX idx_accounts_customer ON core_banking.accounts(customer_id);
CREATE INDEX idx_accounts_product ON core_banking.accounts(product_id);
CREATE INDEX idx_accounts_status ON core_banking.accounts(status);

-- ------------------------------------------------------------
-- Account Balances (daily snapshots)
-- ------------------------------------------------------------
CREATE TABLE core_banking.account_balances (
    balance_id          BIGSERIAL PRIMARY KEY,
    account_id          INTEGER NOT NULL REFERENCES core_banking.accounts(account_id),
    balance_date        DATE NOT NULL,
    ledger_balance      NUMERIC(15,2) NOT NULL,
    available_balance   NUMERIC(15,2) NOT NULL,
    currency            CHAR(3) NOT NULL DEFAULT 'GBP',
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_balance_daily UNIQUE (account_id, balance_date)
);
COMMENT ON TABLE core_banking.account_balances IS 'End-of-day balance snapshots for all accounts — Sensitive financial data';

CREATE INDEX idx_balances_account ON core_banking.account_balances(account_id);
CREATE INDEX idx_balances_date ON core_banking.account_balances(balance_date);

-- ------------------------------------------------------------
-- Transactions
-- ------------------------------------------------------------
CREATE TABLE core_banking.transactions (
    txn_id              BIGSERIAL PRIMARY KEY,
    account_id          INTEGER NOT NULL REFERENCES core_banking.accounts(account_id),
    txn_date            DATE NOT NULL,
    txn_timestamp       TIMESTAMP NOT NULL,
    value_date          DATE NOT NULL,
    amount              NUMERIC(15,2) NOT NULL,       -- Positive = credit, negative = debit
    currency            CHAR(3) NOT NULL DEFAULT 'GBP',
    txn_type            VARCHAR(30) NOT NULL CHECK (txn_type IN (
                            'direct_debit', 'standing_order', 'faster_payment',
                            'bacs', 'chaps', 'card_payment', 'atm_withdrawal',
                            'interest', 'fee', 'transfer_in', 'transfer_out',
                            'salary', 'loan_repayment', 'mortgage_payment'
                        )),
    description         VARCHAR(500),
    counterparty_name   VARCHAR(200),
    counterparty_account VARCHAR(20),
    counterparty_sort_code VARCHAR(6),
    channel             VARCHAR(20) CHECK (channel IN (
                            'online', 'mobile', 'branch', 'atm', 'phone', 'api', 'batch'
                        )),
    reference           VARCHAR(50),
    status              VARCHAR(15) NOT NULL DEFAULT 'completed' CHECK (status IN (
                            'pending', 'completed', 'failed', 'reversed', 'disputed'
                        )),
    balance_after       NUMERIC(15,2),
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE core_banking.transactions IS 'Transaction history — 6 months of full transaction data. Sensitive financial data.';
COMMENT ON COLUMN core_banking.transactions.amount IS 'Transaction amount: positive = credit, negative = debit';
COMMENT ON COLUMN core_banking.transactions.txn_type IS 'Payment method/type classification';

CREATE INDEX idx_txn_account ON core_banking.transactions(account_id);
CREATE INDEX idx_txn_date ON core_banking.transactions(txn_date);
CREATE INDEX idx_txn_type ON core_banking.transactions(txn_type);
CREATE INDEX idx_txn_status ON core_banking.transactions(status);
CREATE INDEX idx_txn_amount ON core_banking.transactions(amount);

-- ------------------------------------------------------------
-- Standing Orders
-- ------------------------------------------------------------
CREATE TABLE core_banking.standing_orders (
    so_id               SERIAL PRIMARY KEY,
    account_id          INTEGER NOT NULL REFERENCES core_banking.accounts(account_id),
    payee_name          VARCHAR(200) NOT NULL,
    payee_account       VARCHAR(8),
    payee_sort_code     VARCHAR(6),
    amount              NUMERIC(15,2) NOT NULL,
    currency            CHAR(3) NOT NULL DEFAULT 'GBP',
    frequency           VARCHAR(15) NOT NULL CHECK (frequency IN (
                            'weekly', 'fortnightly', 'monthly', 'quarterly', 'annually'
                        )),
    start_date          DATE NOT NULL,
    end_date            DATE,
    next_payment_date   DATE,
    reference           VARCHAR(50),
    status              VARCHAR(15) NOT NULL DEFAULT 'active' CHECK (status IN (
                            'active', 'paused', 'cancelled', 'completed'
                        )),
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE core_banking.standing_orders IS 'Recurring payment instructions set up by customers';

CREATE INDEX idx_so_account ON core_banking.standing_orders(account_id);

-- ------------------------------------------------------------
-- Direct Debits
-- ------------------------------------------------------------
CREATE TABLE core_banking.direct_debits (
    dd_id               SERIAL PRIMARY KEY,
    account_id          INTEGER NOT NULL REFERENCES core_banking.accounts(account_id),
    originator_name     VARCHAR(200) NOT NULL,
    originator_id       VARCHAR(20) NOT NULL,         -- SUN (Service User Number)
    reference           VARCHAR(50),
    mandate_date        DATE NOT NULL,
    first_collection    DATE,
    last_collection     DATE,
    status              VARCHAR(15) NOT NULL DEFAULT 'active' CHECK (status IN (
                            'active', 'suspended', 'cancelled'
                        )),
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE core_banking.direct_debits IS 'Direct Debit mandates — authorisations for third parties to collect payments';

CREATE INDEX idx_dd_account ON core_banking.direct_debits(account_id);
