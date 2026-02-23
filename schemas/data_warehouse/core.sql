-- ============================================================
-- MERIDIAN COMMUNITY BANK — Data Warehouse: Core/Conformed Layer
-- Schema: warehouse_core
-- Star schema with conformed dimensions and fact tables
-- ============================================================

CREATE SCHEMA IF NOT EXISTS warehouse_core;

-- ------------------------------------------------------------
-- dim_date — Static date dimension
-- ------------------------------------------------------------
CREATE TABLE warehouse_core.dim_date (
    date_key            INTEGER PRIMARY KEY,            -- YYYYMMDD format
    full_date           DATE NOT NULL UNIQUE,
    day_of_week         INTEGER NOT NULL,               -- 1=Monday, 7=Sunday
    day_name            VARCHAR(10) NOT NULL,
    day_of_month        INTEGER NOT NULL,
    day_of_year         INTEGER NOT NULL,
    week_of_year        INTEGER NOT NULL,
    iso_week            INTEGER NOT NULL,
    month_number        INTEGER NOT NULL,
    month_name          VARCHAR(10) NOT NULL,
    month_short         VARCHAR(3) NOT NULL,
    quarter             INTEGER NOT NULL,
    quarter_name        VARCHAR(2) NOT NULL,            -- Q1, Q2, Q3, Q4
    year                INTEGER NOT NULL,
    fiscal_year         INTEGER NOT NULL,               -- Meridian: Apr-Mar
    fiscal_quarter      INTEGER NOT NULL,
    is_weekend          BOOLEAN NOT NULL,
    is_bank_holiday     BOOLEAN NOT NULL DEFAULT FALSE,
    is_month_end        BOOLEAN NOT NULL,
    is_quarter_end      BOOLEAN NOT NULL,
    is_year_end         BOOLEAN NOT NULL
);
COMMENT ON TABLE warehouse_core.dim_date IS 'Conformed date dimension — covers 2020-2026, includes UK bank holidays and fiscal periods';

-- ------------------------------------------------------------
-- dim_customer — SCD Type 2
-- ------------------------------------------------------------
CREATE TABLE warehouse_core.dim_customer (
    customer_key        SERIAL PRIMARY KEY,             -- Surrogate key
    customer_id         INTEGER NOT NULL,               -- Natural key
    customer_ref        VARCHAR(20) NOT NULL,
    customer_type       VARCHAR(10) NOT NULL,
    full_name           VARCHAR(200) NOT NULL,
    date_of_birth       DATE,
    nationality         VARCHAR(50),
    email               VARCHAR(200),
    phone               VARCHAR(20),
    postcode            VARCHAR(10),
    city                VARCHAR(100),
    -- Enriched from CRM
    preferred_channel   VARCHAR(15),
    relationship_manager VARCHAR(100),
    assigned_branch     VARCHAR(50),
    -- Enriched from Risk
    kyc_status          VARCHAR(30),
    risk_rating         VARCHAR(10),
    credit_score_band   VARCHAR(15),
    credit_score_value  INTEGER,
    pep_status          BOOLEAN DEFAULT FALSE,
    -- Segmentation
    customer_segment    VARCHAR(30),
    -- SCD2 columns
    effective_from      DATE NOT NULL,
    effective_to        DATE,                           -- NULL = current record
    is_current          BOOLEAN NOT NULL DEFAULT TRUE,
    -- Metadata
    _source_systems     TEXT[] NOT NULL DEFAULT ARRAY['core_banking', 'crm', 'risk'],
    _loaded_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    _updated_at         TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE warehouse_core.dim_customer IS 'Conformed customer dimension (SCD Type 2) — merges core banking, CRM, and risk data';

CREATE INDEX idx_dim_cust_natural ON warehouse_core.dim_customer(customer_id);
CREATE INDEX idx_dim_cust_current ON warehouse_core.dim_customer(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_cust_segment ON warehouse_core.dim_customer(customer_segment);
CREATE INDEX idx_dim_cust_risk ON warehouse_core.dim_customer(risk_rating);

-- ------------------------------------------------------------
-- dim_account — SCD Type 2
-- ------------------------------------------------------------
CREATE TABLE warehouse_core.dim_account (
    account_key         SERIAL PRIMARY KEY,
    account_id          INTEGER NOT NULL,
    customer_id         INTEGER NOT NULL,
    account_number      VARCHAR(8) NOT NULL,
    sort_code           VARCHAR(6) NOT NULL,
    product_code        VARCHAR(20) NOT NULL,
    product_name        VARCHAR(100) NOT NULL,
    product_category    VARCHAR(30) NOT NULL,
    account_status      VARCHAR(15) NOT NULL,
    currency            CHAR(3) NOT NULL DEFAULT 'GBP',
    credit_limit        NUMERIC(15,2),
    overdraft_limit     NUMERIC(15,2),
    opened_date         DATE NOT NULL,
    closed_date         DATE,
    -- SCD2
    effective_from      DATE NOT NULL,
    effective_to        DATE,
    is_current          BOOLEAN NOT NULL DEFAULT TRUE,
    _loaded_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    _updated_at         TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE warehouse_core.dim_account IS 'Conformed account dimension (SCD Type 2) — enriched with product attributes';

CREATE INDEX idx_dim_acct_natural ON warehouse_core.dim_account(account_id);
CREATE INDEX idx_dim_acct_current ON warehouse_core.dim_account(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_acct_product ON warehouse_core.dim_account(product_category);

-- ------------------------------------------------------------
-- dim_product — SCD Type 1
-- ------------------------------------------------------------
CREATE TABLE warehouse_core.dim_product (
    product_key         SERIAL PRIMARY KEY,
    product_id          INTEGER NOT NULL UNIQUE,
    product_code        VARCHAR(20) NOT NULL,
    product_name        VARCHAR(100) NOT NULL,
    product_category    VARCHAR(30) NOT NULL,
    interest_rate       NUMERIC(6,4),
    currency            CHAR(3) NOT NULL DEFAULT 'GBP',
    is_active           BOOLEAN NOT NULL,
    launched_date       DATE NOT NULL,
    _loaded_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE warehouse_core.dim_product IS 'Product dimension — one row per product, SCD Type 1';

-- ------------------------------------------------------------
-- dim_branch — Static
-- ------------------------------------------------------------
CREATE TABLE warehouse_core.dim_branch (
    branch_key          SERIAL PRIMARY KEY,
    branch_code         VARCHAR(10) NOT NULL UNIQUE,
    branch_name         VARCHAR(100) NOT NULL,
    region              VARCHAR(50) NOT NULL,
    city                VARCHAR(50) NOT NULL,
    postcode            VARCHAR(10),
    branch_type         VARCHAR(20) CHECK (branch_type IN ('full_service', 'digital_hub', 'agency', 'head_office')),
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    _loaded_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE warehouse_core.dim_branch IS 'Branch/location dimension';

-- ------------------------------------------------------------
-- dim_geography
-- ------------------------------------------------------------
CREATE TABLE warehouse_core.dim_geography (
    geo_key             SERIAL PRIMARY KEY,
    postcode_area       VARCHAR(4) NOT NULL,            -- First part of postcode
    postcode_district   VARCHAR(6),
    city                VARCHAR(100),
    county              VARCHAR(100),
    region              VARCHAR(50) NOT NULL,            -- e.g. South East, North West
    country             VARCHAR(50) NOT NULL DEFAULT 'England',
    _loaded_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE warehouse_core.dim_geography IS 'UK geography dimension based on postcode areas';

-- ------------------------------------------------------------
-- fact_transactions
-- ------------------------------------------------------------
CREATE TABLE warehouse_core.fact_transactions (
    txn_key             BIGSERIAL PRIMARY KEY,
    txn_id              BIGINT NOT NULL,                -- Natural key from source
    date_key            INTEGER NOT NULL,               -- FK to dim_date
    customer_key        INTEGER NOT NULL,               -- FK to dim_customer (current)
    account_key         INTEGER NOT NULL,               -- FK to dim_account (current)
    product_key         INTEGER,                        -- FK to dim_product
    txn_date            DATE NOT NULL,
    txn_timestamp       TIMESTAMP NOT NULL,
    amount              NUMERIC(15,2) NOT NULL,
    amount_abs          NUMERIC(15,2) NOT NULL,         -- Absolute value for aggregation
    is_credit           BOOLEAN NOT NULL,               -- TRUE = credit, FALSE = debit
    currency            CHAR(3) NOT NULL DEFAULT 'GBP',
    txn_type            VARCHAR(30) NOT NULL,
    channel             VARCHAR(20),
    status              VARCHAR(15) NOT NULL,
    counterparty_name   VARCHAR(200),
    balance_after       NUMERIC(15,2),
    _loaded_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE warehouse_core.fact_transactions IS 'Transaction fact table — grain: one row per transaction';

CREATE INDEX idx_fact_txn_date ON warehouse_core.fact_transactions(date_key);
CREATE INDEX idx_fact_txn_customer ON warehouse_core.fact_transactions(customer_key);
CREATE INDEX idx_fact_txn_account ON warehouse_core.fact_transactions(account_key);
CREATE INDEX idx_fact_txn_type ON warehouse_core.fact_transactions(txn_type);

-- ------------------------------------------------------------
-- fact_daily_balances
-- ------------------------------------------------------------
CREATE TABLE warehouse_core.fact_daily_balances (
    balance_key         BIGSERIAL PRIMARY KEY,
    date_key            INTEGER NOT NULL,
    account_key         INTEGER NOT NULL,
    customer_key        INTEGER NOT NULL,
    product_key         INTEGER,
    balance_date        DATE NOT NULL,
    ledger_balance      NUMERIC(15,2) NOT NULL,
    available_balance   NUMERIC(15,2) NOT NULL,
    currency            CHAR(3) NOT NULL DEFAULT 'GBP',
    _loaded_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE warehouse_core.fact_daily_balances IS 'Daily balance snapshot fact — grain: one row per account per day';

CREATE INDEX idx_fact_bal_date ON warehouse_core.fact_daily_balances(date_key);
CREATE INDEX idx_fact_bal_account ON warehouse_core.fact_daily_balances(account_key);

-- ------------------------------------------------------------
-- fact_gl_entries
-- ------------------------------------------------------------
CREATE TABLE warehouse_core.fact_gl_entries (
    gl_key              BIGSERIAL PRIMARY KEY,
    entry_id            BIGINT NOT NULL,
    date_key            INTEGER NOT NULL,
    account_code        VARCHAR(10) NOT NULL,
    cost_centre_code    VARCHAR(10),
    journal_id          VARCHAR(20) NOT NULL,
    batch_id            VARCHAR(30) NOT NULL,
    debit_amount        NUMERIC(15,2) NOT NULL DEFAULT 0,
    credit_amount       NUMERIC(15,2) NOT NULL DEFAULT 0,
    net_amount          NUMERIC(15,2) NOT NULL,         -- debit - credit
    currency            CHAR(3) NOT NULL DEFAULT 'GBP',
    source_system       VARCHAR(30),
    description         VARCHAR(200),
    _loaded_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE warehouse_core.fact_gl_entries IS 'GL entries fact — grain: one row per journal line';

CREATE INDEX idx_fact_gl_date ON warehouse_core.fact_gl_entries(date_key);
CREATE INDEX idx_fact_gl_account ON warehouse_core.fact_gl_entries(account_code);
CREATE INDEX idx_fact_gl_journal ON warehouse_core.fact_gl_entries(journal_id);

-- ------------------------------------------------------------
-- bridge_customer_account (many-to-many)
-- ------------------------------------------------------------
CREATE TABLE warehouse_core.bridge_customer_account (
    bridge_key          SERIAL PRIMARY KEY,
    customer_key        INTEGER NOT NULL,
    account_key         INTEGER NOT NULL,
    relationship_type   VARCHAR(20) DEFAULT 'primary' CHECK (relationship_type IN (
                            'primary', 'joint', 'authorised_signatory', 'beneficiary'
                        )),
    effective_from      DATE NOT NULL,
    effective_to        DATE,
    is_current          BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT uq_bridge UNIQUE (customer_key, account_key, relationship_type)
);
COMMENT ON TABLE warehouse_core.bridge_customer_account IS 'Bridge table for many-to-many customer-account relationships';
