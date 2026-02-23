-- ============================================================
-- MERIDIAN COMMUNITY BANK — Data Warehouse: Staging Layer
-- Schema: warehouse_staging
-- Mirrors source system tables with ingestion metadata
-- ============================================================

CREATE SCHEMA IF NOT EXISTS warehouse_staging;

-- Common staging columns added to every table:
--   _ingested_at   TIMESTAMP   — when the record was loaded
--   _source_system VARCHAR(30) — originating system
--   _batch_id      VARCHAR(30) — ingestion batch identifier
--   _record_hash   VARCHAR(64) — SHA-256 hash of source row for change detection

-- ------------------------------------------------------------
-- From core_banking
-- ------------------------------------------------------------
CREATE TABLE warehouse_staging.stg_customers (
    customer_id         INTEGER,
    customer_ref        VARCHAR(20),
    type                VARCHAR(10),
    title               VARCHAR(10),
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    full_name           VARCHAR(200),
    date_of_birth       DATE,
    gender              VARCHAR(10),
    nationality         VARCHAR(50),
    ni_number           VARCHAR(15),
    email               VARCHAR(200),
    phone_mobile        VARCHAR(20),
    kyc_status          VARCHAR(30),
    kyc_verified_date   DATE,
    risk_rating         VARCHAR(10),
    customer_segment    VARCHAR(30),
    is_active           BOOLEAN,
    onboarded_date      DATE,
    closed_date         DATE,
    -- Staging metadata
    _ingested_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    _source_system      VARCHAR(30) NOT NULL DEFAULT 'core_banking',
    _batch_id           VARCHAR(30) NOT NULL,
    _record_hash        VARCHAR(64)
);
COMMENT ON TABLE warehouse_staging.stg_customers IS 'Staging: Customer master from core banking — full extract, daily refresh';

CREATE TABLE warehouse_staging.stg_accounts (
    account_id          INTEGER,
    customer_id         INTEGER,
    product_id          INTEGER,
    account_number      VARCHAR(8),
    sort_code           VARCHAR(6),
    account_name        VARCHAR(200),
    status              VARCHAR(15),
    currency            CHAR(3),
    credit_limit        NUMERIC(15,2),
    overdraft_limit     NUMERIC(15,2),
    opened_date         DATE,
    closed_date         DATE,
    last_transaction_date DATE,
    _ingested_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    _source_system      VARCHAR(30) NOT NULL DEFAULT 'core_banking',
    _batch_id           VARCHAR(30) NOT NULL,
    _record_hash        VARCHAR(64)
);
COMMENT ON TABLE warehouse_staging.stg_accounts IS 'Staging: Account data from core banking — full extract, daily refresh';

CREATE TABLE warehouse_staging.stg_transactions (
    txn_id              BIGINT,
    account_id          INTEGER,
    txn_date            DATE,
    txn_timestamp       TIMESTAMP,
    value_date          DATE,
    amount              NUMERIC(15,2),
    currency            CHAR(3),
    txn_type            VARCHAR(30),
    description         VARCHAR(500),
    counterparty_name   VARCHAR(200),
    channel             VARCHAR(20),
    reference           VARCHAR(50),
    status              VARCHAR(15),
    balance_after       NUMERIC(15,2),
    _ingested_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    _source_system      VARCHAR(30) NOT NULL DEFAULT 'core_banking',
    _batch_id           VARCHAR(30) NOT NULL,
    _record_hash        VARCHAR(64)
);
COMMENT ON TABLE warehouse_staging.stg_transactions IS 'Staging: Transactions from core banking — incremental extract, daily';

-- ------------------------------------------------------------
-- From CRM
-- ------------------------------------------------------------
CREATE TABLE warehouse_staging.stg_contacts (
    contact_id          INTEGER,
    customer_id         INTEGER,
    contact_name        VARCHAR(200),
    email_primary       VARCHAR(200),
    phone_primary       VARCHAR(20),
    preferred_channel   VARCHAR(15),
    relationship_manager VARCHAR(100),
    assigned_branch     VARCHAR(50),
    _ingested_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    _source_system      VARCHAR(30) NOT NULL DEFAULT 'crm',
    _batch_id           VARCHAR(30) NOT NULL,
    _record_hash        VARCHAR(64)
);
COMMENT ON TABLE warehouse_staging.stg_contacts IS 'Staging: CRM contacts — full extract, daily refresh';

CREATE TABLE warehouse_staging.stg_interactions (
    interaction_id      BIGINT,
    contact_id          INTEGER,
    customer_id         INTEGER,
    interaction_date    TIMESTAMP,
    channel             VARCHAR(15),
    category            VARCHAR(30),
    subject             VARCHAR(200),
    resolved            BOOLEAN,
    sentiment_score     NUMERIC(3,2),
    _ingested_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    _source_system      VARCHAR(30) NOT NULL DEFAULT 'crm',
    _batch_id           VARCHAR(30) NOT NULL,
    _record_hash        VARCHAR(64)
);
COMMENT ON TABLE warehouse_staging.stg_interactions IS 'Staging: CRM interactions — incremental extract, daily';

-- ------------------------------------------------------------
-- From Risk
-- ------------------------------------------------------------
CREATE TABLE warehouse_staging.stg_credit_scores (
    score_id            INTEGER,
    customer_id         INTEGER,
    score_date          DATE,
    score_value         INTEGER,
    score_band          VARCHAR(15),
    model_name          VARCHAR(50),
    is_current          BOOLEAN,
    _ingested_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    _source_system      VARCHAR(30) NOT NULL DEFAULT 'risk_engine',
    _batch_id           VARCHAR(30) NOT NULL,
    _record_hash        VARCHAR(64)
);
COMMENT ON TABLE warehouse_staging.stg_credit_scores IS 'Staging: Credit scores from risk engine — full extract, monthly';

CREATE TABLE warehouse_staging.stg_aml_alerts (
    alert_id            INTEGER,
    customer_id         INTEGER,
    alert_date          TIMESTAMP,
    alert_type          VARCHAR(30),
    rule_id             VARCHAR(20),
    risk_score          NUMERIC(5,2),
    status              VARCHAR(15),
    resolution_date     TIMESTAMP,
    _ingested_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    _source_system      VARCHAR(30) NOT NULL DEFAULT 'risk_engine',
    _batch_id           VARCHAR(30) NOT NULL,
    _record_hash        VARCHAR(64)
);
COMMENT ON TABLE warehouse_staging.stg_aml_alerts IS 'Staging: AML alerts from risk engine — incremental extract, daily';

CREATE TABLE warehouse_staging.stg_risk_assessments (
    assessment_id       INTEGER,
    customer_id         INTEGER,
    assessment_date     DATE,
    assessment_type     VARCHAR(20),
    overall_risk        VARCHAR(10),
    pep_status          BOOLEAN,
    adverse_media       BOOLEAN,
    next_review_date    DATE,
    _ingested_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    _source_system      VARCHAR(30) NOT NULL DEFAULT 'risk_engine',
    _batch_id           VARCHAR(30) NOT NULL,
    _record_hash        VARCHAR(64)
);
COMMENT ON TABLE warehouse_staging.stg_risk_assessments IS 'Staging: Risk assessments — full extract, monthly';

-- ------------------------------------------------------------
-- From GL
-- ------------------------------------------------------------
CREATE TABLE warehouse_staging.stg_gl_entries (
    entry_id            BIGINT,
    journal_id          VARCHAR(20),
    batch_id            VARCHAR(30),
    entry_date          DATE,
    posting_date        DATE,
    account_code        VARCHAR(10),
    cost_centre_code    VARCHAR(10),
    debit_amount        NUMERIC(15,2),
    credit_amount       NUMERIC(15,2),
    currency            CHAR(3),
    description         VARCHAR(200),
    source_system       VARCHAR(30),
    source_reference    VARCHAR(50),
    _ingested_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    _source_system      VARCHAR(30) NOT NULL DEFAULT 'gl',
    _batch_id           VARCHAR(30) NOT NULL,
    _record_hash        VARCHAR(64)
);
COMMENT ON TABLE warehouse_staging.stg_gl_entries IS 'Staging: GL journal entries — incremental extract, daily';

-- Create indexes on staging tables for ETL performance
CREATE INDEX idx_stg_cust_id ON warehouse_staging.stg_customers(customer_id);
CREATE INDEX idx_stg_acct_id ON warehouse_staging.stg_accounts(account_id);
CREATE INDEX idx_stg_txn_id ON warehouse_staging.stg_transactions(txn_id);
CREATE INDEX idx_stg_txn_date ON warehouse_staging.stg_transactions(txn_date);
CREATE INDEX idx_stg_contacts_cust ON warehouse_staging.stg_contacts(customer_id);
CREATE INDEX idx_stg_gl_date ON warehouse_staging.stg_gl_entries(entry_date);
