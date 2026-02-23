-- ============================================================
-- MERIDIAN COMMUNITY BANK — Data Warehouse: Reporting/Mart Layer
-- Schema: warehouse_reporting
-- Pre-aggregated views and tables for business reporting
-- ============================================================

CREATE SCHEMA IF NOT EXISTS warehouse_reporting;

-- ------------------------------------------------------------
-- rpt_customer_360 — Aggregated customer view
-- ------------------------------------------------------------
CREATE TABLE warehouse_reporting.rpt_customer_360 (
    customer_key        INTEGER PRIMARY KEY,
    customer_id         INTEGER NOT NULL,
    customer_ref        VARCHAR(20) NOT NULL,
    full_name           VARCHAR(200) NOT NULL,
    customer_type       VARCHAR(10) NOT NULL,
    -- Demographics
    age                 INTEGER,
    postcode            VARCHAR(10),
    city                VARCHAR(100),
    region              VARCHAR(50),
    -- Relationship summary
    onboarded_date      DATE,
    tenure_months       INTEGER,
    num_active_accounts INTEGER NOT NULL DEFAULT 0,
    num_products        INTEGER NOT NULL DEFAULT 0,
    total_balance       NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_credit_balance NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_debit_balance  NUMERIC(18,2) NOT NULL DEFAULT 0,
    -- Activity (last 3 months)
    txn_count_3m        INTEGER NOT NULL DEFAULT 0,
    txn_total_credit_3m NUMERIC(18,2) NOT NULL DEFAULT 0,
    txn_total_debit_3m  NUMERIC(18,2) NOT NULL DEFAULT 0,
    last_txn_date       DATE,
    -- Risk profile
    risk_rating         VARCHAR(10),
    kyc_status          VARCHAR(30),
    credit_score_band   VARCHAR(15),
    aml_alert_count     INTEGER NOT NULL DEFAULT 0,
    -- CRM
    segment             VARCHAR(30),
    preferred_channel   VARCHAR(15),
    complaint_count     INTEGER NOT NULL DEFAULT 0,
    nps_score           NUMERIC(3,1),
    -- Metadata
    _report_date        DATE NOT NULL,
    _loaded_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE warehouse_reporting.rpt_customer_360 IS 'Customer 360 report — comprehensive aggregated view of each customer relationship';

CREATE INDEX idx_rpt_c360_type ON warehouse_reporting.rpt_customer_360(customer_type);
CREATE INDEX idx_rpt_c360_segment ON warehouse_reporting.rpt_customer_360(segment);
CREATE INDEX idx_rpt_c360_risk ON warehouse_reporting.rpt_customer_360(risk_rating);

-- ------------------------------------------------------------
-- rpt_daily_pnl — Daily P&L summary
-- ------------------------------------------------------------
CREATE TABLE warehouse_reporting.rpt_daily_pnl (
    pnl_id              SERIAL PRIMARY KEY,
    report_date         DATE NOT NULL,
    category            VARCHAR(30) NOT NULL,           -- revenue_interest, revenue_fees, cost_funding, etc.
    subcategory         VARCHAR(50),
    gl_account_code     VARCHAR(10),
    cost_centre_code    VARCHAR(10),
    amount              NUMERIC(18,2) NOT NULL,
    currency            CHAR(3) NOT NULL DEFAULT 'GBP',
    _loaded_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_daily_pnl UNIQUE (report_date, category, subcategory, gl_account_code, cost_centre_code)
);
COMMENT ON TABLE warehouse_reporting.rpt_daily_pnl IS 'Daily P&L report — summarised from GL entries by category';

CREATE INDEX idx_rpt_pnl_date ON warehouse_reporting.rpt_daily_pnl(report_date);

-- ------------------------------------------------------------
-- rpt_liquidity_coverage — LCR inputs
-- ------------------------------------------------------------
CREATE TABLE warehouse_reporting.rpt_liquidity_coverage (
    lcr_id              SERIAL PRIMARY KEY,
    report_date         DATE NOT NULL,
    -- High Quality Liquid Assets
    hqla_level1         NUMERIC(18,2) NOT NULL DEFAULT 0,
    hqla_level2a        NUMERIC(18,2) NOT NULL DEFAULT 0,
    hqla_level2b        NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_hqla          NUMERIC(18,2) NOT NULL DEFAULT 0,
    -- Cash outflows (30-day stress)
    retail_deposits_outflow    NUMERIC(18,2) NOT NULL DEFAULT 0,
    wholesale_funding_outflow  NUMERIC(18,2) NOT NULL DEFAULT 0,
    committed_facilities_outflow NUMERIC(18,2) NOT NULL DEFAULT 0,
    other_outflows             NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_outflows             NUMERIC(18,2) NOT NULL DEFAULT 0,
    -- Cash inflows
    retail_inflows      NUMERIC(18,2) NOT NULL DEFAULT 0,
    wholesale_inflows   NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_inflows       NUMERIC(18,2) NOT NULL DEFAULT 0,
    -- LCR calculation
    net_outflows        NUMERIC(18,2) NOT NULL DEFAULT 0,
    lcr_ratio           NUMERIC(8,4),                   -- Must be >= 100%
    is_compliant        BOOLEAN,
    _loaded_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE warehouse_reporting.rpt_liquidity_coverage IS 'Liquidity Coverage Ratio report — daily calculation for regulatory compliance (minimum 100%)';

CREATE INDEX idx_rpt_lcr_date ON warehouse_reporting.rpt_liquidity_coverage(report_date);

-- ------------------------------------------------------------
-- rpt_aml_summary — AML overview by period
-- ------------------------------------------------------------
CREATE TABLE warehouse_reporting.rpt_aml_summary (
    summary_id          SERIAL PRIMARY KEY,
    report_month        DATE NOT NULL,                  -- First day of month
    total_alerts        INTEGER NOT NULL DEFAULT 0,
    alerts_by_type      JSONB,                          -- {"unusual_activity": 50, "large_cash": 20, ...}
    alerts_open         INTEGER NOT NULL DEFAULT 0,
    alerts_closed       INTEGER NOT NULL DEFAULT 0,
    alerts_escalated    INTEGER NOT NULL DEFAULT 0,
    sars_filed          INTEGER NOT NULL DEFAULT 0,
    false_positive_rate NUMERIC(5,2),                   -- Percentage
    avg_resolution_days NUMERIC(5,1),
    cases_opened        INTEGER NOT NULL DEFAULT 0,
    cases_closed        INTEGER NOT NULL DEFAULT 0,
    high_risk_customers INTEGER NOT NULL DEFAULT 0,
    total_suspicious_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    _loaded_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE warehouse_reporting.rpt_aml_summary IS 'Monthly AML summary — alert volumes, SAR filings, and operational metrics';

CREATE INDEX idx_rpt_aml_month ON warehouse_reporting.rpt_aml_summary(report_month);

-- ------------------------------------------------------------
-- rpt_product_performance
-- ------------------------------------------------------------
CREATE TABLE warehouse_reporting.rpt_product_performance (
    perf_id             SERIAL PRIMARY KEY,
    report_month        DATE NOT NULL,
    product_code        VARCHAR(20) NOT NULL,
    product_name        VARCHAR(100) NOT NULL,
    product_category    VARCHAR(30) NOT NULL,
    active_accounts     INTEGER NOT NULL DEFAULT 0,
    new_accounts        INTEGER NOT NULL DEFAULT 0,
    closed_accounts     INTEGER NOT NULL DEFAULT 0,
    total_balance       NUMERIC(18,2) NOT NULL DEFAULT 0,
    avg_balance         NUMERIC(15,2),
    total_interest_income  NUMERIC(15,2) NOT NULL DEFAULT 0,
    total_interest_expense NUMERIC(15,2) NOT NULL DEFAULT 0,
    net_interest_margin NUMERIC(8,4),
    arrears_count       INTEGER NOT NULL DEFAULT 0,
    arrears_amount      NUMERIC(15,2) NOT NULL DEFAULT 0,
    _loaded_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_product_perf UNIQUE (report_month, product_code)
);
COMMENT ON TABLE warehouse_reporting.rpt_product_performance IS 'Monthly product performance metrics — volumes, balances, and profitability';

CREATE INDEX idx_rpt_prod_month ON warehouse_reporting.rpt_product_performance(report_month);
CREATE INDEX idx_rpt_prod_code ON warehouse_reporting.rpt_product_performance(product_code);

-- ------------------------------------------------------------
-- rpt_regulatory_capital — Capital adequacy inputs
-- ------------------------------------------------------------
CREATE TABLE warehouse_reporting.rpt_regulatory_capital (
    capital_id          SERIAL PRIMARY KEY,
    report_date         DATE NOT NULL,
    -- CET1
    cet1_capital        NUMERIC(18,2) NOT NULL,
    at1_capital         NUMERIC(18,2) NOT NULL DEFAULT 0,
    tier2_capital       NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_capital       NUMERIC(18,2) NOT NULL,
    -- Risk Weighted Assets
    rwa_credit          NUMERIC(18,2) NOT NULL DEFAULT 0,
    rwa_market          NUMERIC(18,2) NOT NULL DEFAULT 0,
    rwa_operational     NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_rwa           NUMERIC(18,2) NOT NULL,
    -- Ratios
    cet1_ratio          NUMERIC(8,4) NOT NULL,          -- Target >= 4.5%
    total_capital_ratio NUMERIC(8,4) NOT NULL,          -- Target >= 8%
    leverage_ratio      NUMERIC(8,4),
    is_compliant        BOOLEAN,
    _loaded_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE warehouse_reporting.rpt_regulatory_capital IS 'Regulatory capital adequacy report — CET1, RWA, and capital ratios for COREP';

CREATE INDEX idx_rpt_capital_date ON warehouse_reporting.rpt_regulatory_capital(report_date);

-- ------------------------------------------------------------
-- rpt_arrears_ageing
-- ------------------------------------------------------------
CREATE TABLE warehouse_reporting.rpt_arrears_ageing (
    arrears_id          SERIAL PRIMARY KEY,
    report_date         DATE NOT NULL,
    product_category    VARCHAR(30) NOT NULL,
    ageing_bucket       VARCHAR(20) NOT NULL CHECK (ageing_bucket IN (
                            '1-30_days', '31-60_days', '61-90_days',
                            '91-180_days', '181-365_days', 'over_365_days'
                        )),
    account_count       INTEGER NOT NULL DEFAULT 0,
    total_arrears_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_outstanding   NUMERIC(18,2) NOT NULL DEFAULT 0,
    provision_amount    NUMERIC(18,2) NOT NULL DEFAULT 0,
    _loaded_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_arrears UNIQUE (report_date, product_category, ageing_bucket)
);
COMMENT ON TABLE warehouse_reporting.rpt_arrears_ageing IS 'Arrears ageing report — loan book quality by product and ageing band';

CREATE INDEX idx_rpt_arrears_date ON warehouse_reporting.rpt_arrears_ageing(report_date);
