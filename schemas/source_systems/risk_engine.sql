-- ============================================================
-- MERIDIAN COMMUNITY BANK — Risk & Compliance Engine
-- Schema: risk
-- ============================================================

CREATE SCHEMA IF NOT EXISTS risk;

-- ------------------------------------------------------------
-- Credit Scores
-- ------------------------------------------------------------
CREATE TABLE risk.credit_scores (
    score_id            SERIAL PRIMARY KEY,
    customer_id         INTEGER NOT NULL,
    score_date          DATE NOT NULL,
    score_value         INTEGER NOT NULL CHECK (score_value BETWEEN 0 AND 999),
    score_band          VARCHAR(15) NOT NULL CHECK (score_band IN (
                            'excellent', 'good', 'fair', 'poor', 'very_poor'
                        )),
    model_name          VARCHAR(50) NOT NULL DEFAULT 'MCB_SCORE_V3',
    model_version       VARCHAR(20),
    factors             JSONB,                          -- Key factors influencing score
    is_current          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE risk.credit_scores IS 'Internal credit scoring — computed monthly for all active customers';

CREATE INDEX idx_credit_scores_customer ON risk.credit_scores(customer_id);
CREATE INDEX idx_credit_scores_band ON risk.credit_scores(score_band);

-- ------------------------------------------------------------
-- Credit Applications
-- ------------------------------------------------------------
CREATE TABLE risk.credit_applications (
    application_id      SERIAL PRIMARY KEY,
    customer_id         INTEGER NOT NULL,
    product_id          INTEGER NOT NULL,
    application_date    DATE NOT NULL,
    requested_amount    NUMERIC(15,2) NOT NULL,
    approved_amount     NUMERIC(15,2),
    term_months         INTEGER,
    interest_rate       NUMERIC(6,4),
    purpose             VARCHAR(50),
    employment_status   VARCHAR(20),
    annual_income       NUMERIC(12,2),
    credit_score_at_application INTEGER,
    decision            VARCHAR(15) NOT NULL CHECK (decision IN (
                            'approved', 'declined', 'referred', 'withdrawn', 'pending'
                        )),
    decision_date       DATE,
    decision_reason     VARCHAR(200),
    affordability_ratio NUMERIC(5,2),                  -- Debt-to-income ratio
    underwriter         VARCHAR(100),
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE risk.credit_applications IS 'Loan and mortgage applications with full underwriting data — Sensitive';

CREATE INDEX idx_applications_customer ON risk.credit_applications(customer_id);
CREATE INDEX idx_applications_decision ON risk.credit_applications(decision);

-- ------------------------------------------------------------
-- AML Alerts (Transaction Monitoring)
-- ------------------------------------------------------------
CREATE TABLE risk.aml_alerts (
    alert_id            SERIAL PRIMARY KEY,
    customer_id         INTEGER NOT NULL,
    alert_date          TIMESTAMP NOT NULL,
    alert_type          VARCHAR(30) NOT NULL CHECK (alert_type IN (
                            'unusual_activity', 'large_cash', 'structuring',
                            'rapid_movement', 'high_risk_jurisdiction',
                            'sanctions_hit', 'pep_activity', 'dormant_reactivation'
                        )),
    rule_id             VARCHAR(20) NOT NULL,
    rule_name           VARCHAR(100),
    trigger_amount      NUMERIC(15,2),
    trigger_details     TEXT,
    risk_score          NUMERIC(5,2),                  -- 0-100
    status              VARCHAR(15) NOT NULL DEFAULT 'open' CHECK (status IN (
                            'open', 'investigating', 'escalated', 'sar_filed',
                            'false_positive', 'closed'
                        )),
    assigned_to         VARCHAR(100),
    resolution_date     TIMESTAMP,
    resolution_notes    TEXT,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE risk.aml_alerts IS 'Anti-Money Laundering transaction monitoring alerts — Regulatory, restricted access';

CREATE INDEX idx_aml_alerts_customer ON risk.aml_alerts(customer_id);
CREATE INDEX idx_aml_alerts_status ON risk.aml_alerts(status);
CREATE INDEX idx_aml_alerts_type ON risk.aml_alerts(alert_type);

-- ------------------------------------------------------------
-- AML Cases (Investigations)
-- ------------------------------------------------------------
CREATE TABLE risk.aml_cases (
    case_id             SERIAL PRIMARY KEY,
    case_ref            VARCHAR(20) NOT NULL UNIQUE,   -- AML-YYYY-NNNN
    customer_id         INTEGER NOT NULL,
    opened_date         DATE NOT NULL,
    case_type           VARCHAR(20) NOT NULL CHECK (case_type IN (
                            'investigation', 'enhanced_monitoring', 'sar', 'referral'
                        )),
    priority            VARCHAR(10) NOT NULL CHECK (priority IN ('low', 'medium', 'high', 'critical')),
    description         TEXT NOT NULL,
    linked_alerts       INTEGER[],                     -- Array of alert_ids
    total_suspicious_amount NUMERIC(15,2),
    status              VARCHAR(25) NOT NULL DEFAULT 'open' CHECK (status IN (
                            'open', 'investigating', 'pending_sar', 'sar_filed',
                            'closed_no_action', 'closed_action_taken'
                        )),
    sar_reference       VARCHAR(30),                   -- Suspicious Activity Report ref
    assigned_to         VARCHAR(100),
    closed_date         DATE,
    outcome_notes       TEXT,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE risk.aml_cases IS 'AML investigation cases — highly sensitive, restricted to MLRO team';

CREATE INDEX idx_aml_cases_customer ON risk.aml_cases(customer_id);
CREATE INDEX idx_aml_cases_status ON risk.aml_cases(status);

-- ------------------------------------------------------------
-- Sanctions Screening
-- ------------------------------------------------------------
CREATE TABLE risk.sanctions_screening (
    screening_id        SERIAL PRIMARY KEY,
    customer_id         INTEGER NOT NULL,
    screening_date      TIMESTAMP NOT NULL,
    screening_type      VARCHAR(20) NOT NULL CHECK (screening_type IN (
                            'onboarding', 'periodic', 'event_triggered', 'batch'
                        )),
    list_checked        VARCHAR(50) NOT NULL,          -- OFSI, EU, UN, OFAC etc.
    match_found         BOOLEAN NOT NULL DEFAULT FALSE,
    match_score         NUMERIC(5,2),                  -- 0-100 fuzzy match confidence
    match_details       TEXT,
    status              VARCHAR(15) NOT NULL CHECK (status IN (
                            'clear', 'potential_match', 'confirmed_match', 'false_positive'
                        )),
    reviewed_by         VARCHAR(100),
    review_date         TIMESTAMP,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE risk.sanctions_screening IS 'Sanctions and PEP screening results — Regulatory requirement';

CREATE INDEX idx_sanctions_customer ON risk.sanctions_screening(customer_id);
CREATE INDEX idx_sanctions_status ON risk.sanctions_screening(status);

-- ------------------------------------------------------------
-- Risk Assessments (CDD/EDD)
-- ------------------------------------------------------------
CREATE TABLE risk.risk_assessments (
    assessment_id       SERIAL PRIMARY KEY,
    customer_id         INTEGER NOT NULL,
    assessment_date     DATE NOT NULL,
    assessment_type     VARCHAR(20) NOT NULL CHECK (assessment_type IN (
                            'standard_cdd', 'enhanced_edd', 'simplified_sdd', 'periodic_review'
                        )),
    overall_risk        VARCHAR(15) NOT NULL CHECK (overall_risk IN (
                            'low', 'medium', 'high', 'very_high', 'unacceptable'
                        )),
    country_risk        VARCHAR(10),
    product_risk        VARCHAR(10),
    channel_risk        VARCHAR(10),
    occupation_risk     VARCHAR(10),
    source_of_funds     VARCHAR(100),
    source_of_wealth    VARCHAR(100),
    pep_status          BOOLEAN DEFAULT FALSE,
    adverse_media       BOOLEAN DEFAULT FALSE,
    next_review_date    DATE NOT NULL,
    assessed_by         VARCHAR(100),
    notes               TEXT,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE risk.risk_assessments IS 'Customer Due Diligence risk assessments — drives risk rating in core banking';

CREATE INDEX idx_risk_assess_customer ON risk.risk_assessments(customer_id);
CREATE INDEX idx_risk_assess_risk ON risk.risk_assessments(overall_risk);

-- ------------------------------------------------------------
-- Regulatory Reports (metadata)
-- ------------------------------------------------------------
CREATE TABLE risk.regulatory_reports (
    report_id           SERIAL PRIMARY KEY,
    report_code         VARCHAR(20) NOT NULL,          -- COREP, FINREP, LCR, NSFR etc.
    report_name         VARCHAR(100) NOT NULL,
    regulator           VARCHAR(50) NOT NULL,          -- PRA, FCA, BoE
    frequency           VARCHAR(15) NOT NULL CHECK (frequency IN (
                            'daily', 'weekly', 'monthly', 'quarterly', 'annually', 'ad_hoc'
                        )),
    reporting_period    DATE NOT NULL,
    submission_deadline DATE NOT NULL,
    actual_submission   TIMESTAMP,
    status              VARCHAR(15) NOT NULL CHECK (status IN (
                            'draft', 'in_review', 'submitted', 'accepted', 'rejected', 'resubmitted'
                        )),
    data_sources        TEXT[],                        -- Array of source dataset names
    prepared_by         VARCHAR(100),
    approved_by         VARCHAR(100),
    notes               TEXT,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE risk.regulatory_reports IS 'Regulatory report submission metadata — tracks filings to PRA/FCA/BoE';

CREATE INDEX idx_reg_reports_code ON risk.regulatory_reports(report_code);
CREATE INDEX idx_reg_reports_status ON risk.regulatory_reports(status);
