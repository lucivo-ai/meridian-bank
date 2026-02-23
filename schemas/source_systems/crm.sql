-- ============================================================
-- MERIDIAN COMMUNITY BANK — CRM System
-- Schema: crm
-- ============================================================

CREATE SCHEMA IF NOT EXISTS crm;

-- ------------------------------------------------------------
-- Contacts (linked to core_banking.customers)
-- ------------------------------------------------------------
CREATE TABLE crm.contacts (
    contact_id          SERIAL PRIMARY KEY,
    customer_id         INTEGER NOT NULL,              -- FK to core_banking.customers
    contact_name        VARCHAR(200) NOT NULL,
    email_primary       VARCHAR(200),
    email_secondary     VARCHAR(200),
    phone_primary       VARCHAR(20),
    phone_secondary     VARCHAR(20),
    preferred_channel   VARCHAR(15) CHECK (preferred_channel IN (
                            'email', 'phone', 'sms', 'post', 'app'
                        )),
    language_pref       VARCHAR(10) DEFAULT 'en',
    relationship_manager VARCHAR(100),
    assigned_branch     VARCHAR(50),
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE crm.contacts IS 'CRM contact records — linked to core banking customer master. Contains PII.';
COMMENT ON COLUMN crm.contacts.customer_id IS 'References core_banking.customers.customer_id — not enforced as cross-schema FK';

CREATE INDEX idx_contacts_customer ON crm.contacts(customer_id);

-- ------------------------------------------------------------
-- Interactions
-- ------------------------------------------------------------
CREATE TABLE crm.interactions (
    interaction_id      BIGSERIAL PRIMARY KEY,
    contact_id          INTEGER NOT NULL REFERENCES crm.contacts(contact_id),
    customer_id         INTEGER NOT NULL,
    interaction_date    TIMESTAMP NOT NULL,
    channel             VARCHAR(15) NOT NULL CHECK (channel IN (
                            'phone_inbound', 'phone_outbound', 'email_inbound',
                            'email_outbound', 'branch_visit', 'webchat',
                            'app_message', 'letter'
                        )),
    category            VARCHAR(30) NOT NULL CHECK (category IN (
                            'enquiry', 'complaint', 'service_request',
                            'product_enquiry', 'account_maintenance',
                            'dispute', 'feedback', 'outbound_campaign'
                        )),
    subject             VARCHAR(200),
    notes               TEXT,
    resolved            BOOLEAN DEFAULT FALSE,
    resolution_date     TIMESTAMP,
    handled_by          VARCHAR(100),
    duration_seconds    INTEGER,
    sentiment_score     NUMERIC(3,2),                  -- -1.0 to 1.0
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE crm.interactions IS 'Customer interaction log — all touchpoints across channels';

CREATE INDEX idx_interactions_customer ON crm.interactions(customer_id);
CREATE INDEX idx_interactions_date ON crm.interactions(interaction_date);
CREATE INDEX idx_interactions_channel ON crm.interactions(channel);
CREATE INDEX idx_interactions_category ON crm.interactions(category);

-- ------------------------------------------------------------
-- Complaints (FCA regulated)
-- ------------------------------------------------------------
CREATE TABLE crm.complaints (
    complaint_id        SERIAL PRIMARY KEY,
    customer_id         INTEGER NOT NULL,
    interaction_id      BIGINT REFERENCES crm.interactions(interaction_id),
    complaint_date      DATE NOT NULL,
    category            VARCHAR(30) NOT NULL CHECK (category IN (
                            'charges_fees', 'service_quality', 'product_mis_sell',
                            'fraud', 'data_breach', 'accessibility',
                            'payment_issue', 'lending_decision', 'other'
                        )),
    severity            VARCHAR(10) NOT NULL CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    description         TEXT NOT NULL,
    root_cause          VARCHAR(100),
    status              VARCHAR(15) NOT NULL DEFAULT 'open' CHECK (status IN (
                            'open', 'investigating', 'resolved', 'escalated',
                            'referred_to_fos', 'closed'
                        )),
    resolution_date     DATE,
    resolution_notes    TEXT,
    compensation_amount NUMERIC(10,2) DEFAULT 0,
    fos_referral        BOOLEAN DEFAULT FALSE,         -- Financial Ombudsman Service
    assigned_to         VARCHAR(100),
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE crm.complaints IS 'Formal complaints register — FCA regulated, must be reported';
COMMENT ON COLUMN crm.complaints.fos_referral IS 'Whether complaint was escalated to the Financial Ombudsman Service';

CREATE INDEX idx_complaints_customer ON crm.complaints(customer_id);
CREATE INDEX idx_complaints_status ON crm.complaints(status);

-- ------------------------------------------------------------
-- Marketing Consents (GDPR)
-- ------------------------------------------------------------
CREATE TABLE crm.marketing_consents (
    consent_id          SERIAL PRIMARY KEY,
    customer_id         INTEGER NOT NULL,
    consent_type        VARCHAR(20) NOT NULL CHECK (consent_type IN (
                            'email_marketing', 'sms_marketing', 'phone_marketing',
                            'post_marketing', 'third_party_sharing', 'profiling',
                            'analytics'
                        )),
    is_consented        BOOLEAN NOT NULL,
    consent_date        TIMESTAMP NOT NULL,
    withdrawal_date     TIMESTAMP,
    consent_source      VARCHAR(30) CHECK (consent_source IN (
                            'onboarding', 'online_update', 'branch', 'phone', 'campaign_response'
                        )),
    lawful_basis        VARCHAR(20) CHECK (lawful_basis IN (
                            'consent', 'legitimate_interest', 'contract', 'legal_obligation'
                        )),
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE crm.marketing_consents IS 'GDPR consent records — tracks all marketing permissions and their lawful basis';

CREATE INDEX idx_consents_customer ON crm.marketing_consents(customer_id);

-- ------------------------------------------------------------
-- Customer Segments
-- ------------------------------------------------------------
CREATE TABLE crm.segments (
    segment_id          SERIAL PRIMARY KEY,
    customer_id         INTEGER NOT NULL,
    segment_code        VARCHAR(20) NOT NULL,
    segment_name        VARCHAR(50) NOT NULL,
    assigned_date       DATE NOT NULL,
    score               NUMERIC(5,2),                  -- Propensity / value score
    is_current          BOOLEAN NOT NULL DEFAULT TRUE,
    model_version       VARCHAR(20),
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE crm.segments IS 'Customer segmentation assignments — used for marketing and service differentiation';

CREATE INDEX idx_segments_customer ON crm.segments(customer_id);
CREATE INDEX idx_segments_code ON crm.segments(segment_code);
