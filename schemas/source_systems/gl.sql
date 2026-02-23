-- ============================================================
-- MERIDIAN COMMUNITY BANK — General Ledger
-- Schema: gl
-- ============================================================

CREATE SCHEMA IF NOT EXISTS gl;

-- ------------------------------------------------------------
-- Chart of Accounts
-- ------------------------------------------------------------
CREATE TABLE gl.chart_of_accounts (
    account_code        VARCHAR(10) PRIMARY KEY,
    account_name        VARCHAR(200) NOT NULL,
    account_type        VARCHAR(20) NOT NULL CHECK (account_type IN (
                            'asset', 'liability', 'equity', 'revenue', 'expense'
                        )),
    account_subtype     VARCHAR(30),
    parent_code         VARCHAR(10) REFERENCES gl.chart_of_accounts(account_code),
    hierarchy_level     INTEGER NOT NULL,
    is_posting_account  BOOLEAN NOT NULL DEFAULT TRUE,  -- Can receive journal entries
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    regulatory_mapping  VARCHAR(50),                     -- Mapping to FINREP/COREP line items
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE gl.chart_of_accounts IS 'General Ledger chart of accounts — hierarchical account structure';
COMMENT ON COLUMN gl.chart_of_accounts.regulatory_mapping IS 'Mapping to regulatory reporting line items (FINREP/COREP)';

CREATE INDEX idx_coa_type ON gl.chart_of_accounts(account_type);
CREATE INDEX idx_coa_parent ON gl.chart_of_accounts(parent_code);

-- ------------------------------------------------------------
-- Cost Centres
-- ------------------------------------------------------------
CREATE TABLE gl.cost_centres (
    cost_centre_code    VARCHAR(10) PRIMARY KEY,
    cost_centre_name    VARCHAR(100) NOT NULL,
    department          VARCHAR(50) NOT NULL,
    manager             VARCHAR(100),
    parent_code         VARCHAR(10) REFERENCES gl.cost_centres(cost_centre_code),
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE gl.cost_centres IS 'Organisational cost centre structure for management reporting';

-- ------------------------------------------------------------
-- GL Entries (Journal Lines)
-- ------------------------------------------------------------
CREATE TABLE gl.gl_entries (
    entry_id            BIGSERIAL PRIMARY KEY,
    journal_id          VARCHAR(20) NOT NULL,           -- Groups related debits/credits
    batch_id            VARCHAR(30) NOT NULL,
    entry_date          DATE NOT NULL,
    posting_date        DATE NOT NULL,
    account_code        VARCHAR(10) NOT NULL REFERENCES gl.chart_of_accounts(account_code),
    cost_centre_code    VARCHAR(10) REFERENCES gl.cost_centres(cost_centre_code),
    debit_amount        NUMERIC(15,2) NOT NULL DEFAULT 0,
    credit_amount       NUMERIC(15,2) NOT NULL DEFAULT 0,
    currency            CHAR(3) NOT NULL DEFAULT 'GBP',
    description         VARCHAR(200),
    source_system       VARCHAR(30),                    -- Which system originated this entry
    source_reference    VARCHAR(50),                    -- Reference in source system
    reversal_of         BIGINT REFERENCES gl.gl_entries(entry_id),
    is_manual           BOOLEAN NOT NULL DEFAULT FALSE,
    posted_by           VARCHAR(100),
    approved_by         VARCHAR(100),
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_debit_or_credit CHECK (
        (debit_amount > 0 AND credit_amount = 0) OR
        (credit_amount > 0 AND debit_amount = 0)
    )
);
COMMENT ON TABLE gl.gl_entries IS 'General Ledger journal entries — all financial postings. Financial data.';
COMMENT ON COLUMN gl.gl_entries.journal_id IS 'Groups related debit and credit entries — debits must equal credits per journal';
COMMENT ON COLUMN gl.gl_entries.batch_id IS 'Processing batch identifier — used for reconciliation';

CREATE INDEX idx_gl_date ON gl.gl_entries(entry_date);
CREATE INDEX idx_gl_account ON gl.gl_entries(account_code);
CREATE INDEX idx_gl_journal ON gl.gl_entries(journal_id);
CREATE INDEX idx_gl_batch ON gl.gl_entries(batch_id);
CREATE INDEX idx_gl_source ON gl.gl_entries(source_system);

-- ------------------------------------------------------------
-- GL Balances (period-end snapshots)
-- ------------------------------------------------------------
CREATE TABLE gl.gl_balances (
    balance_id          SERIAL PRIMARY KEY,
    period_end_date     DATE NOT NULL,
    account_code        VARCHAR(10) NOT NULL REFERENCES gl.chart_of_accounts(account_code),
    cost_centre_code    VARCHAR(10) REFERENCES gl.cost_centres(cost_centre_code),
    opening_balance     NUMERIC(18,2) NOT NULL,
    period_debits       NUMERIC(18,2) NOT NULL DEFAULT 0,
    period_credits      NUMERIC(18,2) NOT NULL DEFAULT 0,
    closing_balance     NUMERIC(18,2) NOT NULL,
    currency            CHAR(3) NOT NULL DEFAULT 'GBP',
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_gl_balance UNIQUE (period_end_date, account_code, cost_centre_code)
);
COMMENT ON TABLE gl.gl_balances IS 'Period-end GL balance snapshots — used for financial reporting and reconciliation';

CREATE INDEX idx_gl_bal_period ON gl.gl_balances(period_end_date);
CREATE INDEX idx_gl_bal_account ON gl.gl_balances(account_code);
