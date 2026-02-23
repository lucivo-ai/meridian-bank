-- ============================================================
-- MERIDIAN COMMUNITY BANK — Payments System
-- Schema: payments
-- ============================================================

CREATE SCHEMA IF NOT EXISTS payments;

-- ------------------------------------------------------------
-- Payment Schemes (reference data)
-- ------------------------------------------------------------
CREATE TABLE payments.payment_schemes (
    scheme_id           SERIAL PRIMARY KEY,
    scheme_code         VARCHAR(10) NOT NULL UNIQUE,
    scheme_name         VARCHAR(50) NOT NULL,
    scheme_type         VARCHAR(20) NOT NULL CHECK (scheme_type IN (
                            'real_time', 'batch', 'high_value', 'international'
                        )),
    max_amount          NUMERIC(15,2),
    settlement_cycle    VARCHAR(20),
    operating_hours     VARCHAR(50),
    is_active           BOOLEAN NOT NULL DEFAULT TRUE
);
COMMENT ON TABLE payments.payment_schemes IS 'UK payment scheme reference data — FPS, BACS, CHAPS, SWIFT etc.';

-- ------------------------------------------------------------
-- Payment Instructions (outbound)
-- ------------------------------------------------------------
CREATE TABLE payments.payment_instructions (
    instruction_id      BIGSERIAL PRIMARY KEY,
    account_id          INTEGER NOT NULL,               -- Source account
    scheme_id           INTEGER NOT NULL REFERENCES payments.payment_schemes(scheme_id),
    instruction_date    TIMESTAMP NOT NULL,
    amount              NUMERIC(15,2) NOT NULL,
    currency            CHAR(3) NOT NULL DEFAULT 'GBP',
    beneficiary_name    VARCHAR(200) NOT NULL,
    beneficiary_account VARCHAR(34),                    -- Account number or IBAN
    beneficiary_sort_code VARCHAR(6),
    beneficiary_bank    VARCHAR(100),
    reference           VARCHAR(140),
    payment_type        VARCHAR(25) NOT NULL CHECK (payment_type IN (
                            'single', 'bulk', 'standing_order', 'direct_debit_collection'
                        )),
    priority            VARCHAR(10) DEFAULT 'normal' CHECK (priority IN (
                            'normal', 'urgent', 'express'
                        )),
    status              VARCHAR(15) NOT NULL DEFAULT 'pending' CHECK (status IN (
                            'pending', 'processing', 'sent', 'settled',
                            'rejected', 'returned', 'cancelled'
                        )),
    rejection_reason    VARCHAR(200),
    settlement_date     DATE,
    batch_id            VARCHAR(30),
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE payments.payment_instructions IS 'Outbound payment instructions — all payment types sent by the bank';

CREATE INDEX idx_pi_account ON payments.payment_instructions(account_id);
CREATE INDEX idx_pi_date ON payments.payment_instructions(instruction_date);
CREATE INDEX idx_pi_status ON payments.payment_instructions(status);
CREATE INDEX idx_pi_scheme ON payments.payment_instructions(scheme_id);

-- ------------------------------------------------------------
-- Payment Receipts (inbound)
-- ------------------------------------------------------------
CREATE TABLE payments.payment_receipts (
    receipt_id          BIGSERIAL PRIMARY KEY,
    account_id          INTEGER NOT NULL,               -- Destination account
    scheme_id           INTEGER NOT NULL REFERENCES payments.payment_schemes(scheme_id),
    receipt_date        TIMESTAMP NOT NULL,
    amount              NUMERIC(15,2) NOT NULL,
    currency            CHAR(3) NOT NULL DEFAULT 'GBP',
    sender_name         VARCHAR(200),
    sender_account      VARCHAR(34),
    sender_sort_code    VARCHAR(6),
    sender_bank         VARCHAR(100),
    reference           VARCHAR(140),
    status              VARCHAR(15) NOT NULL DEFAULT 'applied' CHECK (status IN (
                            'received', 'applied', 'returned', 'held'
                        )),
    return_reason       VARCHAR(200),
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE payments.payment_receipts IS 'Inbound payment notifications — all payments received by bank customers';

CREATE INDEX idx_pr_account ON payments.payment_receipts(account_id);
CREATE INDEX idx_pr_date ON payments.payment_receipts(receipt_date);

-- ------------------------------------------------------------
-- Failed Payments
-- ------------------------------------------------------------
CREATE TABLE payments.failed_payments (
    failure_id          SERIAL PRIMARY KEY,
    instruction_id      BIGINT REFERENCES payments.payment_instructions(instruction_id),
    receipt_id          BIGINT REFERENCES payments.payment_receipts(receipt_id),
    failure_date        TIMESTAMP NOT NULL,
    failure_reason      VARCHAR(50) NOT NULL CHECK (failure_reason IN (
                            'insufficient_funds', 'invalid_account', 'invalid_sort_code',
                            'account_closed', 'beneficiary_deceased', 'sanctions_block',
                            'amount_limit_exceeded', 'duplicate_payment', 'technical_error',
                            'refer_to_drawer'
                        )),
    original_amount     NUMERIC(15,2) NOT NULL,
    currency            CHAR(3) NOT NULL DEFAULT 'GBP',
    resolution_status   VARCHAR(15) DEFAULT 'unresolved' CHECK (resolution_status IN (
                            'unresolved', 'retried', 'reversed', 'written_off'
                        )),
    resolved_date       DATE,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE payments.failed_payments IS 'Failed/rejected payment records — tracked for reconciliation and SLA monitoring';

CREATE INDEX idx_fp_date ON payments.failed_payments(failure_date);
CREATE INDEX idx_fp_reason ON payments.failed_payments(failure_reason);
