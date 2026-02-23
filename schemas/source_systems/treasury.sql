-- ============================================================
-- MERIDIAN COMMUNITY BANK — Treasury System
-- Schema: treasury
-- ============================================================

CREATE SCHEMA IF NOT EXISTS treasury;

-- ------------------------------------------------------------
-- Treasury Positions
-- ------------------------------------------------------------
CREATE TABLE treasury.positions (
    position_id         SERIAL PRIMARY KEY,
    position_date       DATE NOT NULL,
    instrument_type     VARCHAR(30) NOT NULL CHECK (instrument_type IN (
                            'gilt', 'corporate_bond', 'money_market',
                            'interbank_deposit', 'repo', 'fx_spot', 'fx_forward',
                            'interest_rate_swap', 'certificate_of_deposit'
                        )),
    instrument_ref      VARCHAR(30) NOT NULL,
    counterparty        VARCHAR(100),
    notional_amount     NUMERIC(18,2) NOT NULL,
    currency            CHAR(3) NOT NULL DEFAULT 'GBP',
    market_value        NUMERIC(18,2),
    book                VARCHAR(20) NOT NULL CHECK (book IN (
                            'banking_book', 'trading_book', 'liquidity_buffer'
                        )),
    maturity_date       DATE,
    yield_rate          NUMERIC(8,6),
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE treasury.positions IS 'Treasury book positions — investments, hedges, and liquidity instruments';

CREATE INDEX idx_positions_date ON treasury.positions(position_date);
CREATE INDEX idx_positions_book ON treasury.positions(book);

-- ------------------------------------------------------------
-- FX Rates
-- ------------------------------------------------------------
CREATE TABLE treasury.fx_rates (
    rate_id             SERIAL PRIMARY KEY,
    rate_date           DATE NOT NULL,
    base_currency       CHAR(3) NOT NULL DEFAULT 'GBP',
    quote_currency      CHAR(3) NOT NULL,
    spot_rate           NUMERIC(12,6) NOT NULL,
    source              VARCHAR(30) DEFAULT 'ECB',
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_fx_rate UNIQUE (rate_date, base_currency, quote_currency)
);
COMMENT ON TABLE treasury.fx_rates IS 'Daily FX rates — used for multi-currency valuations and reporting';

CREATE INDEX idx_fx_date ON treasury.fx_rates(rate_date);

-- ------------------------------------------------------------
-- Interbank Lending/Borrowing
-- ------------------------------------------------------------
CREATE TABLE treasury.interbank_lending (
    lending_id          SERIAL PRIMARY KEY,
    trade_date          DATE NOT NULL,
    settlement_date     DATE NOT NULL,
    maturity_date       DATE NOT NULL,
    direction           VARCHAR(10) NOT NULL CHECK (direction IN ('lend', 'borrow')),
    counterparty        VARCHAR(100) NOT NULL,
    principal_amount    NUMERIC(18,2) NOT NULL,
    currency            CHAR(3) NOT NULL DEFAULT 'GBP',
    interest_rate       NUMERIC(8,6) NOT NULL,
    interest_amount     NUMERIC(15,2),
    status              VARCHAR(15) NOT NULL CHECK (status IN (
                            'active', 'matured', 'rolled', 'defaulted'
                        )),
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE treasury.interbank_lending IS 'Interbank lending and borrowing positions';

-- ------------------------------------------------------------
-- Liquidity Pool
-- ------------------------------------------------------------
CREATE TABLE treasury.liquidity_pool (
    pool_id             SERIAL PRIMARY KEY,
    report_date         DATE NOT NULL,
    asset_class         VARCHAR(30) NOT NULL CHECK (asset_class IN (
                            'cash_central_bank', 'level_1_hqla', 'level_2a_hqla',
                            'level_2b_hqla', 'other_liquid_assets'
                        )),
    instrument_type     VARCHAR(50),
    nominal_value       NUMERIC(18,2) NOT NULL,
    market_value        NUMERIC(18,2) NOT NULL,
    haircut_pct         NUMERIC(5,2),
    adjusted_value      NUMERIC(18,2),                 -- After haircut
    currency            CHAR(3) NOT NULL DEFAULT 'GBP',
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE treasury.liquidity_pool IS 'Liquidity buffer composition — feeds LCR/NSFR regulatory reporting';

CREATE INDEX idx_liquidity_date ON treasury.liquidity_pool(report_date);
CREATE INDEX idx_liquidity_class ON treasury.liquidity_pool(asset_class);
