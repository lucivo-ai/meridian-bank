"""
Generate data warehouse layers:
- Staging (copy from source with metadata)
- Core dimensions and facts
- Reporting aggregates
"""
import numpy as np
from datetime import date, timedelta
from tqdm import tqdm
from sqlalchemy import text
from generators.config import SEED, WAREHOUSE_BATCH_DATE, STALE_TABLE_COUNT
from generators.utils.relationships import get_engine


def populate_staging():
    """Copy source data into staging tables with ingestion metadata."""
    engine = get_engine()
    batch_id = f'BATCH-{WAREHOUSE_BATCH_DATE.strftime("%Y%m%d")}-001'
    stale_batch_id = f'BATCH-20241220-001'  # 12 days stale for DQ testing

    staging_copies = [
        ('warehouse_staging.stg_customers',
         """INSERT INTO warehouse_staging.stg_customers
            (customer_id, customer_ref, type, title, first_name, last_name, full_name,
             date_of_birth, gender, nationality, ni_number, email, phone_mobile,
             kyc_status, kyc_verified_date, risk_rating, customer_segment, is_active,
             onboarded_date, closed_date, _batch_id, _source_system, _record_hash)
            SELECT customer_id, customer_ref, type, title, first_name, last_name, full_name,
                   date_of_birth, gender, nationality, ni_number, email, phone_mobile,
                   kyc_status, kyc_verified_date, risk_rating, customer_segment, is_active,
                   onboarded_date, closed_date, '{batch}', 'core_banking',
                   md5(ROW(customer_id, full_name, kyc_status, risk_rating)::text)
            FROM core_banking.customers"""),

        ('warehouse_staging.stg_accounts',
         """INSERT INTO warehouse_staging.stg_accounts
            (account_id, customer_id, product_id, account_number, sort_code, account_name,
             status, currency, credit_limit, overdraft_limit, opened_date, closed_date,
             last_transaction_date, _batch_id, _source_system, _record_hash)
            SELECT account_id, customer_id, product_id, account_number, sort_code, account_name,
                   status, currency, credit_limit, overdraft_limit, opened_date, closed_date,
                   last_transaction_date, '{batch}', 'core_banking',
                   md5(ROW(account_id, status, credit_limit)::text)
            FROM core_banking.accounts"""),

        ('warehouse_staging.stg_transactions',
         """INSERT INTO warehouse_staging.stg_transactions
            (txn_id, account_id, txn_date, txn_timestamp, value_date, amount, currency,
             txn_type, description, counterparty_name, channel, reference, status,
             balance_after, _batch_id, _source_system, _record_hash)
            SELECT txn_id, account_id, txn_date, txn_timestamp, value_date, amount, currency,
                   txn_type, description, counterparty_name, channel, reference, status,
                   balance_after, '{batch}', 'core_banking',
                   md5(ROW(txn_id, amount, status)::text)
            FROM core_banking.transactions"""),

        ('warehouse_staging.stg_contacts',
         """INSERT INTO warehouse_staging.stg_contacts
            (contact_id, customer_id, contact_name, email_primary, phone_primary,
             preferred_channel, relationship_manager, assigned_branch,
             _batch_id, _source_system, _record_hash)
            SELECT contact_id, customer_id, contact_name, email_primary, phone_primary,
                   preferred_channel, relationship_manager, assigned_branch,
                   '{batch}', 'crm',
                   md5(ROW(contact_id, contact_name)::text)
            FROM crm.contacts"""),

        ('warehouse_staging.stg_interactions',
         """INSERT INTO warehouse_staging.stg_interactions
            (interaction_id, contact_id, customer_id, interaction_date, channel,
             category, subject, resolved, sentiment_score,
             _batch_id, _source_system, _record_hash)
            SELECT interaction_id, contact_id, customer_id, interaction_date, channel,
                   category, subject, resolved, sentiment_score,
                   '{batch}', 'crm',
                   md5(ROW(interaction_id, resolved)::text)
            FROM crm.interactions"""),

        ('warehouse_staging.stg_credit_scores',
         """INSERT INTO warehouse_staging.stg_credit_scores
            (score_id, customer_id, score_date, score_value, score_band,
             model_name, is_current, _batch_id, _source_system, _record_hash)
            SELECT score_id, customer_id, score_date, score_value, score_band,
                   model_name, is_current, '{batch}', 'risk_engine',
                   md5(ROW(score_id, score_value)::text)
            FROM risk.credit_scores"""),

        ('warehouse_staging.stg_aml_alerts',
         """INSERT INTO warehouse_staging.stg_aml_alerts
            (alert_id, customer_id, alert_date, alert_type, rule_id, risk_score,
             status, resolution_date, _batch_id, _source_system, _record_hash)
            SELECT alert_id, customer_id, alert_date, alert_type, rule_id, risk_score,
                   status, resolution_date, '{batch}', 'risk_engine',
                   md5(ROW(alert_id, status)::text)
            FROM risk.aml_alerts"""),

        ('warehouse_staging.stg_risk_assessments',
         """INSERT INTO warehouse_staging.stg_risk_assessments
            (assessment_id, customer_id, assessment_date, assessment_type, overall_risk,
             pep_status, adverse_media, next_review_date,
             _batch_id, _source_system, _record_hash)
            SELECT assessment_id, customer_id, assessment_date, assessment_type, overall_risk,
                   pep_status, adverse_media, next_review_date,
                   '{batch}', 'risk_engine',
                   md5(ROW(assessment_id, overall_risk)::text)
            FROM risk.risk_assessments"""),

        ('warehouse_staging.stg_gl_entries',
         """INSERT INTO warehouse_staging.stg_gl_entries
            (entry_id, journal_id, batch_id, entry_date, posting_date, account_code,
             cost_centre_code, debit_amount, credit_amount, currency, description,
             source_system, source_reference, _batch_id, _source_system, _record_hash)
            SELECT entry_id, journal_id, batch_id, entry_date, posting_date, account_code,
                   cost_centre_code, debit_amount, credit_amount, currency, description,
                   source_system, source_reference, '{batch}', 'gl',
                   md5(ROW(entry_id, debit_amount, credit_amount)::text)
            FROM gl.gl_entries"""),
    ]

    # Make some staging tables stale for DQ testing
    stale_tables = ['warehouse_staging.stg_credit_scores',
                    'warehouse_staging.stg_risk_assessments',
                    'warehouse_staging.stg_aml_alerts']

    with engine.connect() as conn:
        for table_name, sql_template in tqdm(staging_copies, desc="  Staging tables"):
            use_batch = stale_batch_id if table_name in stale_tables else batch_id
            sql = sql_template.format(batch=use_batch)
            conn.execute(text(sql))
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            stale_note = " (STALE)" if table_name in stale_tables else ""
            print(f"    {table_name}: {count:,} rows{stale_note}")
        conn.commit()

    # Update _ingested_at for stale tables to make them actually stale
    with engine.connect() as conn:
        for t in stale_tables:
            conn.execute(text(f"""
                UPDATE {t} SET _ingested_at = '2024-12-20 02:00:00'
            """))
        conn.commit()

    print(f"  ✓ Staging layer populated ({STALE_TABLE_COUNT} tables marked stale)")


def populate_core_dimensions():
    """Build conformed dimensions from staging."""
    engine = get_engine()

    # ── dim_product ───────────────────────────────────────
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO warehouse_core.dim_product
                (product_id, product_code, product_name, product_category,
                 interest_rate, currency, is_active, launched_date)
            SELECT product_id, product_code, name, category,
                   interest_rate, currency, is_active, launched_date
            FROM core_banking.products
        """))
        conn.commit()
        count = conn.execute(text("SELECT COUNT(*) FROM warehouse_core.dim_product")).scalar()
    print(f"    dim_product: {count} rows")

    # ── dim_customer (SCD2 - single version for demo) ────
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO warehouse_core.dim_customer
                (customer_id, customer_ref, customer_type, full_name, date_of_birth,
                 nationality, email, phone, postcode, city,
                 preferred_channel, relationship_manager, assigned_branch,
                 kyc_status, risk_rating, credit_score_band, credit_score_value,
                 pep_status, customer_segment,
                 effective_from, effective_to, is_current, _source_systems)
            SELECT
                sc.customer_id, sc.customer_ref, sc.type, sc.full_name, sc.date_of_birth,
                sc.nationality, sc.email, sc.phone_mobile,
                a.postcode, a.city,
                c.preferred_channel, c.relationship_manager, c.assigned_branch,
                sc.kyc_status, sc.risk_rating,
                cs.score_band, cs.score_value,
                COALESCE(ra.pep_status, FALSE),
                sc.customer_segment,
                COALESCE(sc.onboarded_date, '2015-01-01'), NULL, TRUE,
                ARRAY['core_banking', 'crm', 'risk']
            FROM warehouse_staging.stg_customers sc
            LEFT JOIN (
                SELECT customer_id, postcode, city
                FROM core_banking.addresses
                WHERE is_primary = TRUE
            ) a ON sc.customer_id = a.customer_id
            LEFT JOIN warehouse_staging.stg_contacts c ON sc.customer_id = c.customer_id
            LEFT JOIN warehouse_staging.stg_credit_scores cs
                ON sc.customer_id = cs.customer_id AND cs.is_current = TRUE
            LEFT JOIN warehouse_staging.stg_risk_assessments ra
                ON sc.customer_id = ra.customer_id
        """))
        conn.commit()
        count = conn.execute(text("SELECT COUNT(*) FROM warehouse_core.dim_customer")).scalar()
    print(f"    dim_customer: {count:,} rows")

    # ── dim_account (SCD2 - single version) ──────────────
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO warehouse_core.dim_account
                (account_id, customer_id, account_number, sort_code,
                 product_code, product_name, product_category,
                 account_status, currency, credit_limit, overdraft_limit,
                 opened_date, closed_date,
                 effective_from, effective_to, is_current)
            SELECT
                sa.account_id, sa.customer_id, sa.account_number, sa.sort_code,
                p.product_code, p.name, p.category,
                sa.status, sa.currency, sa.credit_limit, sa.overdraft_limit,
                sa.opened_date, sa.closed_date,
                COALESCE(sa.opened_date, '2015-01-01'), NULL, TRUE
            FROM warehouse_staging.stg_accounts sa
            JOIN core_banking.products p ON sa.product_id = p.product_id
        """))
        conn.commit()
        count = conn.execute(text("SELECT COUNT(*) FROM warehouse_core.dim_account")).scalar()
    print(f"    dim_account: {count:,} rows")

    # ── dim_geography ─────────────────────────────────────
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO warehouse_core.dim_geography
                (postcode_area, city, region, country)
            SELECT DISTINCT
                SPLIT_PART(postcode, ' ', 1) as postcode_area,
                city,
                CASE
                    WHEN city IN ('London') THEN 'London'
                    WHEN city IN ('Manchester', 'Liverpool') THEN 'North West'
                    WHEN city IN ('Birmingham', 'Coventry') THEN 'West Midlands'
                    WHEN city IN ('Edinburgh', 'Glasgow') THEN 'Scotland'
                    WHEN city IN ('Cardiff', 'Swansea') THEN 'Wales'
                    WHEN city IN ('Bristol', 'Bath') THEN 'South West'
                    WHEN city IN ('Leeds', 'Sheffield') THEN 'Yorkshire and the Humber'
                    ELSE 'Other'
                END as region,
                'England' as country
            FROM core_banking.addresses
            WHERE postcode IS NOT NULL
            LIMIT 500
        """))
        conn.commit()
        count = conn.execute(text("SELECT COUNT(*) FROM warehouse_core.dim_geography")).scalar()
    print(f"    dim_geography: {count} rows")

    print("  ✓ Core dimensions populated")


def populate_core_facts():
    """Build fact tables from staging."""
    engine = get_engine()

    # ── fact_transactions ─────────────────────────────────
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO warehouse_core.fact_transactions
                (txn_id, date_key, customer_key, account_key, product_key,
                 txn_date, txn_timestamp, amount, amount_abs, is_credit,
                 currency, txn_type, channel, status, counterparty_name, balance_after)
            SELECT
                st.txn_id,
                TO_CHAR(st.txn_date, 'YYYYMMDD')::INTEGER as date_key,
                dc.customer_key,
                da.account_key,
                dp.product_key,
                st.txn_date, st.txn_timestamp, st.amount, ABS(st.amount),
                st.amount > 0,
                st.currency, st.txn_type, st.channel, st.status,
                st.counterparty_name, st.balance_after
            FROM warehouse_staging.stg_transactions st
            JOIN warehouse_core.dim_account da
                ON st.account_id = da.account_id AND da.is_current = TRUE
            JOIN warehouse_core.dim_customer dc
                ON da.customer_id = dc.customer_id AND dc.is_current = TRUE
            LEFT JOIN warehouse_core.dim_product dp
                ON da.product_code = dp.product_code
        """))
        conn.commit()
        count = conn.execute(text("SELECT COUNT(*) FROM warehouse_core.fact_transactions")).scalar()
    print(f"    fact_transactions: {count:,} rows")

    # ── fact_gl_entries ───────────────────────────────────
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO warehouse_core.fact_gl_entries
                (entry_id, date_key, account_code, cost_centre_code,
                 journal_id, batch_id, debit_amount, credit_amount, net_amount,
                 currency, source_system, description)
            SELECT
                entry_id,
                TO_CHAR(entry_date, 'YYYYMMDD')::INTEGER,
                account_code, cost_centre_code,
                journal_id, batch_id, debit_amount, credit_amount,
                debit_amount - credit_amount,
                currency, source_system, description
            FROM warehouse_staging.stg_gl_entries
        """))
        conn.commit()
        count = conn.execute(text("SELECT COUNT(*) FROM warehouse_core.fact_gl_entries")).scalar()
    print(f"    fact_gl_entries: {count:,} rows")

    # ── bridge_customer_account ───────────────────────────
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO warehouse_core.bridge_customer_account
                (customer_key, account_key, relationship_type, effective_from, is_current)
            SELECT DISTINCT
                dc.customer_key, da.account_key, 'primary',
                da.opened_date, TRUE
            FROM warehouse_core.dim_account da
            JOIN warehouse_core.dim_customer dc
                ON da.customer_id = dc.customer_id AND dc.is_current = TRUE
            WHERE da.is_current = TRUE
        """))
        conn.commit()
        count = conn.execute(text("SELECT COUNT(*) FROM warehouse_core.bridge_customer_account")).scalar()
    print(f"    bridge_customer_account: {count:,} rows")

    print("  ✓ Core facts populated")


def populate_reporting():
    """Build reporting/mart tables from core layer."""
    engine = get_engine()
    report_date = '2024-12-31'

    # ── rpt_customer_360 ──────────────────────────────────
    with engine.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO warehouse_reporting.rpt_customer_360
                (customer_key, customer_id, customer_ref, full_name, customer_type,
                 age, postcode, city, region,
                 onboarded_date, tenure_months, num_active_accounts, num_products,
                 total_balance, txn_count_3m, txn_total_credit_3m, txn_total_debit_3m,
                 last_txn_date,
                 risk_rating, kyc_status, credit_score_band, aml_alert_count,
                 segment, preferred_channel, complaint_count,
                 _report_date)
            SELECT
                dc.customer_key, dc.customer_id, dc.customer_ref, dc.full_name, dc.customer_type,
                EXTRACT(YEAR FROM AGE('{report_date}'::date, dc.date_of_birth))::INTEGER,
                dc.postcode, dc.city, 'Unknown',
                dc.effective_from,
                EXTRACT(MONTH FROM AGE('{report_date}'::date, dc.effective_from))::INTEGER,
                COALESCE(accts.active_count, 0),
                COALESCE(accts.product_count, 0),
                0,
                COALESCE(txns.txn_count, 0),
                COALESCE(txns.credit_total, 0),
                COALESCE(txns.debit_total, 0),
                txns.last_txn,
                dc.risk_rating, dc.kyc_status, dc.credit_score_band,
                COALESCE(aml.alert_count, 0),
                dc.customer_segment, dc.preferred_channel,
                COALESCE(comp.complaint_count, 0),
                '{report_date}'
            FROM warehouse_core.dim_customer dc
            LEFT JOIN (
                SELECT customer_id,
                       COUNT(*) FILTER (WHERE account_status = 'active') as active_count,
                       COUNT(DISTINCT product_category) as product_count
                FROM warehouse_core.dim_account WHERE is_current = TRUE
                GROUP BY customer_id
            ) accts ON dc.customer_id = accts.customer_id
            LEFT JOIN (
                SELECT dc2.customer_key,
                       COUNT(*) as txn_count,
                       SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as credit_total,
                       SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) as debit_total,
                       MAX(txn_date) as last_txn
                FROM warehouse_core.fact_transactions ft
                JOIN warehouse_core.dim_customer dc2 ON ft.customer_key = dc2.customer_key
                WHERE ft.txn_date >= '{report_date}'::date - INTERVAL '3 months'
                GROUP BY dc2.customer_key
            ) txns ON dc.customer_key = txns.customer_key
            LEFT JOIN (
                SELECT customer_id, COUNT(*) as alert_count
                FROM risk.aml_alerts GROUP BY customer_id
            ) aml ON dc.customer_id = aml.customer_id
            LEFT JOIN (
                SELECT customer_id, COUNT(*) as complaint_count
                FROM crm.complaints GROUP BY customer_id
            ) comp ON dc.customer_id = comp.customer_id
            WHERE dc.is_current = TRUE
        """))
        conn.commit()
        count = conn.execute(text("SELECT COUNT(*) FROM warehouse_reporting.rpt_customer_360")).scalar()
    print(f"    rpt_customer_360: {count:,} rows")

    # ── rpt_daily_pnl ────────────────────────────────────
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO warehouse_reporting.rpt_daily_pnl
                (report_date, category, subcategory, gl_account_code, cost_centre_code, amount, currency)
            SELECT
                entry_date as report_date,
                coa.account_type as category,
                coa.account_subtype as subcategory,
                ge.account_code as gl_account_code,
                ge.cost_centre_code,
                SUM(ge.debit_amount - ge.credit_amount) as amount,
                'GBP'
            FROM gl.gl_entries ge
            JOIN gl.chart_of_accounts coa ON ge.account_code = coa.account_code
            WHERE coa.account_type IN ('revenue', 'expense')
            GROUP BY entry_date, coa.account_type, coa.account_subtype, ge.account_code, ge.cost_centre_code
        """))
        conn.commit()
        count = conn.execute(text("SELECT COUNT(*) FROM warehouse_reporting.rpt_daily_pnl")).scalar()
    print(f"    rpt_daily_pnl: {count:,} rows")

    # ── rpt_aml_summary ──────────────────────────────────
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO warehouse_reporting.rpt_aml_summary
                (report_month, total_alerts, alerts_open, alerts_closed,
                 alerts_escalated, sars_filed, false_positive_rate,
                 avg_resolution_days, cases_opened, cases_closed,
                 high_risk_customers, total_suspicious_amount)
            SELECT
                DATE_TRUNC('month', alert_date)::date as report_month,
                COUNT(*) as total_alerts,
                COUNT(*) FILTER (WHERE status = 'open') as alerts_open,
                COUNT(*) FILTER (WHERE status = 'closed') as alerts_closed,
                COUNT(*) FILTER (WHERE status = 'escalated') as alerts_escalated,
                COUNT(*) FILTER (WHERE status = 'sar_filed') as sars_filed,
                ROUND(COUNT(*) FILTER (WHERE status = 'false_positive')::numeric / NULLIF(COUNT(*), 0) * 100, 2),
                AVG(EXTRACT(DAY FROM (resolution_date - alert_date)))::numeric(5,1),
                0, 0,
                COUNT(DISTINCT customer_id) FILTER (WHERE risk_score > 70),
                SUM(trigger_amount)
            FROM risk.aml_alerts
            GROUP BY DATE_TRUNC('month', alert_date)
        """))
        conn.commit()
        count = conn.execute(text("SELECT COUNT(*) FROM warehouse_reporting.rpt_aml_summary")).scalar()
    print(f"    rpt_aml_summary: {count} rows")

    # ── rpt_product_performance ───────────────────────────
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO warehouse_reporting.rpt_product_performance
                (report_month, product_code, product_name, product_category,
                 active_accounts, new_accounts, closed_accounts, total_balance)
            SELECT
                DATE_TRUNC('month', CURRENT_DATE)::date,
                da.product_code, da.product_name, da.product_category,
                COUNT(*) FILTER (WHERE da.account_status = 'active'),
                COUNT(*) FILTER (WHERE da.opened_date >= DATE_TRUNC('month', CURRENT_DATE)),
                COUNT(*) FILTER (WHERE da.account_status = 'closed'),
                0
            FROM warehouse_core.dim_account da
            WHERE da.is_current = TRUE
            GROUP BY da.product_code, da.product_name, da.product_category
        """))
        conn.commit()
        count = conn.execute(text("SELECT COUNT(*) FROM warehouse_reporting.rpt_product_performance")).scalar()
    print(f"    rpt_product_performance: {count} rows")

    # ── rpt_liquidity_coverage ────────────────────────────
    rng = np.random.default_rng(SEED + 80)
    with engine.connect() as conn:
        for month in range(7, 13):
            rd = date(2024, month, 28)
            lp = conn.execute(text(f"""
                SELECT asset_class, SUM(adjusted_value) as adj
                FROM treasury.liquidity_pool
                WHERE report_date = '{rd.isoformat()}'
                GROUP BY asset_class
            """)).fetchall()
            hqla = {r[0]: float(r[1]) for r in lp}

            total_hqla = sum(hqla.values())
            outflows = total_hqla * float(rng.uniform(0.6, 0.85))
            inflows = outflows * float(rng.uniform(0.3, 0.5))
            net_out = outflows - inflows
            lcr = (total_hqla / net_out * 100) if net_out > 0 else 999

            conn.execute(text(f"""
                INSERT INTO warehouse_reporting.rpt_liquidity_coverage
                    (report_date, hqla_level1, hqla_level2a, hqla_level2b, total_hqla,
                     total_outflows, total_inflows, net_outflows, lcr_ratio, is_compliant)
                VALUES ('{rd.isoformat()}',
                    {hqla.get('level_1_hqla', 0) + hqla.get('cash_central_bank', 0)},
                    {hqla.get('level_2a_hqla', 0)},
                    {hqla.get('level_2b_hqla', 0)},
                    {total_hqla}, {outflows}, {inflows}, {net_out},
                    {round(lcr, 4)}, {lcr >= 100})
            """))
        conn.commit()
        count = conn.execute(text("SELECT COUNT(*) FROM warehouse_reporting.rpt_liquidity_coverage")).scalar()
    print(f"    rpt_liquidity_coverage: {count} rows")

    # ── rpt_regulatory_capital ────────────────────────────
    with engine.connect() as conn:
        for month in range(7, 13):
            rd = date(2024, month, 28)
            cet1 = float(rng.uniform(180_000_000, 220_000_000))
            rwa = float(rng.uniform(1_200_000_000, 1_500_000_000))
            conn.execute(text(f"""
                INSERT INTO warehouse_reporting.rpt_regulatory_capital
                    (report_date, cet1_capital, at1_capital, tier2_capital, total_capital,
                     rwa_credit, rwa_market, rwa_operational, total_rwa,
                     cet1_ratio, total_capital_ratio, leverage_ratio, is_compliant)
                VALUES ('{rd.isoformat()}',
                    {cet1}, {cet1 * 0.05}, {cet1 * 0.1}, {cet1 * 1.15},
                    {rwa * 0.85}, {rwa * 0.05}, {rwa * 0.10}, {rwa},
                    {round(cet1 / rwa * 100, 4)}, {round(cet1 * 1.15 / rwa * 100, 4)},
                    {round(float(rng.uniform(4, 6)), 4)}, TRUE)
            """))
        conn.commit()
        count = conn.execute(text("SELECT COUNT(*) FROM warehouse_reporting.rpt_regulatory_capital")).scalar()
    print(f"    rpt_regulatory_capital: {count} rows")

    # ── rpt_arrears_ageing ────────────────────────────────
    with engine.connect() as conn:
        buckets = ['1-30_days', '31-60_days', '61-90_days', '91-180_days', '181-365_days', 'over_365_days']
        categories = ['personal_loan', 'mortgage', 'business_loan', 'credit_card']
        for cat in categories:
            for i, bucket in enumerate(buckets):
                count_val = max(0, int(rng.normal(50 - i * 8, 10)))
                amount = round(float(rng.lognormal(10, 1)) * count_val, 2)
                conn.execute(text(f"""
                    INSERT INTO warehouse_reporting.rpt_arrears_ageing
                        (report_date, product_category, ageing_bucket, account_count,
                         total_arrears_amount, total_outstanding, provision_amount)
                    VALUES ('{report_date}', '{cat}', '{bucket}',
                        {count_val}, {amount}, {amount * 1.5}, {amount * 0.3})
                """))
        conn.commit()
        count = conn.execute(text("SELECT COUNT(*) FROM warehouse_reporting.rpt_arrears_ageing")).scalar()
    print(f"    rpt_arrears_ageing: {count} rows")

    print("  ✓ Reporting layer populated")


def run():
    print("\n🏗️  Generating warehouse data...")
    print("  Loading staging layer...")
    populate_staging()
    print("  Building core dimensions...")
    populate_core_dimensions()
    print("  Building core facts...")
    populate_core_facts()
    print("  Building reporting layer...")
    populate_reporting()
    print("✅ Warehouse data complete\n")


if __name__ == '__main__':
    run()
