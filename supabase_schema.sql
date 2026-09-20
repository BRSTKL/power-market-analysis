-- ==============================================================================
-- Supabase / PostgreSQL Production Schema for BESS Arbitrage & Power Analytics
-- Database: PostgreSQL 15+ (Supabase)
-- Purpose: Ingest SMARD market data, store ML forecasts, BESS dispatch, and serve Power BI
-- ==============================================================================

-- 1. Enable UUID extension if not enabled
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ------------------------------------------------------------------------------
-- Table 1: Raw & Cleaned Hourly Market Fundamentals (SMARD API)
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.market_fundamentals_hourly (
    id BIGSERIAL PRIMARY KEY,
    timestamp_ms BIGINT NOT NULL UNIQUE,
    datetime_utc TIMESTAMPTZ NOT NULL,
    price_eur_mwh NUMERIC(10, 2) NOT NULL,
    load_mwh NUMERIC(12, 2),
    residual_load_mwh NUMERIC(12, 2),
    solar_mwh NUMERIC(12, 2),
    wind_onshore_mwh NUMERIC(12, 2),
    wind_offshore_mwh NUMERIC(12, 2),
    renewable_share NUMERIC(6, 4),
    created_at TIMESTAMPTZ DEFAULT TIMEZONE('utc', NOW())
);

CREATE INDEX IF NOT EXISTS idx_market_datetime ON public.market_fundamentals_hourly (datetime_utc DESC);
CREATE INDEX IF NOT EXISTS idx_market_timestamp ON public.market_fundamentals_hourly (timestamp_ms);

-- ------------------------------------------------------------------------------
-- Table 2: BESS Hourly Dispatch & Price Forecasts
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.bess_dispatch_forecasts (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    interval_datetime_utc TIMESTAMPTZ NOT NULL,
    market_resolution VARCHAR(10) DEFAULT '1h', -- '1h' or '15m'
    actual_price_eur_mwh NUMERIC(10, 2),
    forecast_price_eur_mwh NUMERIC(10, 2) NOT NULL,
    charge_power_mw NUMERIC(10, 3) NOT NULL,
    discharge_power_mw NUMERIC(10, 3) NOT NULL,
    net_power_mw NUMERIC(10, 3) NOT NULL,
    soc_pct NUMERIC(5, 2) NOT NULL,
    hourly_cashflow_eur NUMERIC(12, 2) NOT NULL,
    cumulative_profit_eur NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT TIMEZONE('utc', NOW())
);

CREATE INDEX IF NOT EXISTS idx_dispatch_datetime ON public.bess_dispatch_forecasts (interval_datetime_utc DESC);
CREATE INDEX IF NOT EXISTS idx_dispatch_run_id ON public.bess_dispatch_forecasts (run_id);

-- ------------------------------------------------------------------------------
-- Table 3: Daily Summary & MLOps KPIs (For High-Level Executive Dashboards)
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.bess_daily_kpis (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL UNIQUE,
    run_date DATE NOT NULL,
    model_name VARCHAR(50) NOT NULL,           -- e.g. 'XGBoost-v2'
    model_r2 NUMERIC(6, 4),
    model_mae_eur_mwh NUMERIC(8, 2),
    battery_power_mw NUMERIC(8, 2) NOT NULL,
    battery_capacity_mwh NUMERIC(8, 2) NOT NULL,
    net_profit_eur NUMERIC(12, 2) NOT NULL,
    perfect_foresight_profit_eur NUMERIC(12, 2) NOT NULL,
    value_capture_ratio_pct NUMERIC(6, 2) NOT NULL,
    realized_spread_eur_mwh NUMERIC(8, 2) NOT NULL,
    avg_discharge_price_eur_mwh NUMERIC(8, 2) NOT NULL,
    avg_charge_price_eur_mwh NUMERIC(8, 2) NOT NULL,
    full_cycle_equivalents NUMERIC(6, 2) NOT NULL,
    rainflow_wear_cost_eur NUMERIC(10, 2) NOT NULL,
    capacity_fade_pct NUMERIC(6, 4),
    projected_lifetime_years NUMERIC(5, 1),
    created_at TIMESTAMPTZ DEFAULT TIMEZONE('utc', NOW())
);

CREATE INDEX IF NOT EXISTS idx_daily_kpis_date ON public.bess_daily_kpis (run_date DESC);

-- ------------------------------------------------------------------------------
-- Views for Direct Power BI / Metabase / Tableau Integration
-- ------------------------------------------------------------------------------

-- View 1: Power BI Hourly Dispatch Feed (DirectQuery ready)
CREATE OR REPLACE VIEW public.v_powerbi_hourly_dispatch AS
SELECT
    f.id,
    f.run_id,
    f.interval_datetime_utc,
    f.interval_datetime_utc AT TIME ZONE 'Europe/Berlin' AS datetime_local_berlin,
    f.actual_price_eur_mwh,
    f.forecast_price_eur_mwh,
    (f.actual_price_eur_mwh - f.forecast_price_eur_mwh) AS forecast_error_eur_mwh,
    f.charge_power_mw,
    f.discharge_power_mw,
    f.net_power_mw,
    f.soc_pct,
    f.hourly_cashflow_eur,
    f.cumulative_profit_eur,
    m.load_mwh,
    m.residual_load_mwh,
    m.solar_mwh,
    m.wind_onshore_mwh,
    m.renewable_share
FROM public.bess_dispatch_forecasts f
LEFT JOIN public.market_fundamentals_hourly m
    ON f.interval_datetime_utc = m.datetime_utc
ORDER BY f.interval_datetime_utc ASC;

-- View 2: Power BI Executive KPI Summary
CREATE OR REPLACE VIEW public.v_powerbi_executive_summary AS
SELECT
    run_date,
    model_name,
    model_r2,
    model_mae_eur_mwh,
    battery_power_mw || ' MW / ' || battery_capacity_mwh || ' MWh' AS bess_asset_name,
    net_profit_eur,
    perfect_foresight_profit_eur,
    value_capture_ratio_pct,
    realized_spread_eur_mwh,
    avg_discharge_price_eur_mwh,
    avg_charge_price_eur_mwh,
    full_cycle_equivalents,
    rainflow_wear_cost_eur,
    projected_lifetime_years
FROM public.bess_daily_kpis
ORDER BY run_date DESC;

-- ------------------------------------------------------------------------------
-- Row Level Security (RLS) Configuration (Read access for BI dashboards)
-- ------------------------------------------------------------------------------
ALTER TABLE public.market_fundamentals_hourly ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.bess_dispatch_forecasts ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.bess_daily_kpis ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow public read access on market fundamentals"
    ON public.market_fundamentals_hourly FOR SELECT USING (true);

CREATE POLICY "Allow public read access on dispatch forecasts"
    ON public.bess_dispatch_forecasts FOR SELECT USING (true);

CREATE POLICY "Allow public read access on daily kpis"
    ON public.bess_daily_kpis FOR SELECT USING (true);

-- Allow service role full CRUD operations for automated pipeline ingestion
CREATE POLICY "Allow service role full access on fundamentals"
    ON public.market_fundamentals_hourly FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Allow service role full access on dispatch"
    ON public.bess_dispatch_forecasts FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Allow service role full access on kpis"
    ON public.bess_daily_kpis FOR ALL USING (auth.role() = 'service_role');
