"""
pipeline_runner.py - Automated End-to-End MLOps & Data Engineering Pipeline

Orchestrates daily execution:
  1. Ingests latest SMARD market data & physical fundamentals.
  2. Syncs raw records to Supabase (PostgreSQL).
  3. Triggers the XGBoost forecasting model for Day-Ahead prices.
  4. Solves the BESS SciPy HiGHS Linear Programming dispatch optimization.
  5. Computes non-linear ASTM E1049-85 Rainflow degradation.
  6. Writes optimal dispatch schedule & executive KPIs to Supabase & exports Power BI CSVs.
"""

import os
import sys
import uuid
import argparse
from datetime import datetime, timezone
import numpy as np
import pandas as pd
import requests

from market_data import fetch_smard_fundamentals
from xgb_model import prepare_feature_dataset
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from bess_optimizer import BESSConfig, optimize_bess_dispatch
from degradation_model import BatteryDegradationModel

# Try to load environment variables from .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ==============================================================================
# Supabase Ingestion Client (Lightweight REST API using requests)
# ==============================================================================

class SupabasePipelineClient:
    """
    Direct HTTP client for Supabase PostgREST API.
    Does not require external heavy drivers and works seamlessly in containerized environments.
    """

    def __init__(self, url: str, key: str):
        self.base_url = url.rstrip("/")
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal,resolution=merge-duplicates",
        }

    def upsert_records(self, table: str, records: list) -> bool:
        if not records:
            return True
        endpoint = f"{self.base_url}/rest/v1/{table}"
        try:
            resp = requests.post(endpoint, json=records, headers=self.headers, timeout=30)
            if resp.status_code in [200, 201, 204]:
                return True
            else:
                print(f"[WARN] Supabase upsert to '{table}' returned HTTP {resp.status_code}: {resp.text[:300]}")
                return False
        except requests.RequestException as e:
            print(f"[WARN] Supabase connection error for table '{table}': {e}")
            return False


# ==============================================================================
# Pipeline Execution
# ==============================================================================

def run_daily_pipeline(
    weeks: int = 4,
    power_mw: float = 1.0,
    duration_h: float = 2.0,
    export_csv: bool = True,
    dry_run: bool = False,
) -> dict:
    run_id = str(uuid.uuid4())
    run_timestamp = datetime.now(timezone.utc)
    print("=" * 70)
    print(f"  BESS MLOps Automated Pipeline Execution | Run ID: {run_id}")
    print(f"  Timestamp: {run_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 70)

    # --------------------------------------------------------------------------
    # Step 1: Ingest SMARD Market Fundamentals
    # --------------------------------------------------------------------------
    print("\n[Step 1/5] Ingesting SMARD Market Fundamentals (Electricity Prices & Grid Physics)...")
    df = fetch_smard_fundamentals(weeks=weeks)
    if df is None or df.empty:
        raise RuntimeError("Failed to fetch SMARD API market data.")

    print(f"[OK] Retrieved {len(df)} hourly market records.")

    # --------------------------------------------------------------------------
    # Step 2: Supabase Ingestion (Raw Fundamentals)
    # --------------------------------------------------------------------------
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
    supabase_client = None

    if supabase_url and supabase_key and not dry_run:
        print("\n[Step 2/5] Syncing raw fundamentals to Supabase (public.market_fundamentals_hourly)...")
        supabase_client = SupabasePipelineClient(supabase_url, supabase_key)
        records_to_sync = []
        for _, row in df.iterrows():
            records_to_sync.append({
                "timestamp_ms": int(row["Timestamp"]),
                "datetime_utc": row["Datetime"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                "price_eur_mwh": round(float(row["Price_EUR_MWh"]), 2),
                "load_mwh": round(float(row.get("Load_MWh", 0.0)), 2),
                "residual_load_mwh": round(float(row.get("Residual_Load_MWh", 0.0)), 2),
                "solar_mwh": round(float(row.get("Solar_MWh", 0.0)), 2),
                "wind_onshore_mwh": round(float(row.get("Wind_Onshore_MWh", 0.0)), 2),
                "wind_offshore_mwh": round(float(row.get("Wind_Offshore_MWh", 0.0)), 2),
                "renewable_share": round(float(row.get("Renewable_Share", 0.0)), 4),
            })
        success = supabase_client.upsert_records("market_fundamentals_hourly", records_to_sync)
        if success:
            print(f"[OK] Ingested {len(records_to_sync)} records into Supabase.")
    else:
        print("\n[Step 2/5] Supabase sync skipped (SUPABASE_URL not configured or dry-run active).")

    # --------------------------------------------------------------------------
    # Step 3: Train XGBoost Model & Predict Next Day-Ahead Prices
    # --------------------------------------------------------------------------
    print("\n[Step 3/5] Training XGBoost Regressor to generate Day-Ahead price forecasts...")
    data, feature_cols = prepare_feature_dataset(df)
    X = data[feature_cols]
    y = data["Price_EUR_MWh"]
    split_idx = int(len(data) * 0.8)

    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    datetimes_test = data["Datetime"].iloc[split_idx:]

    xgb = XGBRegressor(
        n_estimators=250, learning_rate=0.04, max_depth=5,
        subsample=0.85, colsample_bytree=0.85, reg_alpha=0.1, reg_lambda=1.0,
        random_state=42, n_jobs=-1
    )
    xgb.fit(X_train, y_train)
    predictions = xgb.predict(X_test)
    actual_prices = y_test.values

    mae = float(mean_absolute_error(actual_prices, predictions))
    r2 = float(r2_score(actual_prices, predictions))
    print(f"[OK] Model Performance -> R2: {r2:.4f} | MAE: {mae:.2f} EUR/MWh")

    # --------------------------------------------------------------------------
    # Step 4: Solve BESS Optimization & Non-Linear Rainflow Degradation
    # --------------------------------------------------------------------------
    print("\n[Step 4/5] Solving BESS LP Dispatch Optimization (SciPy HiGHS) & Rainflow Fatigue...")
    capacity_mwh = power_mw * duration_h
    bess_cfg = BESSConfig(
        energy_capacity_mwh=capacity_mwh,
        power_capacity_mw=power_mw,
        charge_efficiency=0.93,
        discharge_efficiency=0.93,
        soc_min=0.10,
        soc_max=0.90,
        initial_soc=0.50,
        target_final_soc=0.50,
        degradation_cost_per_mwh=5.0,
    )

    # 1. Forecast-Driven Dispatch
    disp_forecast = optimize_bess_dispatch(predictions, bess_cfg, dt_hours=1.0)
    # 2. Perfect Foresight Benchmark
    disp_perfect = optimize_bess_dispatch(actual_prices, bess_cfg, dt_hours=1.0)

    p_ch = disp_forecast["p_charge_mw"]
    p_dis = disp_forecast["p_discharge_mw"]
    soc = disp_forecast["soc"]

    gross_rev = float(np.sum(p_dis * actual_prices))
    gross_cost = float(np.sum(p_ch * actual_prices))
    gross_profit = gross_rev - gross_cost

    # Rainflow non-linear fatigue
    deg_model = BatteryDegradationModel(ref_cycles_at_80_dod=6000.0, battery_capex_eur_per_kwh=140.0)
    sim_days = len(actual_prices) / 24.0
    rainflow_res = deg_model.evaluate_soc_degradation(
        soc_series=soc,
        battery_capacity_mwh=capacity_mwh,
        simulation_days=sim_days
    )
    wear_cost = rainflow_res["rainflow_degradation_cost_eur"]
    net_profit = gross_profit - wear_cost

    # Benchmark net profit
    perf_rev = float(np.sum(disp_perfect["p_discharge_mw"] * actual_prices))
    perf_cost = float(np.sum(disp_perfect["p_charge_mw"] * actual_prices))
    perf_net = (perf_rev - perf_cost) - wear_cost

    capture_ratio = (net_profit / max(perf_net, 1e-4)) * 100.0
    total_dis = float(np.sum(p_dis))
    total_ch = float(np.sum(p_ch))
    avg_dis_price = gross_rev / max(total_dis, 1e-4)
    avg_ch_price = gross_cost / max(total_ch, 1e-4)
    realized_spread = avg_dis_price - avg_ch_price
    usable_cap = (bess_cfg.soc_max - bess_cfg.soc_min) * capacity_mwh
    fce = (total_ch + total_dis) / (2.0 * usable_cap)

    print(f"[OK] Net Arbitrage Profit: {net_profit:,.2f} EUR (Value Capture: {capture_ratio:.1f}%)")
    print(f"[OK] Realized Spread: {realized_spread:.2f} EUR/MWh | Battery Life: {rainflow_res['projected_lifetime_years']:.1f} Years")

    # --------------------------------------------------------------------------
    # Step 5: Export to Power BI (CSV) and Supabase
    # --------------------------------------------------------------------------
    print("\n[Step 5/5] Exporting Data Feeds to Supabase & Power BI...")
    hourly_cashflows = (p_dis * actual_prices) - (p_ch * actual_prices) - (wear_cost / len(actual_prices))
    cumulative_profits = np.cumsum(hourly_cashflows)

    dispatch_records = []
    dispatch_rows = []
    for i in range(len(actual_prices)):
        dt_str = datetimes_test.iloc[i].strftime("%Y-%m-%dT%H:%M:%SZ")
        rec = {
            "run_id": run_id,
            "interval_datetime_utc": dt_str,
            "market_resolution": "1h",
            "actual_price_eur_mwh": round(float(actual_prices[i]), 2),
            "forecast_price_eur_mwh": round(float(predictions[i]), 2),
            "charge_power_mw": round(float(p_ch[i]), 3),
            "discharge_power_mw": round(float(p_dis[i]), 3),
            "net_power_mw": round(float(disp_forecast["net_power_mw"][i]), 3),
            "soc_pct": round(float(soc[i] * 100.0), 1),
            "hourly_cashflow_eur": round(float(hourly_cashflows[i]), 2),
            "cumulative_profit_eur": round(float(cumulative_profits[i]), 2),
        }
        dispatch_records.append(rec)
        dispatch_rows.append(rec)

    daily_kpi_record = {
        "run_id": run_id,
        "run_date": run_timestamp.strftime("%Y-%m-%d"),
        "model_name": "XGBoost-Phase3",
        "model_r2": round(r2, 4),
        "model_mae_eur_mwh": round(mae, 2),
        "battery_power_mw": power_mw,
        "battery_capacity_mwh": capacity_mwh,
        "net_profit_eur": round(net_profit, 2),
        "perfect_foresight_profit_eur": round(perf_net, 2),
        "value_capture_ratio_pct": round(capture_ratio, 2),
        "realized_spread_eur_mwh": round(realized_spread, 2),
        "avg_discharge_price_eur_mwh": round(avg_dis_price, 2),
        "avg_charge_price_eur_mwh": round(avg_ch_price, 2),
        "full_cycle_equivalents": round(fce, 2),
        "rainflow_wear_cost_eur": round(wear_cost, 2),
        "capacity_fade_pct": round(rainflow_res["capacity_fade_pct"], 4),
        "projected_lifetime_years": round(rainflow_res["projected_lifetime_years"], 1),
    }

    # Ingest to Supabase
    if supabase_client and not dry_run:
        supabase_client.upsert_records("bess_dispatch_forecasts", dispatch_records)
        supabase_client.upsert_records("bess_daily_kpis", [daily_kpi_record])
        print("[OK] Uploaded dispatch schedule and daily KPIs to Supabase.")

    # Export to CSV for Power BI Direct Integration
    if export_csv:
        output_dir = os.path.join(os.path.dirname(__file__), "data", "output")
        os.makedirs(output_dir, exist_ok=True)

        hourly_csv_path = os.path.join(output_dir, "powerbi_hourly_dispatch.csv")
        pd.DataFrame(dispatch_rows).to_csv(hourly_csv_path, index=False)

        kpis_csv_path = os.path.join(output_dir, "powerbi_daily_kpis.csv")
        pd.DataFrame([daily_kpi_record]).to_csv(kpis_csv_path, index=False)

        print(f"[OK] Power BI datasets exported:\n  -> {hourly_csv_path}\n  -> {kpis_csv_path}")

    print("\n" + "=" * 70)
    print("  Pipeline Completed Successfully!")
    print("=" * 70)

    return {
        "run_id": run_id,
        "net_profit_eur": net_profit,
        "r2_score": r2,
        "mae": mae,
        "value_capture_pct": capture_ratio,
        "realized_spread": realized_spread,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Automated BESS MLOps & Power Market Pipeline")
    parser.add_argument("--weeks", type=int, default=4, help="Historical weeks of training data")
    parser.add_argument("--power-mw", type=float, default=1.0, help="BESS power capacity in MW")
    parser.add_argument("--duration-h", type=float, default=2.0, help="BESS storage duration in hours")
    parser.add_argument("--dry-run", action="store_true", help="Run locally without remote DB writes")
    parser.add_argument("--no-csv", action="store_true", help="Disable local Power BI CSV exports")

    args = parser.parse_args()

    run_daily_pipeline(
        weeks=args.weeks,
        power_mw=args.power_mw,
        duration_h=args.duration_h,
        export_csv=not args.no_csv,
        dry_run=args.dry_run,
    )
