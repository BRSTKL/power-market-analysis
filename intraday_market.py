"""
intraday_market.py - 15-Minute Intraday Market Pipeline & Ramp Analytics

Fetches 15-minute (quarterhour) power market data from SMARD API to model
fast physical ramps (cloud cover transients on solar, wind gusts, load spikes).
Provides high-resolution forecasting and dispatch optimization for BESS.
"""

from typing import Optional, Dict, Any, Tuple
import requests
import numpy as np
import pandas as pd
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, r2_score

from bess_optimizer import BESSConfig, optimize_bess_dispatch
from degradation_model import BatteryDegradationModel


# ==============================================================================
# 1. 15-Minute Intraday Data Fetcher
# ==============================================================================

def fetch_smard_intraday(
    weeks: int = 2,
    region: str = "DE",
) -> Optional[pd.DataFrame]:
    """
    Fetch 15-minute (quarter-hour) resolution data from SMARD API:
      - 4169: Intraday / Market Price (EUR/MWh)
      - 410:  Grid Load (MWh / quarter-hour)
      - 4068: Solar Generation (MWh / quarter-hour)
      - 4067: Wind Onshore Generation (MWh / quarter-hour)
      - 1225: Wind Offshore Generation (MWh / quarter-hour)

    Parameters
    ----------
    weeks : int
        Number of historical weeks (each week has 672 quarter-hours).
    region : str
        Market region code (default: 'DE').

    Returns
    -------
    pd.DataFrame or None
        Quarter-hourly DataFrame with physical ramp indicators.
    """
    filter_map = {
        "Price_EUR_MWh": 4169,
        "Load_MWh": 410,
        "Solar_MWh": 4068,
        "Wind_Onshore_MWh": 4067,
        "Wind_Offshore_MWh": 1225,
    }

    # 1. Get available 15-minute timestamps
    index_url = f"https://www.smard.de/app/chart_data/4169/{region}/index_quarterhour.json"
    print(f"Fetching 15-minute timestamp index ({weeks} weeks)...")
    try:
        resp = requests.get(index_url, timeout=30)
        resp.raise_for_status()
        all_ts = resp.json().get("timestamps", [])
    except requests.RequestException as e:
        print(f"Failed to fetch 15-minute index: {e}")
        return None

    if not all_ts:
        return None

    target_ts = all_ts[-weeks:]
    print(f"Retrieving {len(target_ts)} week(s) of 15-minute data ({len(target_ts) * 672} intervals)...")

    dfs = {}
    for col_name, fid in filter_map.items():
        all_series = []
        for ts in target_ts:
            url = f"https://www.smard.de/app/chart_data/{fid}/{region}/{fid}_{region}_quarterhour_{ts}.json"
            try:
                r = requests.get(url, timeout=25)
                if r.status_code == 200:
                    data = r.json().get("series", [])
                    all_series.extend(data)
            except requests.RequestException:
                pass

        df_t = pd.DataFrame(all_series, columns=["Timestamp", col_name]).dropna()
        df_t.drop_duplicates(subset=["Timestamp"], inplace=True)
        dfs[col_name] = df_t

    # Merge on Timestamp
    merged = dfs["Price_EUR_MWh"]
    for col_name in list(filter_map.keys())[1:]:
        if col_name in dfs and not dfs[col_name].empty:
            merged = pd.merge(merged, dfs[col_name], on="Timestamp", how="inner")

    if merged.empty:
        return None

    merged["Datetime"] = pd.to_datetime(merged["Timestamp"], unit="ms")
    merged.sort_values("Datetime", inplace=True)
    merged.reset_index(drop=True, inplace=True)

    # --------------------------------------------------------------------------
    # 2. High-Frequency Physical Ramp Features (Cloud Transients & Wind Swings)
    # --------------------------------------------------------------------------
    merged["Total_Renewable_MWh"] = (
        merged["Solar_MWh"] + merged["Wind_Onshore_MWh"] + merged.get("Wind_Offshore_MWh", 0.0)
    )
    merged["Residual_Load_MWh"] = merged["Load_MWh"] - merged["Total_Renewable_MWh"]

    # 15-minute ramp rates (delta from previous 15-minute interval)
    merged["Solar_Ramp_15m"] = merged["Solar_MWh"].diff(1)       # Captures cloud cover transients
    merged["Wind_Ramp_15m"] = merged["Wind_Onshore_MWh"].diff(1) # Wind drop-off/surge
    merged["Load_Ramp_15m"] = merged["Load_MWh"].diff(1)
    merged["Residual_Ramp_15m"] = merged["Residual_Load_MWh"].diff(1)
    merged["Price_Ramp_15m"] = merged["Price_EUR_MWh"].diff(1)

    # Time and quarter indicators
    merged["Hour"] = merged["Datetime"].dt.hour
    merged["Minute"] = merged["Datetime"].dt.minute
    merged["Quarter_Index"] = merged["Minute"] // 15  # 0, 1, 2, 3
    merged["DayOfWeek"] = merged["Datetime"].dt.dayofweek

    # Multi-step lags
    merged["Price_Lag_1_15m"] = merged["Price_EUR_MWh"].shift(1)
    merged["Price_Lag_4_1h"] = merged["Price_EUR_MWh"].shift(4)
    merged["Price_Lag_96_24h"] = merged["Price_EUR_MWh"].shift(96)  # 24h = 96 * 15m

    merged.dropna(inplace=True)
    merged.reset_index(drop=True, inplace=True)

    print(f"[OK] Fetched {len(merged)} 15-minute intervals with physical ramps.")
    return merged


# ==============================================================================
# 2. 15-Minute Intraday XGBoost Forecasting Model
# ==============================================================================

def train_intraday_model(df_15m: pd.DataFrame) -> Dict[str, Any]:
    """
    Train an XGBoost model tailored to 15-minute ramp dynamics.
    """
    feature_cols = [
        "Hour", "Minute", "Quarter_Index", "DayOfWeek",
        "Load_MWh", "Solar_MWh", "Wind_Onshore_MWh", "Total_Renewable_MWh",
        "Residual_Load_MWh",
        "Solar_Ramp_15m", "Wind_Ramp_15m", "Residual_Ramp_15m",
        "Price_Lag_1_15m", "Price_Lag_4_1h", "Price_Lag_96_24h"
    ]

    X = df_15m[feature_cols]
    y = df_15m["Price_EUR_MWh"]
    split_idx = int(len(df_15m) * 0.8)

    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    datetimes_test = df_15m["Datetime"].iloc[split_idx:]

    print(f"Training 15-Minute Intraday XGBoost Model ({len(X_train)} train, {len(X_test)} test)...")
    xgb = XGBRegressor(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.85,
        colsample_bytree=0.85,
        random_state=42,
        n_jobs=-1
    )
    xgb.fit(X_train, y_train)

    preds = xgb.predict(X_test)
    mae = mean_absolute_error(y_test, preds)
    r2 = r2_score(y_test, preds)

    print(f"[OK] Intraday 15-min Model -> MAE: {mae:.2f} EUR/MWh | R2: {r2:.4f}")

    return {
        "model": xgb,
        "predictions": preds,
        "actual": y_test.values,
        "datetimes": datetimes_test.values,
        "mae": mae,
        "r2": r2,
        "test_df": df_15m.iloc[split_idx:].copy().reset_index(drop=True),
    }


# ==============================================================================
# 3. 15-Minute BESS Dispatch with Rainflow Degradation Analysis
# ==============================================================================

def run_intraday_bess_optimization(
    actual_prices: np.ndarray,
    forecast_prices: np.ndarray,
    config: BESSConfig,
) -> Dict[str, Any]:
    """
    Run high-resolution 15-minute dispatch optimization (dt = 0.25 hours)
    and evaluate both linear and non-linear Rainflow degradation.
    """
    dt_hours = 0.25  # 15 minutes = 0.25 h
    T = len(actual_prices)

    # 1. Dispatch optimization on forecast prices
    disp = optimize_bess_dispatch(forecast_prices, config, dt_hours=dt_hours)

    p_ch = disp["p_charge_mw"]
    p_dis = disp["p_discharge_mw"]
    soc = disp["soc"]

    # 2. Hourly equivalent cashflows (settled against actual 15-min prices)
    revenue = np.sum(p_dis * actual_prices * dt_hours)
    charging_cost = np.sum(p_ch * actual_prices * dt_hours)
    gross_profit = float(revenue - charging_cost)

    # 3. Fixed Linear Degradation baseline (5 €/MWh throughput)
    fixed_deg_cost = float(np.sum((p_ch + p_dis) * dt_hours) * config.degradation_cost_per_mwh)
    net_profit_fixed = gross_profit - fixed_deg_cost

    # 4. Non-Linear Rainflow Cycle Counting Degradation
    deg_model = BatteryDegradationModel(
        ref_cycles_at_80_dod=6000.0,
        battery_capex_eur_per_kwh=140.0,
    )
    sim_days = (T * dt_hours) / 24.0
    rainflow_eval = deg_model.evaluate_soc_degradation(
        soc_series=soc,
        battery_capacity_mwh=config.energy_capacity_mwh,
        simulation_days=sim_days
    )

    rainflow_deg_cost = rainflow_eval["rainflow_degradation_cost_eur"]
    net_profit_rainflow = gross_profit - rainflow_deg_cost

    return {
        "gross_profit_eur": gross_profit,
        "revenue_eur": float(revenue),
        "charging_cost_eur": float(charging_cost),
        "fixed_deg_cost_eur": fixed_deg_cost,
        "net_profit_fixed_deg_eur": net_profit_fixed,
        "rainflow_deg_cost_eur": rainflow_deg_cost,
        "net_profit_rainflow_eur": net_profit_rainflow,
        "capacity_fade_pct": rainflow_eval["capacity_fade_pct"],
        "projected_lifetime_years": rainflow_eval["projected_lifetime_years"],
        "total_cycles_count": rainflow_eval["total_cycles_count"],
        "avg_cycle_dod": rainflow_eval["avg_cycle_dod"],
        "dispatch": disp,
        "rainflow_eval": rainflow_eval,
    }


if __name__ == "__main__":
    print("=" * 65)
    print("  SMARD 15-Minute Intraday & Rainflow Degradation Pipeline")
    print("=" * 65)

    df_15m = fetch_smard_intraday(weeks=2)
    if df_15m is not None:
        model_out = train_intraday_model(df_15m)

        bess_cfg = BESSConfig(
            energy_capacity_mwh=2.0,
            power_capacity_mw=1.0,
            charge_efficiency=0.93,
            discharge_efficiency=0.93,
            soc_min=0.10,
            soc_max=0.90,
            degradation_cost_per_mwh=5.0,
        )

        opt_out = run_intraday_bess_optimization(
            actual_prices=model_out["actual"],
            forecast_prices=model_out["predictions"],
            config=bess_cfg,
        )

        print("\n" + "=" * 55)
        print("  15-Minute Intraday BESS Performance")
        print("=" * 55)
        print(f"Gross Arbitrage Profit       : {opt_out['gross_profit_eur']:.2f} €")
        print(f"Fixed Linear Deg Cost (5€)   : {opt_out['fixed_deg_cost_eur']:.2f} €")
        print(f"Net Profit (Fixed Deg)       : {opt_out['net_profit_fixed_deg_eur']:.2f} €")
        print("-" * 55)
        print(f"Rainflow Wear Cost           : {opt_out['rainflow_deg_cost_eur']:.2f} €")
        print(f"Net Profit (Rainflow Deg)    : {opt_out['net_profit_rainflow_eur']:.2f} €")
        print(f"Rainflow Cycles Identified   : {opt_out['total_cycles_count']}")
        print(f"Avg Cycle Depth (DoD)        : {opt_out['avg_cycle_dod'] * 100:.1f}%")
        print(f"Capacity Fade (% SOH loss)   : {opt_out['capacity_fade_pct']:.4f}%")
        print(f"Projected Battery Life       : {opt_out['projected_lifetime_years']:.1f} years")
        print("=" * 55)
