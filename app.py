"""
app.py - Interactive Streamlit Dashboard for BESS Arbitrage & Power Market Analytics

Enterprise-grade energy analytics dashboard featuring:
  1. Day-Ahead (Hourly) & Intraday (15-Minute) market resolutions with cloud ramp analytics.
  2. XGBoost and Random Forest ML price forecasting models.
  3. Real-time Linear Programming dispatch optimization (SciPy HiGHS).
  4. Non-Linear ASTM E1049-85 Rainflow Cycle Counting & SOH Battery Degradation.
"""

import time
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from market_data import fetch_smard_fundamentals
from intraday_market import fetch_smard_intraday, train_intraday_model
from xgb_model import prepare_feature_dataset
from xgboost import XGBRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from bess_optimizer import BESSConfig, optimize_bess_dispatch, backtest_bess_strategy
from degradation_model import BatteryDegradationModel


# ==============================================================================
# Streamlit Page Configuration
# ==============================================================================
st.set_page_config(
    page_title="BESS Arbitrage & Power Market Intelligence",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .main-title {
        font-size: 2.1rem;
        font-weight: 700;
        color: #1E3A8A;
        margin-bottom: 0.1rem;
    }
    .sub-title {
        font-size: 1.0rem;
        color: #4B5563;
        margin-bottom: 1.2rem;
    }
    .stMetric label {
        font-weight: 600 !important;
        color: #374151 !important;
    }
</style>
""", unsafe_allow_html=True)


# ==============================================================================
# Cached Data Fetchers & Model Engines
# ==============================================================================
@st.cache_data(ttl=3600, show_spinner="Fetching Day-Ahead Market Fundamentals (Hourly)...")
def get_dayahead_data(weeks: int = 4):
    df = fetch_smard_fundamentals(weeks=weeks)
    if df is None or df.empty:
        raise RuntimeError("Failed to fetch SMARD Day-Ahead data.")
    return df


@st.cache_data(ttl=3600, show_spinner="Fetching Intraday High-Frequency Data (15-Minute)...")
def get_intraday_data(weeks: int = 2):
    df = fetch_smard_intraday(weeks=weeks)
    if df is None or df.empty:
        raise RuntimeError("Failed to fetch SMARD 15-minute Intraday data.")
    return df


@st.cache_resource(show_spinner="Training Day-Ahead Forecasting Models...")
def get_dayahead_models(df: pd.DataFrame):
    data, feature_cols = prepare_feature_dataset(df)
    X = data[feature_cols]
    y = data["Price_EUR_MWh"]
    split_idx = int(len(data) * 0.8)

    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    dt_test = data["Datetime"].iloc[split_idx:]

    # XGBoost
    xgb = XGBRegressor(
        n_estimators=250, learning_rate=0.04, max_depth=5,
        subsample=0.85, colsample_bytree=0.85, reg_alpha=0.1, reg_lambda=1.0,
        random_state=42, n_jobs=-1
    )
    xgb.fit(X_train, y_train)
    p_xgb = xgb.predict(X_test)

    # Random Forest
    rf = RandomForestRegressor(
        n_estimators=150, max_depth=15, min_samples_leaf=2,
        random_state=42, n_jobs=-1
    )
    rf.fit(X_train, y_train)
    p_rf = rf.predict(X_test)

    return {
        "actual": y_test.values,
        "datetimes": dt_test.values,
        "test_df": data.iloc[split_idx:].copy().reset_index(drop=True),
        "XGBoost": {"preds": p_xgb, "mae": mean_absolute_error(y_test, p_xgb), "r2": r2_score(y_test, p_xgb)},
        "Random Forest": {"preds": p_rf, "mae": mean_absolute_error(y_test, p_rf), "r2": r2_score(y_test, p_rf)},
    }


@st.cache_resource(show_spinner="Training 15-Minute Intraday Forecasting Model...")
def get_intraday_model_cached(df_15m: pd.DataFrame):
    return train_intraday_model(df_15m)


# ==============================================================================
# Sidebar - Interactive Parameters
# ==============================================================================
st.sidebar.image("https://img.icons8.com/color/96/electricity.png", width=56)
st.sidebar.title("⚡ BESS Optimization Lab")

st.sidebar.markdown("### 🌐 Market Resolution")
market_resolution = st.sidebar.radio(
    "Select Market",
    ["Hourly (Day-Ahead - 60m)", "Quarter-Hourly (Intraday - 15m)"],
    index=0,
    help="Switch between standard hourly Day-Ahead trading and high-frequency 15-minute Intraday trading."
)
is_intraday = "15m" in market_resolution
dt_hours = 0.25 if is_intraday else 1.0

st.sidebar.markdown("---")
st.sidebar.markdown("### 🔋 Battery Storage System")
power_mw = st.sidebar.slider("Inverter Power (MW)", 0.5, 20.0, 1.0, 0.5)
duration_hours = st.sidebar.slider("Storage Duration (Hours)", 1.0, 8.0, 2.0, 0.5)
capacity_mwh = power_mw * duration_hours
st.sidebar.caption(f"**Usable Capacity:** `{capacity_mwh:.1f} MWh` ({duration_hours:.1f}C Duration)")

rte_pct = st.sidebar.slider("Round-Trip Efficiency (RTE %)", 75.0, 96.0, 86.5, 0.5)

col_s1, col_s2 = st.sidebar.columns(2)
with col_s1:
    soc_min = st.slider("Min SoC (%)", 5, 25, 10, step=5) / 100.0
with col_s2:
    soc_max = st.slider("Max SoC (%)", 75, 100, 90, step=5) / 100.0

st.sidebar.markdown("---")
st.sidebar.markdown("### 🔬 Battery Degradation Model")
degradation_mode = st.sidebar.radio(
    "Degradation Mechanism",
    ["Non-Linear Rainflow (ASTM E1049)", "Fixed Linear Baseline"],
    index=0,
    help="Rainflow counts individual half/full cycles and evaluates Wöhler DoD fatigue & SoC mean stress."
)
linear_deg_cost = st.sidebar.slider("Linear Degradation Rate (€/MWh)", 0.0, 15.0, 5.0, 0.5)


# ==============================================================================
# Model Training & Dispatch Execution
# ==============================================================================
if not is_intraday:
    # Hourly Day-Ahead Pipeline
    raw_df = get_dayahead_data(weeks=4)
    model_data = get_dayahead_models(raw_df)

    selected_model = st.sidebar.radio("Forecasting Algorithm", ["XGBoost", "Random Forest"], index=0)
    actual_prices = model_data["actual"]
    forecast_prices = model_data[selected_model]["preds"]
    datetimes = model_data["datetimes"]
    active_mae = model_data[selected_model]["mae"]
    active_r2 = model_data[selected_model]["r2"]
    test_context_df = model_data["test_df"]
else:
    # 15-Minute Intraday Pipeline
    raw_df = get_intraday_data(weeks=2)
    intraday_out = get_intraday_model_cached(raw_df)

    selected_model = "XGBoost (15-min Ramps)"
    actual_prices = intraday_out["actual"]
    forecast_prices = intraday_out["predictions"]
    datetimes = intraday_out["datetimes"]
    active_mae = intraday_out["mae"]
    active_r2 = intraday_out["r2"]
    test_context_df = intraday_out["test_df"]

# Configure BESS
one_way_eff = np.sqrt(rte_pct / 100.0)
bess_config = BESSConfig(
    energy_capacity_mwh=capacity_mwh,
    power_capacity_mw=power_mw,
    charge_efficiency=one_way_eff,
    discharge_efficiency=one_way_eff,
    soc_min=soc_min,
    soc_max=soc_max,
    initial_soc=0.50,
    target_final_soc=0.50,
    degradation_cost_per_mwh=linear_deg_cost,
)

# Solve Dispatch Optimization via Linear Programming
t_start = time.time()
# 1. Forecast-Driven
disp_forecast = optimize_bess_dispatch(forecast_prices, bess_config, dt_hours=dt_hours)
# 2. Perfect Foresight Benchmark
disp_perfect = optimize_bess_dispatch(actual_prices, bess_config, dt_hours=dt_hours)
t_solve_ms = (time.time() - t_start) * 1000

# Compute Post-Dispatch Economics & Degradation
p_ch = disp_forecast["p_charge_mw"]
p_dis = disp_forecast["p_discharge_mw"]
soc_series = disp_forecast["soc"]

gross_revenue = float(np.sum(p_dis * actual_prices * dt_hours))
gross_charge_cost = float(np.sum(p_ch * actual_prices * dt_hours))
gross_profit = gross_revenue - gross_charge_cost

# Fixed degradation cost
cost_fixed_deg = float(np.sum((p_ch + p_dis) * dt_hours) * linear_deg_cost)

# Non-linear Rainflow degradation
deg_model = BatteryDegradationModel(ref_cycles_at_80_dod=6000.0, battery_capex_eur_per_kwh=140.0)
sim_days = (len(actual_prices) * dt_hours) / 24.0
rainflow_res = deg_model.evaluate_soc_degradation(
    soc_series=soc_series,
    battery_capacity_mwh=capacity_mwh,
    simulation_days=sim_days
)
cost_rainflow_deg = rainflow_res["rainflow_degradation_cost_eur"]

# Choose active net profit based on selected degradation mechanism
if "Rainflow" in degradation_mode:
    active_deg_cost = cost_rainflow_deg
    active_net_profit = gross_profit - cost_rainflow_deg
else:
    active_deg_cost = cost_fixed_deg
    active_net_profit = gross_profit - cost_fixed_deg

# Benchmark metrics
perf_rev = float(np.sum(disp_perfect["p_discharge_mw"] * actual_prices * dt_hours))
perf_cost = float(np.sum(disp_perfect["p_charge_mw"] * actual_prices * dt_hours))
perf_gross = perf_rev - perf_cost
perf_net = perf_gross - (cost_rainflow_deg if "Rainflow" in degradation_mode else cost_fixed_deg)

capture_ratio = (active_net_profit / max(perf_net, 1e-4)) * 100.0

total_discharged_mwh = float(np.sum(p_dis * dt_hours))
total_charged_mwh = float(np.sum(p_ch * dt_hours))
avg_dis_price = gross_revenue / max(total_discharged_mwh, 1e-4)
avg_ch_price = gross_charge_cost / max(total_charged_mwh, 1e-4)
realized_spread = avg_dis_price - avg_ch_price


# ==============================================================================
# Header & KPI Metrics
# ==============================================================================
st.markdown('<div class="main-title">⚡ BESS Revenue Optimization & Intraday Trading Lab</div>', unsafe_allow_html=True)
market_tag = "15-Minute Intraday Continuous (GİP)" if is_intraday else "Hourly Day-Ahead (GÖP)"
st.markdown(
    f'<div class="sub-title">'
    f'Market: <b>{market_tag}</b> | '
    f'Asset: <b>{power_mw:.1f} MW / {capacity_mwh:.1f} MWh BESS</b> | '
    f'Forecasting: <b>{selected_model} (R²: {active_r2:.4f}, MAE: {active_mae:.2f} €/MWh)</b> | '
    f'Degradation: <b>{degradation_mode}</b>'
    f'</div>',
    unsafe_allow_html=True
)

m1, m2, m3, m4, m5 = st.columns(5)
with m1:
    st.metric(
        label="Net Arbitrage Profit",
        value=f"{active_net_profit:,.2f} €",
        delta=f"Benchmark: {perf_net:,.2f} €"
    )
with m2:
    st.metric(
        label="Value Capture Ratio",
        value=f"{capture_ratio:.1f} %",
        delta=f"Vs. 100% Perfect Foresight"
    )
with m3:
    st.metric(
        label="Realized Price Spread",
        value=f"{realized_spread:.2f} €/MWh",
        delta=f"Discharge: {avg_dis_price:.1f} € | Charge: {avg_ch_price:.1f} €"
    )
with m4:
    st.metric(
        label="Degradation Wear Cost",
        value=f"{active_deg_cost:,.2f} €",
        delta=f"Rainflow Cost: {cost_rainflow_deg:.2f} €"
    )
with m5:
    st.metric(
        label="Battery Lifetime Expectancy",
        value=f"{rainflow_res['projected_lifetime_years']:.1f} Years",
        delta=f"Capacity Fade: {rainflow_res['capacity_fade_pct']:.4f}% SOH"
    )

st.markdown("---")


# ==============================================================================
# Interactive Visual Tabs
# ==============================================================================
tab_dash, tab_deg, tab_ramp, tab_data = st.tabs([
    "📊 Dispatch & Arbitrage Dashboard",
    "🔬 Rainflow Degradation & SOH Analytics",
    "☁️ Renewable Ramps & Cloud Transients",
    "📋 Hourly / 15-Min Operational Log"
])

with tab_dash:
    timeline_idx = np.arange(len(actual_prices))

    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.07,
        subplot_titles=(
            f"1. Electricity Price Signals ({selected_model} Forecast vs Actual Price)",
            f"2. BESS Dispatch Schedule (Optimal Power: +Discharge / -Charge in MW)",
            f"3. State of Charge (SoC %) & Cumulative Net Arbitrage Profit (€)"
        ),
        specs=[[{"secondary_y": False}],
               [{"secondary_y": False}],
               [{"secondary_y": True}]]
    )

    # 1. Prices
    fig.add_trace(
        go.Scatter(x=timeline_idx, y=actual_prices, mode="lines", name="Actual Price (€/MWh)", line=dict(color="#1f77b4", width=2)),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(x=timeline_idx, y=forecast_prices, mode="lines", name="Model Predicted (€/MWh)", line=dict(color="#d62728", width=1.7, dash="dash")),
        row=1, col=1
    )

    # 2. Dispatch
    fig.add_trace(
        go.Bar(x=timeline_idx, y=disp_forecast["p_discharge_mw"], name="Discharge to Grid (+MW)", marker_color="#2ca02c", opacity=0.85),
        row=2, col=1
    )
    fig.add_trace(
        go.Bar(x=timeline_idx, y=-disp_forecast["p_charge_mw"], name="Charge from Grid (-MW)", marker_color="#ff7f0e", opacity=0.85),
        row=2, col=1
    )

    # 3. SoC and Cumulative Profit
    cashflows = (p_dis * actual_prices * dt_hours) - (p_ch * actual_prices * dt_hours) - (active_deg_cost / len(actual_prices))
    cum_profit = np.cumsum(cashflows)

    fig.add_trace(
        go.Scatter(x=timeline_idx, y=soc_series * 100.0, mode="lines", name="Battery SoC (%)", line=dict(color="#00b4d8", width=2.5)),
        row=3, col=1, secondary_y=False
    )
    fig.add_trace(
        go.Scatter(x=timeline_idx, y=cum_profit, mode="lines", name="Cumulative Net Profit (€)", line=dict(color="#2ca02c", width=2.5, dash="dashdot")),
        row=3, col=1, secondary_y=True
    )

    fig.update_layout(
        height=820,
        hovermode="x unified",
        margin=dict(l=40, r=40, t=50, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1.0),
    )
    fig.update_yaxes(title_text="Price (€/MWh)", row=1, col=1)
    fig.update_yaxes(title_text="Power (MW)", row=2, col=1, range=[-power_mw * 1.25, power_mw * 1.25])
    fig.update_yaxes(title_text="SoC (%)", range=[0, 100], row=3, col=1, secondary_y=False)
    fig.update_yaxes(title_text="Net Profit (€)", row=3, col=1, secondary_y=True)
    fig.update_xaxes(title_text="Timeline Intervals", row=3, col=1)

    st.plotly_chart(fig, use_container_width=True)

with tab_deg:
    st.markdown("### 🔬 Non-Linear ASTM E1049 Rainflow Cycle Counting & Fatigue Physics")
    st.markdown(
        "Standard market models penalize battery operation with an arbitrary flat rate (e.g. 5 €/MWh). "
        "In physical reality, battery degradation follows **Wöhler fatigue power-laws** where depth of discharge (DoD) "
        "exponentially dictates cycle damage, and resting at extreme SoC accelerates chemical electrolyte degradation."
    )

    col_d1, col_d2, col_d3 = st.columns(3)
    with col_d1:
        st.info(f"**Identified Rainflow Cycles:** `{rainflow_res['total_cycles_count']} cycles`")
    with col_d2:
        st.info(f"**Average Cycle Depth (DoD):** `{rainflow_res['avg_cycle_dod']*100:.1f}% DoD`")
    with col_d3:
        st.success(f"**Projected Life to 80% SOH:** `{rainflow_res['projected_lifetime_years']:.1f} Years`")

    # Degradation Comparison Bar Chart
    c_df = pd.DataFrame({
        "Mechanism": ["Linear Fixed (5 €/MWh)", "ASTM E1049 Rainflow"],
        "Wear Cost (€)": [cost_fixed_deg, cost_rainflow_deg],
        "Net Arbitrage Profit (€)": [gross_profit - cost_fixed_deg, gross_profit - cost_rainflow_deg]
    })
    st.table(c_df)

    if not rainflow_res["cycles_df"].empty:
        st.markdown("#### 📊 Cycle Depth of Discharge (DoD) Distribution")
        dod_fig = go.Figure()
        dod_fig.add_trace(go.Histogram(
            x=rainflow_res["cycles_df"]["range_dod"] * 100.0,
            nbinsx=15,
            marker_color="#3B82F6",
            opacity=0.8
        ))
        dod_fig.update_layout(
            title="Rainflow Extracted Cycle Depth Distribution (DoD %)",
            xaxis_title="Cycle Depth (DoD %)",
            yaxis_title="Number of Cycles",
            height=380,
        )
        st.plotly_chart(dod_fig, use_container_width=True)

with tab_ramp:
    st.markdown("### ☁️ High-Frequency Physical Ramps: Cloud Transients & Wind Volatility")
    st.markdown(
        "In 15-minute Intraday markets, sudden cloud cover transitions and wind drop-offs cause intra-hour generation "
        "deficits, driving steep price spikes that high-speed batteries can profitably mitigate."
    )

    if "Solar_MWh" in test_context_df.columns:
        ramp_fig = make_subplots(rows=2, cols=1, shared_xaxes=True, subplot_titles=("Renewable Generation Profile (MWh)", "15-Minute Generation Delta (Ramp Rates)"))

        ramp_fig.add_trace(go.Scatter(y=test_context_df["Solar_MWh"], name="Solar PV", line=dict(color="#f59e0b", width=2)), row=1, col=1)
        ramp_fig.add_trace(go.Scatter(y=test_context_df["Wind_Onshore_MWh"], name="Wind Onshore", line=dict(color="#10b981", width=2)), row=1, col=1)

        if "Solar_Ramp_15m" in test_context_df.columns:
            ramp_fig.add_trace(go.Bar(y=test_context_df["Solar_Ramp_15m"], name="Solar Cloud Transient Ramp", marker_color="#f59e0b", opacity=0.7), row=2, col=1)
            ramp_fig.add_trace(go.Bar(y=test_context_df["Wind_Ramp_15m"], name="Wind Ramp", marker_color="#10b981", opacity=0.7), row=2, col=1)

        ramp_fig.update_layout(height=520, hovermode="x unified", margin=dict(l=40, r=40, t=50, b=30))
        st.plotly_chart(ramp_fig, use_container_width=True)
    else:
        st.info("Switch to 'Quarter-Hourly (Intraday - 15m)' in the sidebar to view cloud transient ramp dynamics.")

with tab_data:
    st.markdown("### 📋 Operational Dispatch Schedule Log")
    log_df = pd.DataFrame({
        "Interval": timeline_idx,
        "Actual_Price_EUR_MWh": np.round(actual_prices, 2),
        "Forecast_Price_EUR_MWh": np.round(forecast_prices, 2),
        "Charge_Power_MW": np.round(p_ch, 3),
        "Discharge_Power_MW": np.round(p_dis, 3),
        "Net_Power_MW": np.round(disp_forecast["net_power_mw"], 3),
        "Battery_SoC_Pct": np.round(soc_series * 100.0, 1),
        "Cumulative_Profit_EUR": np.round(cum_profit, 2),
    })
    st.dataframe(log_df, use_container_width=True, height=380)

    csv = log_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download Complete Schedule (CSV)",
        data=csv,
        file_name=f"bess_dispatch_{'15m' if is_intraday else '1h'}.csv",
        mime="text/csv"
    )
