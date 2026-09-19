"""
app.py - Interactive Streamlit Dashboard for BESS Arbitrage & Power Market Analytics

Allows users and interviewers/juries to interactively adjust battery capacity (MW/MWh),
efficiency, degradation cost, and choose between XGBoost and Random Forest forecasting
models to view real-time linear programming dispatch optimization.
"""

import time
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from market_data import fetch_smard_fundamentals
from xgb_model import prepare_feature_dataset
from xgboost import XGBRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from bess_optimizer import BESSConfig, optimize_bess_dispatch, backtest_bess_strategy


# ==============================================================================
# Streamlit Page Setup
# ==============================================================================
st.set_page_config(
    page_title="BESS Arbitrage & Power Market Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for executive presentation styling
st.markdown("""
<style>
    .main-title {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1E3A8A;
        margin-bottom: 0.2rem;
    }
    .sub-title {
        font-size: 1.05rem;
        color: #4B5563;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background-color: #F3F4F6;
        border-radius: 10px;
        padding: 15px;
        border-left: 5px solid #2563EB;
    }
    .stMetric label {
        font-weight: 600 !important;
        color: #374151 !important;
    }
</style>
""", unsafe_allow_html=True)


# ==============================================================================
# Cached Data Fetching & Model Training
# ==============================================================================
@st.cache_data(ttl=3600, show_spinner="Fetching German Power Market Data (SMARD API)...")
def get_market_data(weeks: int = 4):
    df = fetch_smard_fundamentals(weeks=weeks)
    if df is None or df.empty:
        raise RuntimeError("Failed to fetch SMARD API data.")
    return df


@st.cache_resource(show_spinner="Training Price Forecasting Models...")
def train_forecasting_models(df: pd.DataFrame):
    data, feature_cols = prepare_feature_dataset(df)
    X = data[feature_cols]
    y = data["Price_EUR_MWh"]
    datetimes = data["Datetime"]

    split_idx = int(len(data) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    dt_test = datetimes.iloc[split_idx:]

    # 1. XGBoost
    xgb = XGBRegressor(
        n_estimators=250, learning_rate=0.04, max_depth=5,
        subsample=0.85, colsample_bytree=0.85, reg_alpha=0.1, reg_lambda=1.0,
        random_state=42, n_jobs=-1
    )
    xgb.fit(X_train, y_train)
    p_xgb = xgb.predict(X_test)
    mae_xgb = mean_absolute_error(y_test, p_xgb)
    r2_xgb = r2_score(y_test, p_xgb)

    # 2. Random Forest
    rf = RandomForestRegressor(
        n_estimators=150, max_depth=15, min_samples_leaf=2,
        random_state=42, n_jobs=-1
    )
    rf.fit(X_train, y_train)
    p_rf = rf.predict(X_test)
    mae_rf = mean_absolute_error(y_test, p_rf)
    r2_rf = r2_score(y_test, p_rf)

    return {
        "actual": y_test.values,
        "datetimes": dt_test.values,
        "XGBoost": {
            "predictions": p_xgb,
            "mae": mae_xgb,
            "r2": r2_xgb,
            "model": xgb
        },
        "Random Forest": {
            "predictions": p_rf,
            "mae": mae_rf,
            "r2": r2_rf,
            "model": rf
        },
    }


# ==============================================================================
# Sidebar - Interactive Parameters
# ==============================================================================
st.sidebar.image("https://img.icons8.com/color/96/electricity.png", width=64)
st.sidebar.title("⚡ BESS Configuration")

st.sidebar.markdown("### 🔋 Battery Storage Parameters")
power_mw = st.sidebar.slider(
    "Inverter Power (MW)",
    min_value=0.5,
    max_value=20.0,
    value=1.0,
    step=0.5,
    help="Maximum continuous charge/discharge power capacity in Megawatts."
)

duration_hours = st.sidebar.slider(
    "Storage Duration (Hours)",
    min_value=1.0,
    max_value=8.0,
    value=2.0,
    step=0.5,
    help="Energy-to-power ratio (e.g. 2h duration for 1 MW = 2 MWh capacity)."
)
capacity_mwh = power_mw * duration_hours
st.sidebar.caption(f"**Total Usable Capacity:** `{capacity_mwh:.1f} MWh`")

rte_pct = st.sidebar.slider(
    "Round-Trip Efficiency (RTE %)",
    min_value=75.0,
    max_value=95.0,
    value=86.5,
    step=0.5,
    help="Combined AC-AC round-trip efficiency of the BESS."
)

col_s1, col_s2 = st.sidebar.columns(2)
with col_s1:
    soc_min = st.slider("Min SoC (%)", 5, 25, 10, step=5) / 100.0
with col_s2:
    soc_max = st.slider("Max SoC (%)", 75, 100, 90, step=5) / 100.0

degradation_cost = st.sidebar.slider(
    "Degradation Cost (€/MWh)",
    min_value=0.0,
    max_value=15.0,
    value=5.0,
    step=0.5,
    help="Marginal battery cell wear & cycling cost per MWh throughput."
)

st.sidebar.markdown("---")
st.sidebar.markdown("### 🤖 ML Forecasting Model")
selected_model_name = st.sidebar.radio(
    "Select Price Forecaster",
    ["XGBoost", "Random Forest"],
    index=0,
    help="XGBoost offers lower MAE and higher R² accuracy."
)


# ==============================================================================
# Data Loading & Processing
# ==============================================================================
try:
    raw_df = get_market_data(weeks=4)
    model_results = train_forecasting_models(raw_df)
except Exception as e:
    st.error(f"Error loading data or training models: {e}")
    st.stop()

actual_prices = model_results["actual"]
datetimes = model_results["datetimes"]
active_model = model_results[selected_model_name]
forecast_prices = active_model["predictions"]

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
    degradation_cost_per_mwh=degradation_cost,
)

# Solve Optimizations on the fly (instant via SciPy HiGHS)
t_opt_start = time.time()
# 1. Forecast-Driven Dispatch
disp_forecast = optimize_bess_dispatch(forecast_prices, bess_config)
kpis_forecast = backtest_bess_strategy(actual_prices, disp_forecast, bess_config)

# 2. Perfect Foresight Benchmark
disp_perfect = optimize_bess_dispatch(actual_prices, bess_config)
kpis_perfect = backtest_bess_strategy(actual_prices, disp_perfect, bess_config)
t_opt_duration = (time.time() - t_opt_start) * 1000

capture_ratio = (
    (kpis_forecast["net_profit_eur"] / kpis_perfect["net_profit_eur"]) * 100.0
    if kpis_perfect["net_profit_eur"] > 0 else 0.0
)


# ==============================================================================
# Main Dashboard UI
# ==============================================================================
st.markdown('<div class="main-title">⚡ BESS Price Arbitrage & Dispatch Optimization</div>', unsafe_allow_html=True)
st.markdown(
    f'<div class="sub-title">'
    f'German Day-Ahead Electricity Market (SMARD API) | <b>{bess_config.power_capacity_mw:.1f} MW / {bess_config.energy_capacity_mwh:.1f} MWh BESS</b> | '
    f'ML Model: <b>{selected_model_name} (R²: {active_model["r2"]:.4f}, MAE: {active_model["mae"]:.2f} €)</b> | '
    f'LP Solver: <b>SciPy HiGHS ({t_opt_duration:.1f} ms)</b>'
    f'</div>',
    unsafe_allow_html=True
)

# ------------------------------------------------------------------------------
# KPI Scorecards
# ------------------------------------------------------------------------------
kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

with kpi1:
    st.metric(
        label="Net Arbitrage Profit",
        value=f"{kpis_forecast['net_profit_eur']:,.2f} €",
        delta=f"Benchmark: {kpis_perfect['net_profit_eur']:,.2f} €"
    )
with kpi2:
    st.metric(
        label="Value Capture Ratio",
        value=f"{capture_ratio:.1f} %",
        delta=f"Model: {selected_model_name}"
    )
with kpi3:
    st.metric(
        label="Realized Price Spread",
        value=f"{kpis_forecast['realized_spread_eur_mwh']:.2f} €/MWh",
        delta=f"Discharge: {kpis_forecast['avg_discharge_price_eur_mwh']:.1f} € | Charge: {kpis_forecast['avg_charge_price_eur_mwh']:.1f} €"
    )
with kpi4:
    st.metric(
        label="Full Cycles (FCE)",
        value=f"{kpis_forecast['fce_cycles']:.2f} cycles",
        delta=f"{kpis_forecast['fce_cycles'] / (len(actual_prices)/24.0):.2f} cycles/day"
    )
with kpi5:
    st.metric(
        label="Throughput Discharged",
        value=f"{kpis_forecast['energy_discharged_mwh']:.1f} MWh",
        delta=f"Charged: {kpis_forecast['energy_charged_mwh']:.1f} MWh"
    )

st.markdown("---")

# ------------------------------------------------------------------------------
# Tabs: Dashboard, Benchmarks, Data Export
# ------------------------------------------------------------------------------
tab1, tab2, tab3 = st.tabs([
    "📊 Dispatch & Arbitrage Dashboard",
    "⚖️ Strategy Benchmark (Forecast vs Perfect)",
    "📋 Hourly Schedule & Data Export"
])

with tab1:
    # Build 3-panel interactive Plotly Figure
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.07,
        subplot_titles=(
            f"1. Day-Ahead Electricity Prices ({selected_model_name} Forecast vs Actual)",
            f"2. BESS Dispatch Schedule (Power Profile: +Discharge / -Charge in MW)",
            f"3. Battery State of Charge (SoC %) & Cumulative Net Arbitrage Profit (€)"
        ),
        specs=[[{"secondary_y": False}],
               [{"secondary_y": False}],
               [{"secondary_y": True}]]
    )

    hours_idx = np.arange(len(actual_prices))

    # --- Subplot 1: Prices ---
    fig.add_trace(
        go.Scatter(
            x=hours_idx, y=actual_prices,
            mode='lines', name='Actual Price (€/MWh)',
            line=dict(color='#1f77b4', width=2)
        ),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(
            x=hours_idx, y=forecast_prices,
            mode='lines', name=f'{selected_model_name} Predicted (€/MWh)',
            line=dict(color='#d62728', width=1.8, dash='dash')
        ),
        row=1, col=1
    )

    # --- Subplot 2: Dispatch Power ---
    fig.add_trace(
        go.Bar(
            x=hours_idx, y=disp_forecast["p_discharge_mw"],
            name='Discharge to Grid (+MW)',
            marker_color='#2ca02c', opacity=0.85
        ),
        row=2, col=1
    )
    fig.add_trace(
        go.Bar(
            x=hours_idx, y=-disp_forecast["p_charge_mw"],
            name='Charge from Grid (-MW)',
            marker_color='#ff7f0e', opacity=0.85
        ),
        row=2, col=1
    )

    # --- Subplot 3: SoC and Cumulative Cash Flow ---
    fig.add_trace(
        go.Scatter(
            x=hours_idx, y=disp_forecast["soc"] * 100.0,
            mode='lines', name='Battery SoC (%)',
            line=dict(color='#00b4d8', width=2.5)
        ),
        row=3, col=1, secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=hours_idx, y=kpis_forecast["cumulative_profit_eur"],
            mode='lines', name='Cumulative Net Profit (€)',
            line=dict(color='#2ca02c', width=2.5, dash='dashdot')
        ),
        row=3, col=1, secondary_y=True
    )

    # Layout styling
    fig.update_layout(
        height=820,
        hovermode="x unified",
        margin=dict(l=40, r=40, t=50, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1.0),
    )
    fig.update_yaxes(title_text="Price (€/MWh)", row=1, col=1)
    fig.update_yaxes(title_text="Power (MW)", row=2, col=1, range=[-bess_config.power_capacity_mw * 1.25, bess_config.power_capacity_mw * 1.25])
    fig.update_yaxes(title_text="SoC (%)", range=[0, 100], row=3, col=1, secondary_y=False)
    fig.update_yaxes(title_text="Net Profit (€)", row=3, col=1, secondary_y=True)
    fig.update_xaxes(title_text="Timeline (Hours)", row=3, col=1)

    st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.markdown("### ⚖️ Strategy Performance Comparison")
    st.markdown(
        "Demonstrates how the realistic forecast-driven strategy performs against an ideal theoretical benchmark with 100% price certainty."
    )

    comp_df = pd.DataFrame({
        "Performance Metric": [
            "Net Arbitrage Profit (€)",
            "Gross Arbitrage Profit (excl. wear) (€)",
            "Gross Revenue from Discharging (€)",
            "Total Electricity Charging Cost (€)",
            "Cell Degradation Cost (€)",
            "Realized Price Spread (€/MWh)",
            "Average Discharge Price (€/MWh)",
            "Average Charge Price (€/MWh)",
            "Equivalent Full Cycles (FCE)",
            "Forecast Value Capture Ratio (%)"
        ],
        f"Forecast-Driven ({selected_model_name})": [
            f"{kpis_forecast['net_profit_eur']:,.2f} €",
            f"{kpis_forecast['gross_profit_eur']:,.2f} €",
            f"{kpis_forecast['revenue_eur']:,.2f} €",
            f"{kpis_forecast['charging_cost_eur']:,.2f} €",
            f"{kpis_forecast['degradation_cost_eur']:,.2f} €",
            f"{kpis_forecast['realized_spread_eur_mwh']:.2f} €/MWh",
            f"{kpis_forecast['avg_discharge_price_eur_mwh']:.2f} €/MWh",
            f"{kpis_forecast['avg_charge_price_eur_mwh']:.2f} €/MWh",
            f"{kpis_forecast['fce_cycles']:.2f}",
            f"{capture_ratio:.1f} %"
        ],
        "Perfect Foresight (Benchmark)": [
            f"{kpis_perfect['net_profit_eur']:,.2f} €",
            f"{kpis_perfect['gross_profit_eur']:,.2f} €",
            f"{kpis_perfect['revenue_eur']:,.2f} €",
            f"{kpis_perfect['charging_cost_eur']:,.2f} €",
            f"{kpis_perfect['degradation_cost_eur']:,.2f} €",
            f"{kpis_perfect['realized_spread_eur_mwh']:.2f} €/MWh",
            f"{kpis_perfect['avg_discharge_price_eur_mwh']:.2f} €/MWh",
            f"{kpis_perfect['avg_charge_price_eur_mwh']:.2f} €/MWh",
            f"{kpis_perfect['fce_cycles']:.2f}",
            "100.0 %"
        ]
    })
    st.table(comp_df)

    col_b1, col_b2 = st.columns(2)
    with col_b1:
        st.info(
            f"**💡 ML Accuracy Impact:** {selected_model_name} achieved an R² score of "
            f"**{active_model['r2']:.4f}** and MAE of **{active_model['mae']:.2f} €/MWh**, "
            f"enabling the battery to capture **{capture_ratio:.1f}%** of maximum theoretical arbitrage profits."
        )
    with col_b2:
        st.success(
            f"**⚡ Merit-Order Arbitrage:** The BESS achieved an average realized spread of "
            f"**{kpis_forecast['realized_spread_eur_mwh']:.2f} €/MWh** by consistently buying during "
            f"low-residual-load periods and selling during evening peak hours."
        )

with tab3:
    st.markdown("### 📋 Hourly BESS Dispatch Schedule")
    schedule_df = pd.DataFrame({
        "Hour": hours_idx,
        "Actual_Price_EUR_MWh": np.round(actual_prices, 2),
        "Forecast_Price_EUR_MWh": np.round(forecast_prices, 2),
        "Charge_Power_MW": np.round(disp_forecast["p_charge_mw"], 3),
        "Discharge_Power_MW": np.round(disp_forecast["p_discharge_mw"], 3),
        "Net_Power_MW": np.round(disp_forecast["net_power_mw"], 3),
        "Battery_SoC_Pct": np.round(disp_forecast["soc"] * 100.0, 1),
        "Cumulative_Profit_EUR": np.round(kpis_forecast["cumulative_profit_eur"], 2),
    })

    st.dataframe(schedule_df, use_container_width=True, height=400)

    csv_data = schedule_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download Dispatch Schedule as CSV",
        data=csv_data,
        file_name="bess_optimal_dispatch_schedule.csv",
        mime="text/csv",
    )
