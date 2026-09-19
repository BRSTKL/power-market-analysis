"""
bess_optimizer.py - Battery Energy Storage System (BESS) Arbitrage & Dispatch Optimizer

This module models a utility-scale Battery Energy Storage System (BESS) and solves
the price arbitrage dispatch problem using Linear Programming (SciPy linprog).
It compares realistic forecast-driven dispatch (using Random Forest predictions)
against a perfect-foresight benchmark.
"""

from dataclasses import dataclass
from typing import Dict, Any, Tuple, Optional
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.optimize import linprog

from market_data import fetch_smard_fundamentals
from rf_model import train_price_forecasting_model


# ==============================================================================
# 1. BESS Technical & Financial Configuration
# ==============================================================================

@dataclass
class BESSConfig:
    """
    Technical and economic specification of the Battery Energy Storage System.

    Attributes
    ----------
    energy_capacity_mwh : float
        Total usable battery storage capacity in Megawatt-hours (MWh). Default 2.0.
    power_capacity_mw : float
        Maximum charge / discharge inverter power in Megawatts (MW). Default 1.0 (2-hour system).
    charge_efficiency : float
        Efficiency of charging (one-way). Default 0.93.
    discharge_efficiency : float
        Efficiency of discharging (one-way). Default 0.93.
        Round-Trip Efficiency (RTE) = 0.93 * 0.93 ≈ 86.5%.
    soc_min : float
        Minimum allowable State of Charge (SoC) fraction [0, 1]. Default 0.10.
    soc_max : float
        Maximum allowable State of Charge (SoC) fraction [0, 1]. Default 0.90.
    initial_soc : float
        State of Charge fraction at the beginning of optimization. Default 0.50.
    target_final_soc : float
        Required State of Charge fraction at the end of optimization. Default 0.50.
    degradation_cost_per_mwh : float
        Marginal degradation and battery wear cost per MWh throughput (EUR/MWh). Default 5.0.
    """
    energy_capacity_mwh: float = 2.0
    power_capacity_mw: float = 1.0
    charge_efficiency: float = 0.93
    discharge_efficiency: float = 0.93
    soc_min: float = 0.10
    soc_max: float = 0.90
    initial_soc: float = 0.50
    target_final_soc: float = 0.50
    degradation_cost_per_mwh: float = 5.0

    @property
    def round_trip_efficiency(self) -> float:
        return self.charge_efficiency * self.discharge_efficiency


# ==============================================================================
# 2. Linear Programming Dispatch Optimizer
# ==============================================================================

def optimize_bess_dispatch(
    price_signals: np.ndarray,
    config: BESSConfig,
    dt_hours: float = 1.0,
) -> Dict[str, Any]:
    """
    Formulate and solve the BESS arbitrage dispatch problem via Linear Programming.

    Decision Variables for each hour t in {0, ..., T-1}:
      - P_ch[t]  : Charging power in MW, bounded in [0, P_max]
      - P_dis[t] : Discharging power in MW, bounded in [0, P_max]
      - E[t]     : Stored energy in MWh at end of hour t, bounded in [E_min, E_max]

    Total variables = 3 * T.

    Objective:
      Min sum_t [ price[t] * P_ch[t] * dt
                 - price[t] * P_dis[t] * dt
                 + c_deg * (P_ch[t] + P_dis[t]) * dt ]
      (Equivalent to maximizing arbitrage revenue minus charging and wear costs).

    Parameters
    ----------
    price_signals : np.ndarray
        Array of hourly electricity prices (EUR/MWh) used to compute the dispatch.
    config : BESSConfig
        BESS technical parameters.
    dt_hours : float
        Time resolution in hours (default 1.0 for hourly data).

    Returns
    -------
    dict
        Contains optimal dispatch schedules (P_charge, P_discharge, net_power, energy, soc).
    """
    T = len(price_signals)
    prices = np.asarray(price_signals, dtype=float)

    # Number of variables = 3 * T:
    # indices: [0..T-1] for P_ch, [T..2T-1] for P_dis, [2T..3T-1] for E
    num_vars = 3 * T

    # --------------------------------------------------------------------------
    # Objective Function Vector (c)
    # --------------------------------------------------------------------------
    c = np.zeros(num_vars)
    # Charging: pay price + degradation cost
    c[0:T] = (prices + config.degradation_cost_per_mwh) * dt_hours
    # Discharging: receive price minus degradation cost (minimize negative revenue)
    c[T:2*T] = (-prices + config.degradation_cost_per_mwh) * dt_hours
    # Stored energy: no direct cost
    c[2*T:3*T] = 0.0

    # --------------------------------------------------------------------------
    # Variable Bounds
    # --------------------------------------------------------------------------
    bounds = []
    # P_charge: [0, P_max]
    for _ in range(T):
        bounds.append((0.0, config.power_capacity_mw))
    # P_discharge: [0, P_max]
    for _ in range(T):
        bounds.append((0.0, config.power_capacity_mw))
    # Energy: [E_min, E_max]
    e_min = config.soc_min * config.energy_capacity_mwh
    e_max = config.soc_max * config.energy_capacity_mwh
    for _ in range(T):
        bounds.append((e_min, e_max))

    # --------------------------------------------------------------------------
    # Equality Constraints (A_eq * x = b_eq)
    # Energy continuity equations:
    # t = 0: E[0] - eta_ch * dt * P_ch[0] + (1 / eta_dis) * dt * P_dis[0] = E_init
    # t > 0: E[t] - E[t-1] - eta_ch * dt * P_ch[t] + (1 / eta_dis) * dt * P_dis[t] = 0
    # End constraint: E[T-1] = E_target
    # --------------------------------------------------------------------------
    num_eq = T + 1
    A_eq = np.zeros((num_eq, num_vars))
    b_eq = np.zeros(num_eq)

    eta_ch = config.charge_efficiency
    inv_eta_dis = 1.0 / config.discharge_efficiency
    e_init = config.initial_soc * config.energy_capacity_mwh
    e_target = config.target_final_soc * config.energy_capacity_mwh

    # t = 0 continuity
    A_eq[0, 2*T] = 1.0                     # E[0]
    A_eq[0, 0] = -eta_ch * dt_hours        # -eta_ch * dt * P_ch[0]
    A_eq[0, T] = inv_eta_dis * dt_hours    # +dt / eta_dis * P_dis[0]
    b_eq[0] = e_init

    # t = 1..T-1 continuity
    for t in range(1, T):
        A_eq[t, 2*T + t] = 1.0             # E[t]
        A_eq[t, 2*T + t - 1] = -1.0         # -E[t-1]
        A_eq[t, t] = -eta_ch * dt_hours    # -eta_ch * dt * P_ch[t]
        A_eq[t, T + t] = inv_eta_dis * dt_hours  # +dt / eta_dis * P_dis[t]
        b_eq[t] = 0.0

    # Final cycle SoC target equality
    A_eq[T, 2*T + T - 1] = 1.0            # E[T-1]
    b_eq[T] = e_target

    # --------------------------------------------------------------------------
    # Solve Linear Program with HiGHS Interior Point / Simplex solver
    # --------------------------------------------------------------------------
    res = linprog(
        c,
        A_eq=A_eq,
        b_eq=b_eq,
        bounds=bounds,
        method="highs",
    )

    if not res.success:
        raise RuntimeError(f"BESS optimization failed: {res.message}")

    sol = res.x
    p_charge = sol[0:T]
    p_discharge = sol[T:2*T]
    energy = sol[2*T:3*T]
    soc = energy / config.energy_capacity_mwh
    net_power = p_discharge - p_charge  # positive = discharging, negative = charging

    return {
        "success": True,
        "p_charge_mw": p_charge,
        "p_discharge_mw": p_discharge,
        "net_power_mw": net_power,
        "energy_mwh": energy,
        "soc": soc,
    }


# ==============================================================================
# 3. Backtest & Financial Performance Analytics
# ==============================================================================

def backtest_bess_strategy(
    actual_prices: np.ndarray,
    dispatch_plan: Dict[str, Any],
    config: BESSConfig,
) -> Dict[str, float]:
    """
    Evaluate the financial performance of a dispatch plan settled against ACTUAL market prices.

    Parameters
    ----------
    actual_prices : np.ndarray
        True observed electricity prices (EUR/MWh).
    dispatch_plan : dict
        Dispatch schedules from optimize_bess_dispatch.
    config : BESSConfig
        BESS configuration.

    Returns
    -------
    dict
        Comprehensive financial and operational Key Performance Indicators (KPIs).
    """
    p_ch = dispatch_plan["p_charge_mw"]
    p_dis = dispatch_plan["p_discharge_mw"]

    # Hourly cash flows
    revenue_hourly = p_dis * actual_prices
    cost_hourly = p_ch * actual_prices
    degradation_hourly = (p_ch + p_dis) * config.degradation_cost_per_mwh
    net_cashflow_hourly = revenue_hourly - cost_hourly - degradation_hourly

    # Cumulative sums
    total_revenue = float(np.sum(revenue_hourly))
    total_charging_cost = float(np.sum(cost_hourly))
    total_degradation_cost = float(np.sum(degradation_hourly))
    net_profit = float(np.sum(net_cashflow_hourly))

    # Energy throughput & cycling
    total_energy_charged_mwh = float(np.sum(p_ch))
    total_energy_discharged_mwh = float(np.sum(p_dis))
    # Full Cycle Equivalent (FCE) = Throughput / (2 * Usable Capacity)
    usable_capacity = (config.soc_max - config.soc_min) * config.energy_capacity_mwh
    fce = (total_energy_charged_mwh + total_energy_discharged_mwh) / (2.0 * usable_capacity)

    # Average realized prices
    avg_charge_price = (
        total_charging_cost / total_energy_charged_mwh
        if total_energy_charged_mwh > 0 else 0.0
    )
    avg_discharge_price = (
        total_revenue / total_energy_discharged_mwh
        if total_energy_discharged_mwh > 0 else 0.0
    )
    realized_spread = avg_discharge_price - avg_charge_price

    # Negative price capture (hours charging when price < 0, getting paid to charge!)
    neg_price_charge_mwh = float(np.sum(p_ch[actual_prices < 0]))
    neg_price_earnings = float(np.sum(cost_hourly[actual_prices < 0] * -1.0))

    return {
        "net_profit_eur": net_profit,
        "gross_profit_eur": total_revenue - total_charging_cost,
        "revenue_eur": total_revenue,
        "charging_cost_eur": total_charging_cost,
        "degradation_cost_eur": total_degradation_cost,
        "energy_charged_mwh": total_energy_charged_mwh,
        "energy_discharged_mwh": total_energy_discharged_mwh,
        "fce_cycles": fce,
        "avg_charge_price_eur_mwh": avg_charge_price,
        "avg_discharge_price_eur_mwh": avg_discharge_price,
        "realized_spread_eur_mwh": realized_spread,
        "neg_price_charge_mwh": neg_price_charge_mwh,
        "neg_price_earnings_eur": neg_price_earnings,
        "cumulative_profit_eur": np.cumsum(net_cashflow_hourly),
    }


# ==============================================================================
# 4. Multi-Strategy Benchmark Comparison
# ==============================================================================

def run_bess_comparison(
    actual_prices: np.ndarray,
    forecast_prices: np.ndarray,
    config: BESSConfig,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """
    Run and compare both:
      1. Perfect Foresight Dispatch (Theoretical upper bound benchmark)
      2. Forecast-Driven Dispatch (Realistic dispatch using RF model predictions)

    Parameters
    ----------
    actual_prices : np.ndarray
        True observed electricity prices.
    forecast_prices : np.ndarray
        Random Forest model predicted electricity prices.
    config : BESSConfig
        BESS configuration.

    Returns
    -------
    tuple of (perfect_dispatch, forecast_dispatch, summary_kpis)
    """
    print("\n" + "=" * 60)
    print("  BESS Dispatch Optimization: Strategy Backtest")
    print("=" * 60)

    # 1. Perfect Foresight (Optimized on actual prices)
    print("[1/2] Solving Perfect Foresight Optimization (Benchmark)...")
    perfect_dispatch = optimize_bess_dispatch(actual_prices, config)
    kpis_perfect = backtest_bess_strategy(actual_prices, perfect_dispatch, config)

    # 2. Forecast-Driven Dispatch (Optimized on RF forecasts, settled on actual prices)
    print("[2/2] Solving Forecast-Driven Optimization (Realistic Day-Ahead)...")
    forecast_dispatch = optimize_bess_dispatch(forecast_prices, config)
    kpis_forecast = backtest_bess_strategy(actual_prices, forecast_dispatch, config)

    # Value Capture Ratio
    capture_ratio = (
        (kpis_forecast["net_profit_eur"] / kpis_perfect["net_profit_eur"]) * 100.0
        if kpis_perfect["net_profit_eur"] > 0 else 0.0
    )

    print("\n" + "=" * 65)
    print(f"{'Performance Metric':<32} | {'Forecast-Driven':>13} | {'Perfect Foresight':>15}")
    print("-" * 65)
    print(f"{'Net Arbitrage Profit (EUR)':<32} | {kpis_forecast['net_profit_eur']:>12.2f} € | {kpis_perfect['net_profit_eur']:>14.2f} €")
    print(f"{'Gross Profit (excl. deg.) (EUR)':<32} | {kpis_forecast['gross_profit_eur']:>12.2f} € | {kpis_perfect['gross_profit_eur']:>14.2f} €")
    print(f"{'Total Revenue (EUR)':<32} | {kpis_forecast['revenue_eur']:>12.2f} € | {kpis_perfect['revenue_eur']:>14.2f} €")
    print(f"{'Total Charging Cost (EUR)':<32} | {kpis_forecast['charging_cost_eur']:>12.2f} € | {kpis_perfect['charging_cost_eur']:>14.2f} €")
    print(f"{'Degradation Cost (EUR)':<32} | {kpis_forecast['degradation_cost_eur']:>12.2f} € | {kpis_perfect['degradation_cost_eur']:>14.2f} €")
    print(f"{'Realized Spread (EUR/MWh)':<32} | {kpis_forecast['realized_spread_eur_mwh']:>12.2f} € | {kpis_perfect['realized_spread_eur_mwh']:>14.2f} €")
    print(f"{'Avg Discharge Price (EUR/MWh)':<32} | {kpis_forecast['avg_discharge_price_eur_mwh']:>12.2f} € | {kpis_perfect['avg_discharge_price_eur_mwh']:>14.2f} €")
    print(f"{'Avg Charge Price (EUR/MWh)':<32} | {kpis_forecast['avg_charge_price_eur_mwh']:>12.2f} € | {kpis_perfect['avg_charge_price_eur_mwh']:>14.2f} €")
    print(f"{'Equivalent Full Cycles (FCE)':<32} | {kpis_forecast['fce_cycles']:>12.2f}   | {kpis_perfect['fce_cycles']:>14.2f}  ")
    print("-" * 65)
    print(f"{'Forecast Value Capture Ratio':<32} | {capture_ratio:>12.1f} % | {'100.0 %':>15}")
    print("=" * 65)

    summary = {
        "kpis_perfect": kpis_perfect,
        "kpis_forecast": kpis_forecast,
        "value_capture_ratio_pct": capture_ratio,
    }

    return perfect_dispatch, forecast_dispatch, summary


# ==============================================================================
# 5. Visual Dashboard Generation
# ==============================================================================

def plot_bess_dashboard(
    actual_prices: np.ndarray,
    forecast_prices: np.ndarray,
    forecast_dispatch: Dict[str, Any],
    kpis_forecast: Dict[str, Any],
    config: BESSConfig,
    save_path: str = "bess_dispatch_results.png",
) -> None:
    """
    Generate an executive 3-panel publication-quality dispatch dashboard.

    Panel 1: Day-Ahead Electricity Prices (Actual vs Predicted)
    Panel 2: Optimal BESS Dispatch Profile (Charging vs Discharging)
    Panel 3: State of Charge (SoC %) and Cumulative Arbitrage Profit (€)
    """
    print(f"\nGenerating BESS dispatch dashboard plot -> {save_path}...")
    hours = np.arange(len(actual_prices))

    fig, axes = plt.subplots(3, 1, figsize=(14, 11), sharex=True, constrained_layout=True)
    fig.suptitle(
        f"BESS Revenue Optimization & Arbitrage Dispatch Dashboard\n"
        f"(1 MW / 2 MWh Battery | Value Capture: {kpis_forecast['net_profit_eur']:.2f} € | "
        f"Realized Spread: {kpis_forecast['realized_spread_eur_mwh']:.2f} €/MWh)",
        fontsize=14,
        fontweight="bold",
    )

    # --------------------------------------------------------------------------
    # Panel 1: Price Signals
    # --------------------------------------------------------------------------
    ax1 = axes[0]
    ax1.plot(hours, actual_prices, label="Actual Price (EUR/MWh)", color="#1f77b4", linewidth=1.6)
    ax1.plot(hours, forecast_prices, label="Predicted Price (EUR/MWh)", color="#d62728", linestyle="--", linewidth=1.4)
    ax1.axhline(0, color="gray", linestyle=":", linewidth=0.8)
    # Highlight negative price hours if any
    neg_mask = actual_prices < 0
    if np.any(neg_mask):
        ax1.fill_between(hours, actual_prices, 0, where=neg_mask, color="purple", alpha=0.25, label="Negative Price Zone")
    ax1.set_ylabel("Price (€/MWh)", fontsize=11)
    ax1.set_title("Day-Ahead Electricity Price Signals", fontsize=11, fontweight="bold")
    ax1.legend(loc="upper right", framealpha=0.9)
    ax1.grid(True, linestyle=":", alpha=0.6)

    # --------------------------------------------------------------------------
    # Panel 2: Dispatch Power Profile (MW)
    # --------------------------------------------------------------------------
    ax2 = axes[1]
    p_dis = forecast_dispatch["p_discharge_mw"]
    p_ch = forecast_dispatch["p_charge_mw"]
    net_p = forecast_dispatch["net_power_mw"]

    ax2.bar(hours, p_dis, width=0.8, color="#2ca02c", alpha=0.85, label="Discharge to Grid (+MW)")
    ax2.bar(hours, -p_ch, width=0.8, color="#ff7f0e", alpha=0.85, label="Charge from Grid (-MW)")
    ax2.axhline(0, color="black", linewidth=0.8)
    ax2.set_ylabel("Power (MW)", fontsize=11)
    ax2.set_ylim(-config.power_capacity_mw * 1.25, config.power_capacity_mw * 1.25)
    ax2.set_title("BESS Optimal Dispatch Profile (Forecast-Driven Strategy)", fontsize=11, fontweight="bold")
    ax2.legend(loc="upper right", framealpha=0.9)
    ax2.grid(True, linestyle=":", alpha=0.6)

    # --------------------------------------------------------------------------
    # Panel 3: State of Charge (SoC %) and Cumulative Profit
    # --------------------------------------------------------------------------
    ax3 = axes[2]
    soc_pct = forecast_dispatch["soc"] * 100.0
    color_soc = "#17becf"
    ax3.plot(hours, soc_pct, color=color_soc, linewidth=2.0, label="Battery SoC (%)")
    ax3.axhline(config.soc_max * 100, color="red", linestyle="--", alpha=0.6, label="SoC Limits (10% - 90%)")
    ax3.axhline(config.soc_min * 100, color="red", linestyle="--", alpha=0.6)
    ax3.set_ylabel("State of Charge (%)", color=color_soc, fontsize=11)
    ax3.tick_params(axis="y", labelcolor=color_soc)
    ax3.set_ylim(0, 100)
    ax3.set_xlabel("Timeline (Hours)", fontsize=11)
    ax3.grid(True, linestyle=":", alpha=0.6)

    # Twin axis for cumulative profit
    ax3_twin = ax3.twinx()
    color_profit = "#2ca02c"
    cum_profit = kpis_forecast["cumulative_profit_eur"]
    ax3_twin.plot(hours, cum_profit, color=color_profit, linewidth=2.2, linestyle="-.", label="Cumulative Profit (€)")
    ax3_twin.set_ylabel("Net Profit (€)", color=color_profit, fontsize=11)
    ax3_twin.tick_params(axis="y", labelcolor=color_profit)

    # Combined legend for Panel 3
    lines_1, labels_1 = ax3.get_legend_handles_labels()
    lines_2, labels_2 = ax3_twin.get_legend_handles_labels()
    ax3.legend(lines_1 + lines_2, labels_1 + labels_2, loc="upper left", framealpha=0.9)
    ax3.set_title("State of Charge (SoC) Dynamics & Cumulative Net Profit Evolution", fontsize=11, fontweight="bold")

    plt.savefig(save_path, dpi=300, bbox_inches="tight")
    print(f"[OK] Dashboard plot saved successfully as '{save_path}'")
    plt.close()


# ==============================================================================
# 6. Main Execution Pipeline
# ==============================================================================

if __name__ == "__main__":
    print("=" * 65)
    print("  Battery Energy Storage System (BESS) Arbitrage Optimizer")
    print("  Market: Germany (SMARD API) | Model: LP + Random Forest")
    print("=" * 65)

    # Step 1: Fetch multi-week fundamental market dataset
    print("\n[Step 1/4] Fetching market fundamentals and electricity prices...")
    dataset = fetch_smard_fundamentals(weeks=4)
    if dataset is None or dataset.empty:
        raise RuntimeError("Could not retrieve market dataset from SMARD API.")

    # Step 2: Train Random Forest forecasting model to obtain Day-Ahead predictions
    print("\n[Step 2/4] Training Random Forest to predict Day-Ahead prices...")
    model, y_test, predictions = train_price_forecasting_model(dataset)

    actual_prices = y_test.values
    forecast_prices = predictions

    # Step 3: Configure Battery Energy Storage System (1 MW / 2 MWh)
    print("\n[Step 3/4] Initializing BESS Specification...")
    bess_cfg = BESSConfig(
        energy_capacity_mwh=2.0,
        power_capacity_mw=1.0,
        charge_efficiency=0.93,
        discharge_efficiency=0.93,
        soc_min=0.10,
        soc_max=0.90,
        initial_soc=0.50,
        target_final_soc=0.50,
        degradation_cost_per_mwh=5.0,
    )
    print(f"  Configuration: {bess_cfg.power_capacity_mw} MW / {bess_cfg.energy_capacity_mwh} MWh")
    print(f"  Round-Trip Efficiency: {bess_cfg.round_trip_efficiency * 100:.1f}%")
    print(f"  Usable SoC Range: {bess_cfg.soc_min * 100:.0f}% - {bess_cfg.soc_max * 100:.0f}%")
    print(f"  Degradation Cost: {bess_cfg.degradation_cost_per_mwh:.2f} €/MWh throughput")

    # Step 4: Run Optimization, Backtest & Dashboard Generation
    print("\n[Step 4/4] Executing LP Dispatch Optimization and Strategy Backtest...")
    perfect_disp, forecast_disp, summary = run_bess_comparison(
        actual_prices=actual_prices,
        forecast_prices=forecast_prices,
        config=bess_cfg,
    )

    # Plot visual dashboard
    plot_bess_dashboard(
        actual_prices=actual_prices,
        forecast_prices=forecast_prices,
        forecast_dispatch=forecast_disp,
        kpis_forecast=summary["kpis_forecast"],
        config=bess_cfg,
        save_path="bess_dispatch_results.png",
    )

    print("\n" + "=" * 65)
    print("  BESS Arbitrage Optimization Pipeline Completed Successfully!")
    print("=" * 65)
