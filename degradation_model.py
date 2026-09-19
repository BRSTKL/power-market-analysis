"""
degradation_model.py - Non-Linear Battery Degradation & Rainflow Cycle Counting

Implements an industry-standard ASTM E1049-85 Rainflow Cycle Counting algorithm
and a semi-empirical non-linear battery degradation model (Wöhler / Coffin-Manson
power-law + SoC mean stress) to move beyond fixed linear €/MWh assumptions to
operational utility-scale battery storage economics.
"""

from typing import List, Dict, Tuple, Any
import numpy as np
import pandas as pd


# ==============================================================================
# 1. ASTM E1049-85 Rainflow Cycle Counting Algorithm
# ==============================================================================

def rainflow_counting(soc_series: np.ndarray) -> List[Dict[str, float]]:
    """
    Perform four-point Rainflow Cycle Counting on an arbitrary State of Charge (SoC) time-series.
    Complies with standard ASTM E1049-85 (Cycle Counting in Fatigue Analysis).

    Parameters
    ----------
    soc_series : np.ndarray
        Array of battery State of Charge values in range [0, 1].

    Returns
    -------
    list of dict
        Each dict contains:
          - 'range_dod' : Depth of Discharge (DoD) = peak-to-peak amplitude [0, 1]
          - 'count'     : 1.0 for a closed full-cycle, 0.5 for an unclosed half-cycle
          - 'mean_soc'  : Average SoC during the cycle
    """
    soc = np.asarray(soc_series, dtype=float)
    if len(soc) < 2:
        return []

    # Step 1: Filter to turning points (local extrema / reversal points)
    diffs = np.diff(soc)
    # Exclude consecutive identical values
    nonzero_mask = diffs != 0.0
    if not np.any(nonzero_mask):
        return []

    pts = [soc[0]]
    for i in range(len(diffs) - 1):
        if (diffs[i] * diffs[i + 1]) < 0.0:
            pts.append(soc[i + 1])
    pts.append(soc[-1])

    # Step 2: Four-point rainflow stack algorithm
    cycles = []
    stack = []

    for pt in pts:
        stack.append(pt)
        while len(stack) >= 3:
            s0 = stack[-3]
            s1 = stack[-2]
            s2 = stack[-1]
            range_inner = abs(s1 - s0)
            range_outer = abs(s2 - s1)

            if range_outer >= range_inner:
                mean_val = (s0 + s1) / 2.0
                if len(stack) == 3:
                    # Count as half cycle
                    cycles.append({
                        "range_dod": float(range_inner),
                        "count": 0.5,
                        "mean_soc": float(mean_val)
                    })
                    stack.pop(-2)
                else:
                    # Count as closed full cycle
                    cycles.append({
                        "range_dod": float(range_inner),
                        "count": 1.0,
                        "mean_soc": float(mean_val)
                    })
                    stack.pop(-2)
                    stack.pop(-2)
            else:
                break

    # Step 3: Count remaining unclosed reversals in stack as half-cycles
    for i in range(len(stack) - 1):
        rng = abs(stack[i + 1] - stack[i])
        mean_v = (stack[i] + stack[i + 1]) / 2.0
        if rng > 1e-4:
            cycles.append({
                "range_dod": float(rng),
                "count": 0.5,
                "mean_soc": float(mean_v)
            })

    return cycles


# ==============================================================================
# 2. Non-Linear Battery Cell Degradation Physics Model
# ==============================================================================

class BatteryDegradationModel:
    """
    Semi-empirical battery degradation model combining:
      1. Wöhler / Coffin-Manson Depth of Discharge (DoD) power-law fatigue curve.
      2. SoC Mean Stress Factor (SEI growth accelerated at elevated SoC > 70%).
      3. Replacement capital cost conversion to compute true operational euro wear.
    """

    def __init__(
        self,
        ref_cycles_at_80_dod: float = 6000.0,
        dod_exponent: float = 1.8,
        soc_stress_coeff: float = 0.85,
        battery_capex_eur_per_kwh: float = 140.0,
        eol_capacity_fade: float = 0.20,
    ):
        """
        Parameters
        ----------
        ref_cycles_at_80_dod : float
            Rated cycle life at standard 80% Depth of Discharge (typically 5,000 - 8,000 for LFP).
        dod_exponent : float
            Non-linear fatigue exponent (1.6 - 2.0 for lithium-ion chemistries).
        soc_stress_coeff : float
            Coefficient scaling calendar & SEI stress as a function of mean operating SoC.
        battery_capex_eur_per_kwh : float
            Battery system replacement capital expenditure in EUR/kWh (default: 140 €/kWh).
        eol_capacity_fade : float
            End-of-Life (EOL) capacity loss threshold (typically 20% fade, i.e. 80% SOH remaining).
        """
        self.ref_cycles = ref_cycles_at_80_dod
        self.dod_exponent = dod_exponent
        self.soc_stress_coeff = soc_stress_coeff
        self.capex_eur_per_kwh = battery_capex_eur_per_kwh
        self.eol_fade = eol_capacity_fade

    def cycles_to_failure(self, dod: float) -> float:
        """
        Calculate expected cycles to EOL for a given cycle Depth of Discharge.
        Wöhler curve: N(DoD) = N_ref * (0.80 / DoD)^k
        """
        dod_clamped = max(float(dod), 0.01)
        return self.ref_cycles * ((0.80 / dod_clamped) ** self.dod_exponent)

    def soc_mean_stress_factor(self, mean_soc: float) -> float:
        """
        Penalty factor for cycling around high average SoC levels (>70%) where
        electrolyte oxidation and SEI layer growth accelerate.
        """
        # Baseline = 1.0 at 50% SoC
        stress = np.exp(self.soc_stress_coeff * (mean_soc - 0.50))
        return float(max(stress, 0.7))

    def evaluate_soc_degradation(
        self,
        soc_series: np.ndarray,
        battery_capacity_mwh: float,
        simulation_days: float,
    ) -> Dict[str, Any]:
        """
        Compute full Rainflow-based non-linear degradation metrics for an SoC dispatch profile.

        Parameters
        ----------
        soc_series : np.ndarray
            Dispatched State of Charge time series.
        battery_capacity_mwh : float
            Nominal battery capacity in MWh.
        simulation_days : float
            Time horizon of the simulation in days.

        Returns
        -------
        dict
            Degradation damage, capacity fade (% SOH loss), monetary wear cost (€),
            and projected battery operational lifetime in years.
        """
        cycles = rainflow_counting(soc_series)

        if not cycles:
            return {
                "total_cycles_count": 0,
                "cumulative_fatigue_damage": 0.0,
                "capacity_fade_pct": 0.0,
                "rainflow_degradation_cost_eur": 0.0,
                "projected_lifetime_years": 25.0,
                "cycles_df": pd.DataFrame(),
            }

        cycles_df = pd.DataFrame(cycles)

        # Compute damage for each cycle
        n_fails = cycles_df["range_dod"].apply(self.cycles_to_failure)
        stress_factors = cycles_df["mean_soc"].apply(self.soc_mean_stress_factor)

        # Cycle damage = (count / N_fail) * stress_factor
        cycle_damages = (cycles_df["count"] / n_fails) * stress_factors
        cycles_df["damage"] = cycle_damages

        total_damage = float(cycles_df["damage"].sum())

        # SOH loss = damage * EOL capacity fade (e.g. 20%)
        capacity_fade_pct = total_damage * self.eol_fade * 100.0

        # Total capital replacement cost = capacity (kWh) * capex (€/kWh)
        total_battery_capex_eur = battery_capacity_mwh * 1000.0 * self.capex_eur_per_kwh
        # Economic wear cost in EUR
        economic_wear_cost_eur = total_damage * total_battery_capex_eur

        # Projected battery lifetime in years to EOL (100% damage)
        damage_per_day = total_damage / max(simulation_days, 1e-4)
        if damage_per_day > 0:
            projected_lifetime_years = min(1.0 / (damage_per_day * 365.0), 30.0)
        else:
            projected_lifetime_years = 30.0

        return {
            "total_cycles_count": len(cycles_df),
            "effective_full_cycles": float(cycles_df[cycles_df["count"] == 1.0]["range_dod"].sum()),
            "cumulative_fatigue_damage": total_damage,
            "capacity_fade_pct": capacity_fade_pct,
            "rainflow_degradation_cost_eur": economic_wear_cost_eur,
            "projected_lifetime_years": projected_lifetime_years,
            "avg_cycle_dod": float(cycles_df["range_dod"].mean()),
            "cycles_df": cycles_df,
        }


# Quick module test
if __name__ == "__main__":
    print("Testing ASTM E1049 Rainflow Battery Degradation Model...")
    model = BatteryDegradationModel()
    # Sample 1-week dispatch profile with shallow and deep cycles
    t = np.linspace(0, 7 * 24, 7 * 24)
    # Mix of 1 deep cycle per day (DoD ~0.7) + minor micro-cycles
    sample_soc = 0.5 + 0.35 * np.sin(2 * np.pi * t / 24) + 0.05 * np.sin(2 * np.pi * t / 4)
    sample_soc = np.clip(sample_soc, 0.1, 0.9)

    res = model.evaluate_soc_degradation(sample_soc, battery_capacity_mwh=2.0, simulation_days=7.0)
    print(f"Cycles Count: {res['total_cycles_count']}")
    print(f"Cumulative Damage: {res['cumulative_fatigue_damage']:.6f}")
    print(f"Capacity Fade: {res['capacity_fade_pct']:.4f} % SOH loss in 7 days")
    print(f"Economic Degradation Cost: {res['rainflow_degradation_cost_eur']:.2f} €")
    print(f"Projected Battery Lifetime: {res['projected_lifetime_years']:.1f} years to 80% SOH")
