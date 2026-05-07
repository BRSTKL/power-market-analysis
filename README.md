# 🔌 Germany Day-Ahead Electricity Price Forecasting

> Machine learning pipeline for forecasting German hourly electricity prices using SMARD market data and ENTSO-E generation mix.

---

## 📊 Results

| Metric | Value |
|--------|-------|
| **MAE** | 16.70 €/MWh |
| **R²** | 0.78 |
| **Model** | Random Forest Regressor |
| **Data range** | 2020–2024 (hourly) |
| **Features** | 12 (price lags, net surplus, wind/solar, time) |

---

## 🗂️ Project Structure

```
power-market-analysis/
├── notebooks/
│   ├── DE_power_analytics.ipynb              # DE market EDA, backtest, heatmap, RF model
│   ├── European_Power_Analysis_EN.ipynb      # Multi-country ENTSO-E price comparison
│   ├── intraday_arbitrage_backtest.ipynb     # Peak/off-peak spread strategy backtest
│   ├── var_cvar_risk_analysis.ipynb          # Portfolio risk: VaR, CVaR, Monte Carlo
│   └── renewable_price_correlation.ipynb    # Renewable generation vs price analysis
├── scripts/
│   ├── fetch_smard.py
│   ├── fetch_prices_all_countries.py
│   ├── fetch_entso_e.py
│   └── merge_data.py
├── data/
│   └── output/
├── .github/
│   └── workflows/
│       └── daily_fetch.yml
└── requirements.txt
```

---

## ⚡ Key Findings

- **Net surplus** (generation − consumption) is the strongest price driver — **80.8% feature importance**
- **Price lags** (t-24h) are the second strongest signal — 4.7% importance
- **Negative price hours** occur mainly during midday on high-solar weekends (cannibalization effect)
- **Month×hour heatmap** reveals clear seasonal patterns: winter mornings and summer afternoons peak
- **DE–NL price correlation** is very high (0.98) — near-identical market behaviour
- **France is structurally cheaper** than Germany by ~35 €/MWh on average (nuclear baseload)
- **Peak/off-peak spread** averages ~15 €/MWh, providing consistent arbitrage signal
---

## 📓 Notebooks

### 1. DE Power Analytics (`DE_power_analytics.ipynb`)
Full German market analysis: EDA, correlation, month×hour heatmap, Random Forest model.

### 2. Intraday Arbitrage Backtest (`intraday_arbitrage_backtest.ipynb`)
Rule-based peak/off-peak spread strategy backtested across 1,985 trading days (2018–2026).

| Metric | Value |
|--------|-------|
| **Win rate** | 70.2% |
| **Avg daily P&L** | 15.68 €/MWh |
| **Profit factor** | 3.43x |
| **Max drawdown** | -4,236 €/MWh |
| **Best year** | 2022 — energy crisis (49.9 €/MWh/day) |
| **Key finding** | Strategy decay post-2025 driven by renewable spread compression |

### 3. VaR/CVaR Risk Analysis (`var_cvar_risk_analysis.ipynb`)
Portfolio risk measurement using Historical Simulation, Parametric,
and Monte Carlo methods across 1,985 trading days.

| Metric | Value |
|--------|-------|
| **VaR 95%** | -43.28 €/MWh |
| **CVaR 95%** | -64.99 €/MWh |
| **VaR breach rate** | 5.0% |
| **Monte Carlo sims** | 10,000 |
| **Key finding** | Fat-tail risk (kurtosis 11.3) underestimated by parametric approach |

### 4. European Power Analysis (`European_Power_Analysis_EN.ipynb`)
Multi-country day-ahead price analysis using ENTSO-E API across 5 European markets.

| Metric | Value |
|--------|-------|
| **Countries** | DE, FR, NL, NO, PL |
| **DE–NL correlation** | 0.98 — near-identical market behaviour |
| **FR avg spread vs DE** | -35 €/MWh — structurally cheaper (nuclear baseload) |
| **FR→DE arb frequency** | 68.5% of hours |
| **Key finding** | France persistently cheaper than Germany due to nuclear capacity |
---

### 5. Renewable Energy & Price Correlation (`renewable_price_correlation.ipynb`)
Hourly generation mix analysis using ENTSO-E API and SMARD prices (2023–2025).

| Metric | Value |
|--------|-------|
| **Dataset** | 26,300 hourly records (2023–2025) |
| **Renewable share vs price** | r = -0.766 |
| **Solar vs price (daytime)** | r = -0.623 (cannibalization effect) |
| **Wind Onshore vs price** | r = -0.357 |
| **Fossil generation vs price** | r = +0.714 |
| **Negative price frequency** | 5.1% of hours |
| **Key finding** | High solar output drives prices negative — cannibalization effect quantified |

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| `Python 3.11` | Core language |
| `pandas / NumPy` | Data manipulation & time series |
| `scikit-learn` | Random Forest model, cross-validation |
| `matplotlib` | Visualization |
| `SMARD API` | German day-ahead price data (free, official) |
| `ENTSO-E API` | European generation mix & multi-country prices |
| `GitHub Actions` | Daily automated data refresh (07:30 UTC) |

---

## 📈 Model Architecture

```
SMARD API (hourly DE prices)
ENTSO-E API (generation mix)   →  Feature Engineering  →  Random Forest  →  Price Forecast
Open-Meteo (weather proxies)
```

**Features used:**
- Net surplus MWh (generation − consumption)
- Price lags (t-24h, t-48h, t-168h — daily, 2-day, weekly)
- Wind & solar generation (MWh)
- Hour of day, day of week, month

---

## 🚀 Quick Start

```bash
git clone https://github.com/BRSTKL/power-market-analysis.git
cd power-market-analysis
pip install -r requirements.txt
```

Run the data pipeline:

```bash
python scripts/fetch_smard.py
python scripts/fetch_prices_all_countries.py
python scripts/fetch_entso_e.py
python scripts/merge_data.py
```

Then open `notebooks/DE_power_analytics.ipynb` to reproduce the full analysis.

---

## 📌 Market Context

This project analyzes the **EPEX SPOT Germany/Luxembourg day-ahead market** — one of Europe's most liquid electricity markets. Key dynamics modeled:

- **Merit order effect**: renewables push conventional plants out of the stack → price suppression
- **Cannibalization effect**: high solar output → midday price collapse
- **Net surplus effect**: when generation exceeds consumption, prices drop sharply — the dominant price signal
- **Cross-border flows**: DE price influenced by French nuclear capacity and Dutch gas generation

---

## 🔗 Related Work

- [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/)
- [SMARD Market Data](https://www.smard.de/en)
- [EPEX SPOT Market Results](https://www.epexspot.com/en/market-results)

---

## 👤 Author

**Barış Egemen Tokul**
MSc Engineering Management — Berlin
Energy Systems Engineering (BSc) — Bahçeşehir University

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue)](https://www.linkedin.com/in/baris-egemen-tokul-8016751b5)
[![GitHub](https://img.shields.io/badge/GitHub-BRSTKL-black)](https://github.com/BRSTKL)

---

*Data is automatically refreshed daily via GitHub Actions. Last model training: 2024.*
