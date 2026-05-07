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
│   ├── DE_power_analytics.ipynb         # Full DE analysis: EDA, backtest, heatmap, RF model
│   └── European_Power_Analysis_EN.ipynb # Multi-country ENTSO-E price comparison
├── scripts/
│   ├── fetch_smard.py                   # SMARD API data fetcher (DE prices)
│   ├── fetch_prices_all_countries.py    # ENTSO-E multi-country prices
│   ├── fetch_entso_e.py                 # Generation mix fetcher
│   └── merge_data.py                    # Dataset merger
├── data/
│   └── output/                          # Auto-updated CSV datasets
├── .github/
│   └── workflows/
│       └── daily_fetch.yml              # GitHub Actions (runs daily 07:30 UTC)
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
