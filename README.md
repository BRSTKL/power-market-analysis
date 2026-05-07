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

\```
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
\```

---

## ⚡ Key Findings

- **Net surplus** (generation − consumption) is the strongest price driver — **80.8% feature importance**
- **Price lags** (t-24h) are the second strongest signal — 4.7% importance
- **Negative price hours** occur mainly during midday on high-solar weekends (cannibalization effect)
- **Month×hour heatmap** reveals clear seasonal patterns: winter mornings and summer afternoons peak
- **DE–NL price correlation** is very high (0.98) — near-identical market behaviour
- **France is structurally cheaper** than Germany by ~35 €/MWh on average (nuclear baseload)
- **Peak/off-peak spread** averages ~15 €/MWh, providing consistent arbitrage signal
