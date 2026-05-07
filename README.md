# 🔌 Germany Day-Ahead Electricity Price Forecasting

> Machine learning pipeline for forecasting German hourly electricity prices using SMARD market data and ENTSO-E generation mix.

---

## 📊 Results

| Metric | Value |
|--------|-------|
| **MAE** | 16.66 €/MWh |
| **R²** | 0.78 |
| **Model** | Random Forest Regressor |
| **Data range** | 2020–2024 (hourly) |
| **Features** | 12 (price lags, generation mix, weather proxies) |

---

## 🗂️ Project Structure

```
power-market-analysis/
├── notebooks/
│   ├── 01_data_exploration.ipynb       # SMARD API data pipeline & EDA
│   ├── 02_correlation_analysis.ipynb   # Price vs. generation mix correlations
│   ├── 03_backtesting.ipynb            # Spread strategy backtesting
│   ├── 04_heatmap_analysis.ipynb       # Month×hour price heatmap
│   └── 05_random_forest_model.ipynb    # ML model training & evaluation
├── scripts/
│   └── fetch_smard_data.py             # Automated SMARD API data fetcher
├── data/
│   └── output/                         # Processed CSV datasets
├── .github/
│   └── workflows/                      # GitHub Actions (daily auto-fetch)
└── requirements.txt
```

---

## ⚡ Key Findings

- **Renewable share** is the strongest price driver — **48% feature importance** in the Random Forest model
- **Negative price hours** occur mainly during midday on high-solar weekends (cannibalization effect)
- **Month×hour heatmap** reveals clear seasonal patterns: winter mornings and summer afternoons peak
- **DE–FR price correlation** is high (>0.90), while DE–IT shows more divergence due to grid constraints
- **Peak/off-peak spread** averages ~15 €/MWh, providing consistent arbitrage signal

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| `Python 3.11` | Core language |
| `pandas / NumPy` | Data manipulation & time series |
| `scikit-learn` | Random Forest model, cross-validation |
| `matplotlib / seaborn` | Visualization |
| `SMARD API` | German day-ahead price data (free, official) |
| `ENTSO-E API` | European generation mix data |
| `GitHub Actions` | Daily automated data refresh (07:30 UTC) |

---

## 📈 Model Architecture

```
SMARD API (hourly prices)
ENTSO-E API (generation mix)        →  Feature Engineering  →  Random Forest  →  Price Forecast
Open-Meteo (weather proxies)
```

**Features used:**
- Price lags (t-1, t-24, t-168 — hourly, daily, weekly)
- Hour of day, day of week, month (cyclical encoding)
- Renewable share (wind + solar as % of total generation)
- Load forecast
- Gas & coal generation share

---

## 🚀 Quick Start

```bash
git clone https://github.com/BRSTKL/power-market-analysis.git
cd power-market-analysis
pip install -r requirements.txt
```

Run the data pipeline:
```bash
python scripts/fetch_smard_data.py
```

Open notebooks in order (`01_` → `05_`) to reproduce the full analysis.

---

## 📌 Market Context

This project analyzes the **EPEX SPOT Germany/Luxembourg day-ahead market** — one of Europe's most liquid electricity markets. Key dynamics modeled:

- **Merit order effect**: renewables push conventional plants out of the stack → price suppression
- **Cannibalization effect**: high solar output → midday price collapse
- **Seasonal demand**: winter heating load vs. summer air conditioning in neighboring markets
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

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue)](https://www.linkedin.com/in/barısegementokul)
[![GitHub](https://img.shields.io/badge/GitHub-BRSTKL-black)](https://github.com/BRSTKL)

---

*Data is automatically refreshed daily via GitHub Actions. Last model training: 2024.*
