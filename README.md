# Germany Power Market Analysis

Data-driven analysis of the German electricity market
using real market data from the SMARD API.

## Project Scope

- **Module A:** 1-year hourly data collection via SMARD API,
  negative price analysis, correlation study
- **Module B:** Peak/off-peak arbitrage strategy backtest
- **Module C:** Month × hour heatmap visualization
- **Module D:** Random Forest price forecasting model (MAE: 16.66 €/MWh, R²: 0.78)

## Key Findings

- **6.6%** of annual hours had negative electricity prices
- Highest negative price frequency: **April-June, 10:00-13:00**
- Net surplus (wind + solar - consumption) showed **-0.86 correlation** with price
- Feature importance analysis: `net_fazla_mwh` ranked first with **80% weight**

## Tech Stack

Python, pandas, scikit-learn, matplotlib, SMARD API

## Data Source

[SMARD - Bundesnetzagentur](https://www.smard.de)
