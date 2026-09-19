"""
market_data.py - Energy Market Data Module

This module provides utilities for fetching, processing, and analyzing
energy market data including electricity prices, natural gas prices,
and renewable energy generation statistics.
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional


# ── Configuration ────────────────────────────────────────────────────────────

BASE_URL = "https://api.eia.gov/v2"
API_KEY = ""  # Set your EIA API key here

ENERGY_CATEGORIES = {
    "electricity": "ELEC",
    "natural_gas": "NG",
    "petroleum": "PET",
    "coal": "COAL",
    "renewable": "RENEW",
}


# ── Data Fetching ────────────────────────────────────────────────────────────

def fetch_energy_prices(
    category: str = "electricity",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    region: str = "US",
) -> pd.DataFrame:
    """
    Fetch energy price data from the EIA API.

    Parameters
    ----------
    category : str
        Energy category ('electricity', 'natural_gas', 'petroleum', 'coal', 'renewable').
    start_date : str, optional
        Start date in 'YYYY-MM-DD' format. Defaults to 1 year ago.
    end_date : str, optional
        End date in 'YYYY-MM-DD' format. Defaults to today.
    region : str
        Region code (default: 'US').

    Returns
    -------
    pd.DataFrame
        DataFrame with columns ['date', 'price', 'unit', 'region'].
    """
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    cat_code = ENERGY_CATEGORIES.get(category.lower())
    if cat_code is None:
        raise ValueError(
            f"Unknown category '{category}'. "
            f"Choose from: {list(ENERGY_CATEGORIES.keys())}"
        )

    params = {
        "api_key": API_KEY,
        "frequency": "monthly",
        "data[0]": "value",
        "start": start_date,
        "end": end_date,
        "facets[region][]": region,
    }

    url = f"{BASE_URL}/{cat_code}/data/"
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json().get("response", {}).get("data", [])
    if not data:
        return pd.DataFrame(columns=["date", "price", "unit", "region"])

    df = pd.DataFrame(data)
    df.rename(columns={"period": "date", "value": "price"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values("date", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def load_csv_data(filepath: str, date_column: str = "date") -> pd.DataFrame:
    """
    Load market data from a local CSV file.

    Parameters
    ----------
    filepath : str
        Path to the CSV file.
    date_column : str
        Name of the date column to parse.

    Returns
    -------
    pd.DataFrame
        Parsed DataFrame with date index.
    """
    df = pd.read_csv(filepath, parse_dates=[date_column])
    df.sort_values(date_column, inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# ── Data Processing ──────────────────────────────────────────────────────────

def calculate_moving_average(
    df: pd.DataFrame,
    column: str = "price",
    window: int = 7,
) -> pd.DataFrame:
    """
    Calculate simple and exponential moving averages.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing the price column.
    column : str
        Column name to compute averages on.
    window : int
        Window size for the moving average.

    Returns
    -------
    pd.DataFrame
        DataFrame with added 'SMA_{window}' and 'EMA_{window}' columns.
    """
    result = df.copy()
    result[f"SMA_{window}"] = result[column].rolling(window=window).mean()
    result[f"EMA_{window}"] = result[column].ewm(span=window, adjust=False).mean()
    return result


def calculate_volatility(
    df: pd.DataFrame,
    column: str = "price",
    window: int = 30,
) -> pd.DataFrame:
    """
    Calculate rolling volatility (standard deviation of returns).

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing the price column.
    column : str
        Column name to compute volatility on.
    window : int
        Rolling window size.

    Returns
    -------
    pd.DataFrame
        DataFrame with added 'returns' and 'volatility' columns.
    """
    result = df.copy()
    result["returns"] = result[column].pct_change()
    result["volatility"] = result["returns"].rolling(window=window).std() * np.sqrt(252)
    return result


def calculate_price_statistics(df: pd.DataFrame, column: str = "price") -> dict:
    """
    Calculate summary statistics for price data.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    column : str
        Column name to analyze.

    Returns
    -------
    dict
        Dictionary with mean, median, std, min, max, skewness, and kurtosis.
    """
    series = df[column].dropna()
    return {
        "mean": round(series.mean(), 4),
        "median": round(series.median(), 4),
        "std": round(series.std(), 4),
        "min": round(series.min(), 4),
        "max": round(series.max(), 4),
        "skewness": round(series.skew(), 4),
        "kurtosis": round(series.kurtosis(), 4),
        "count": int(series.count()),
    }


def detect_price_anomalies(
    df: pd.DataFrame,
    column: str = "price",
    z_threshold: float = 2.5,
) -> pd.DataFrame:
    """
    Detect price anomalies using Z-score method.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    column : str
        Column name to check for anomalies.
    z_threshold : float
        Z-score threshold for flagging anomalies.

    Returns
    -------
    pd.DataFrame
        DataFrame with added 'z_score' and 'is_anomaly' columns.
    """
    result = df.copy()
    mean = result[column].mean()
    std = result[column].std()

    result["z_score"] = (result[column] - mean) / std
    result["is_anomaly"] = result["z_score"].abs() > z_threshold
    return result


# ── Entry Point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Example: Generate sample data and run analysis
    print("=" * 60)
    print("  Energy Market Data - Sample Analysis")
    print("=" * 60)

    # Generate sample data for demonstration
    np.random.seed(42)
    dates = pd.date_range(start="2025-01-01", periods=365, freq="D")
    base_price = 50 + np.cumsum(np.random.randn(365) * 0.5)
    sample_df = pd.DataFrame({"date": dates, "price": base_price})

    # Calculate statistics
    stats = calculate_price_statistics(sample_df)
    print("\n📊 Price Statistics:")
    for key, value in stats.items():
        print(f"   {key:>12}: {value}")

    # Moving averages
    ma_df = calculate_moving_average(sample_df, window=30)
    print(f"\n📈 30-day SMA (latest): {ma_df['SMA_30'].iloc[-1]:.2f}")
    print(f"📈 30-day EMA (latest): {ma_df['EMA_30'].iloc[-1]:.2f}")

    # Volatility
    vol_df = calculate_volatility(sample_df, window=30)
    latest_vol = vol_df["volatility"].iloc[-1]
    print(f"\n📉 Annualized Volatility (latest): {latest_vol:.4f}")

    # Anomaly detection
    anomaly_df = detect_price_anomalies(sample_df)
    anomaly_count = anomaly_df["is_anomaly"].sum()
    print(f"\n⚠️  Anomalies detected: {anomaly_count} out of {len(anomaly_df)} data points")

    print("\n" + "=" * 60)
    print("  Analysis complete.")
    print("=" * 60)
