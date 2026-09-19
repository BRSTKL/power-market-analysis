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

# ── SMARD API (Germany) ──────────────────────────────────────────────────────

def fetch_smard_data(
    filter_id: int = 4169,
    region: str = "DE",
    resolution: str = "hour",
) -> Optional[pd.DataFrame]:
    """
    Fetch German day-ahead electricity prices from the SMARD API.

    Uses a two-step process:
      1. Fetch available timestamps from the index endpoint.
      2. Retrieve price data for the latest timestamp.

    Parameters
    ----------
    filter_id : int
        SMARD filter code (default 4169 = DE/LU market price).
    region : str
        Region code (default 'DE').
    resolution : str
        Time resolution ('hour', 'quarterhour', 'day', 'week', 'month').

    Returns
    -------
    pd.DataFrame or None
        DataFrame with columns ['Datetime', 'Price_EUR_MWh'], or None on failure.
    """
    # Step 1: Get the index of available timestamps
    index_url = (
        f"https://www.smard.de/app/chart_data/{filter_id}/{region}"
        f"/index_{resolution}.json"
    )
    print(f"Fetching timestamp index from SMARD API...")
    try:
        resp = requests.get(index_url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"Index request failed: {e}")
        return None

    timestamps = resp.json().get("timestamps", [])
    if not timestamps:
        print("No timestamps available.")
        return None

    # Step 2: Fetch data for the latest timestamp
    latest_ts = timestamps[-1]
    data_url = (
        f"https://www.smard.de/app/chart_data/{filter_id}/{region}"
        f"/{filter_id}_{region}_{resolution}_{latest_ts}.json"
    )
    print(f"Fetching price data (timestamp: {latest_ts})...")
    try:
        resp = requests.get(data_url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"Data request failed: {e}")
        return None

    series_data = resp.json().get("series", [])
    if not series_data:
        print("No series data found.")
        return None

    # Build DataFrame
    df = pd.DataFrame(series_data, columns=["Timestamp", "Price_EUR_MWh"])
    df["Datetime"] = pd.to_datetime(df["Timestamp"], unit="ms")
    df = df[["Datetime", "Price_EUR_MWh"]]

    # Drop rows where price is None (gaps in data)
    df.dropna(subset=["Price_EUR_MWh"], inplace=True)
    df.reset_index(drop=True, inplace=True)

    print(f"\n[OK] Data fetched successfully! {len(df)} records retrieved.")
    print(df.head())
    return df


def fetch_smard_fundamentals(
    weeks: int = 4,
    region: str = "DE",
    resolution: str = "hour",
) -> Optional[pd.DataFrame]:
    """
    Fetch electricity prices and physical market fundamentals from SMARD API:
      - 4169: Day-Ahead Electricity Price (EUR/MWh)
      - 410:  Total Grid Load / Power Consumption (MWh)
      - 4359: Residual Load (MWh)
      - 4068: Photovoltaic / Solar Generation (MWh)
      - 4067: Wind Onshore Generation (MWh)
      - 1225: Wind Offshore Generation (MWh)

    Parameters
    ----------
    weeks : int
        Number of historical weeks to fetch (each timestamp block is 1 week = 168h).
    region : str
        Region code (default 'DE').
    resolution : str
        Time resolution (default 'hour').

    Returns
    -------
    pd.DataFrame or None
        Combined DataFrame with physical fundamentals and prices aligned by timestamp.
    """
    filter_map = {
        "Price_EUR_MWh": 4169,
        "Load_MWh": 410,
        "Residual_Load_MWh": 4359,
        "Solar_MWh": 4068,
        "Wind_Onshore_MWh": 4067,
        "Wind_Offshore_MWh": 1225,
    }

    # Fetch available timestamps from price index
    index_url = f"https://www.smard.de/app/chart_data/4169/{region}/index_{resolution}.json"
    print(f"Fetching timestamp index for {weeks} weeks of data...")
    try:
        resp = requests.get(index_url, timeout=30)
        resp.raise_for_status()
        all_ts = resp.json().get("timestamps", [])
    except requests.RequestException as e:
        print(f"Failed to fetch timestamp index: {e}")
        return None

    if not all_ts:
        print("No timestamps found.")
        return None

    target_ts = all_ts[-weeks:]
    print(f"Fetching {len(target_ts)} week(s) of physical fundamental data...")

    dfs = {}
    for col_name, fid in filter_map.items():
        all_series = []
        for ts in target_ts:
            url = f"https://www.smard.de/app/chart_data/{fid}/{region}/{fid}_{region}_{resolution}_{ts}.json"
            try:
                r = requests.get(url, timeout=20)
                if r.status_code == 200:
                    data = r.json().get("series", [])
                    all_series.extend(data)
            except requests.RequestException:
                pass
        
        df_temp = pd.DataFrame(all_series, columns=["Timestamp", col_name]).dropna()
        df_temp.drop_duplicates(subset=["Timestamp"], inplace=True)
        dfs[col_name] = df_temp

    # Merge all metrics on Timestamp
    merged = dfs["Price_EUR_MWh"]
    for col_name in list(filter_map.keys())[1:]:
        if col_name in dfs and not dfs[col_name].empty:
            merged = pd.merge(merged, dfs[col_name], on="Timestamp", how="inner")

    if merged.empty:
        print("Failed to merge fundamental datasets.")
        return None

    merged["Datetime"] = pd.to_datetime(merged["Timestamp"], unit="ms")
    merged.sort_values("Datetime", inplace=True)
    merged.reset_index(drop=True, inplace=True)

    # Derived fundamental market metrics
    merged["Total_Renewable_MWh"] = (
        merged["Solar_MWh"] + merged["Wind_Onshore_MWh"] + merged["Wind_Offshore_MWh"]
    )
    merged["Renewable_Share"] = merged["Total_Renewable_MWh"] / (merged["Load_MWh"] + 1e-5)

    print(f"[OK] Fetched {len(merged)} hourly records with fundamentals.")
    return merged


# ── Entry Point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  Energy Market Data - SMARD Germany Analysis")
    print("=" * 60)

    # Fetch real data from SMARD API
    df = fetch_smard_data()

    if df is not None:
        # Calculate statistics
        stats = calculate_price_statistics(df, column="Price_EUR_MWh")
        print("\n[STATS] Price Statistics (EUR/MWh):")
        for key, value in stats.items():
            print(f"   {key:>12}: {value}")

        # Moving averages
        ma_df = calculate_moving_average(df, column="Price_EUR_MWh", window=24)
        print(f"\n[SMA] 24-hour SMA (latest): {ma_df['SMA_24'].iloc[-1]:.2f} EUR/MWh")
        print(f"[EMA] 24-hour EMA (latest): {ma_df['EMA_24'].iloc[-1]:.2f} EUR/MWh")

        # Volatility
        vol_df = calculate_volatility(df, column="Price_EUR_MWh", window=24)
        latest_vol = vol_df["volatility"].dropna().iloc[-1]
        print(f"\n[VOL] Annualized Volatility (latest): {latest_vol:.4f}")

        # Anomaly detection
        anomaly_df = detect_price_anomalies(df, column="Price_EUR_MWh")
        anomaly_count = anomaly_df["is_anomaly"].sum()
        print(f"\n[!] Anomalies detected: {anomaly_count} out of {len(anomaly_df)} data points")

    print("\n" + "=" * 60)
    print("  Analysis complete.")
    print("=" * 60)
