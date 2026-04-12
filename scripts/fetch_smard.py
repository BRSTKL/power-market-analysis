"""
fetch_smard.py
--------------
SMARD API'den Almanya Day-Ahead spot fiyatlarını çeker.
Her gün otomatik olarak GitHub Actions tarafından çalıştırılır.

Veri kaynağı: https://www.smard.de/en/downloadcenter/download-market-data
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import json
import logging

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
BASE_URL   = "https://www.smard.de/app/chart_data"
FILTER_ID  = 4169   # Day-Ahead Spot Price (DE-LU)
REGION     = "DE"
RESOLUTION = "hour"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "output")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "prices_daily.csv")


def get_available_timestamps() -> list:
    """SMARD'dan mevcut timestamp index'ini çeker."""
    url = f"{BASE_URL}/{FILTER_ID}/{REGION}/index_{RESOLUTION}.json"
    log.info(f"Fetching index: {url}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json().get("timestamps", [])


def fetch_smard_data(timestamp: int) -> pd.DataFrame:
    """Belirli bir timestamp için saatlik fiyat verisini çeker."""
    url = (
    f"{BASE_URL}/{FILTER_ID}/{REGION}"
    f"/{FILTER_ID}_{REGION}_{RESOLUTION}_{timestamp}.json"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    series = resp.json().get("series", [])

    records = []
    for ts, price in series:
        if price is not None:
            records.append({
    "timestamp": pd.to_datetime(ts, unit="ms", utc=True)
                   .tz_convert("Europe/Berlin"),
    "price_eur_mwh": round(price / 10, 2)  # SMARD → EUR/MWh dönüşümü
})
    return pd.DataFrame(records)


def load_existing_data() -> pd.DataFrame:
    """Mevcut CSV'yi yükler (varsa)."""
    if os.path.exists(OUTPUT_FILE):
        df = pd.read_csv(OUTPUT_FILE, parse_dates=["timestamp"])
        log.info(f"Existing data loaded: {len(df)} rows")
        return df
    return pd.DataFrame(columns=["timestamp", "price_eur_mwh"])


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """Analiz için türetilmiş sütunlar ekler."""
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["date"]          = pd.to_datetime(df["timestamp"]).dt.date
    df["hour"]          = pd.to_datetime(df["timestamp"]).dt.hour
    df["weekday"]       = pd.to_datetime(df["timestamp"]).dt.day_name()
    df["is_weekend"]    = pd.to_datetime(df["timestamp"]).dt.weekday >= 5
    df["month"]         = pd.to_datetime(df["timestamp"]).dt.month
    df["year"]          = pd.to_datetime(df["timestamp"]).dt.year

    # Rolling features
    df["price_ma24h"]   = df["price_eur_mwh"].rolling(24, min_periods=1).mean().round(2)
    df["price_ma7d"]    = df["price_eur_mwh"].rolling(24*7, min_periods=1).mean().round(2)
    df["price_std24h"]  = df["price_eur_mwh"].rolling(24, min_periods=1).std().round(2)

    # Negative price flag (önemli trading sinyali)
    df["negative_price"] = df["price_eur_mwh"] < 0

    # Peak / Off-peak (DE market convention)
    df["is_peak"] = df["hour"].between(8, 20) & ~df["is_weekend"]

    return df


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Mevcut veriyi yükle
    existing_df = load_existing_data()

    # Son 90 günlük timestamp'leri çek
    timestamps = get_available_timestamps()
    recent_timestamps = timestamps[-90:]  # Son ~90 gün

    all_frames = [existing_df]
    new_rows   = 0

    for ts in recent_timestamps:
        try:
            df_chunk = fetch_smard_data(ts)
            all_frames.append(df_chunk)
            new_rows += len(df_chunk)
            log.info(f"  Fetched {len(df_chunk)} rows for ts={ts}")
        except Exception as e:
            log.warning(f"  Failed for ts={ts}: {e}")

    # Birleştir, duplikasyonları temizle
    merged = pd.concat(all_frames, ignore_index=True)
    merged["timestamp"] = pd.to_datetime(merged["timestamp"], utc=True)
    merged = merged.drop_duplicates(subset=["timestamp"]).sort_values("timestamp")

    # Feature engineering
    merged = add_features(merged)

    # Kaydet
    merged.to_csv(OUTPUT_FILE, index=False)
    log.info(f"✅ Saved {len(merged)} rows → {OUTPUT_FILE}")
    log.info(f"   Date range: {merged['timestamp'].min()} → {merged['timestamp'].max()}")
    log.info(f"   Avg price: {merged['price_eur_mwh'].mean():.2f} EUR/MWh")
    log.info(f"   Negative price hours: {merged['negative_price'].sum()}")


if __name__ == "__main__":
    main()
