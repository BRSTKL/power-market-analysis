"""
merge_data.py
-------------
SMARD + ENTSO-E verilerini birleştirip Power BI için
tek bir unified dataset üretir.
"""

import pandas as pd
import numpy as np
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DATA_DIR   = os.path.join(os.path.dirname(__file__), "..", "data", "output")
PRICES_F   = os.path.join(DATA_DIR, "prices_daily.csv")
GENMIX_F   = os.path.join(DATA_DIR, "generation_mix.csv")
OUTPUT_F   = os.path.join(DATA_DIR, "merged_dataset.csv")
SUMMARY_F  = os.path.join(DATA_DIR, "daily_summary.csv")


def load_prices() -> pd.DataFrame:
    df = pd.read_csv(PRICES_F, parse_dates=["timestamp"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert("Europe/Berlin")
    # DST geçiş anlarında ambiguous hatası önle
    df["timestamp_h"] = df["timestamp"].dt.tz_convert("UTC").dt.floor("h").dt.tz_convert("Europe/Berlin")
    log.info(f"Prices loaded: {len(df)} rows")
    return df


def load_generation() -> pd.DataFrame:
    df = pd.read_csv(GENMIX_F, parse_dates=["timestamp"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert("Europe/Berlin")
    df["timestamp_h"] = df["timestamp"].dt.floor("h")
    log.info(f"Generation mix loaded: {len(df)} rows")
    return df


def compute_spark_spread(df: pd.DataFrame) -> pd.DataFrame:
    """
    Spark Spread = Power Price - (Gas Price / Efficiency)
    Gas fiyatı mock (gerçek için TTF API eklenebilir)
    Efficiency: CCGT için ~0.50
    """
    GAS_EFFICIENCY = 0.50
    # TTF mock: EUR/MWh_th (yaklaşık değer — gerçek veriye geçilince değiştir)
    MOCK_GAS_PRICE_EUR_MWTH = 35.0

    df["gas_price_eur_mwh_mock"]  = MOCK_GAS_PRICE_EUR_MWTH
    df["spark_spread_eur_mwh"]    = (
        df["price_eur_mwh"] - (MOCK_GAS_PRICE_EUR_MWTH / GAS_EFFICIENCY)
    ).round(2)
    df["spark_spread_positive"]   = df["spark_spread_eur_mwh"] > 0
    return df


def compute_var_cvar(series: pd.Series, confidence: float = 0.95) -> dict:
    """Basit historical VaR/CVaR hesabı."""
    sorted_returns = series.dropna().sort_values()
    idx = int((1 - confidence) * len(sorted_returns))
    var  = sorted_returns.iloc[idx]
    cvar = sorted_returns.iloc[:idx].mean()
    return {"VaR_95": round(var, 2), "CVaR_95": round(cvar, 2)}


def build_daily_summary(df_merged: pd.DataFrame) -> pd.DataFrame:
    """Power BI için günlük özet tablo."""
    agg = df_merged.groupby("date").agg(
        avg_price       =("price_eur_mwh", "mean"),
        max_price       =("price_eur_mwh", "max"),
        min_price       =("price_eur_mwh", "min"),
        std_price       =("price_eur_mwh", "std"),
        negative_hours  =("negative_price", "sum"),
        peak_avg_price  =("price_eur_mwh", lambda x: x[df_merged.loc[x.index, "is_peak"]].mean()),
        offpeak_avg_price=("price_eur_mwh", lambda x: x[~df_merged.loc[x.index, "is_peak"]].mean()),
    ).reset_index()

    if "renewable_share_pct" in df_merged.columns:
        ren = df_merged.groupby("date")["renewable_share_pct"].mean().reset_index()
        ren.columns = ["date", "avg_renewable_share_pct"]
        agg = agg.merge(ren, on="date", how="left")

    if "spark_spread_eur_mwh" in df_merged.columns:
        ss = df_merged.groupby("date")["spark_spread_eur_mwh"].mean().reset_index()
        ss.columns = ["date", "avg_spark_spread"]
        agg = agg.merge(ss, on="date", how="left")

    # Fiyat rejimleri (Power BI'da renk kodlama için)
    agg["price_regime"] = pd.cut(
        agg["avg_price"],
        bins=[-999, 0, 50, 100, 200, 9999],
        labels=["Negative", "Low (<50)", "Medium (50-100)", "High (100-200)", "Extreme (>200)"]
    )

    agg = agg.round(2)
    return agg


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # Veri yükle
    df_prices = load_prices()
    df_gen    = load_generation()

    # Join (timestamp bazında)
    df_merged = df_prices.merge(df_gen, on="timestamp_h", how="left", suffixes=("", "_gen"))
    df_merged = df_merged.drop(columns=["timestamp_gen"], errors="ignore")

    # Feature engineering
    df_merged = compute_spark_spread(df_merged)

    # Fiyat dönüşü (trading P&L için)
    df_merged["price_return_pct"] = df_merged["price_eur_mwh"].pct_change() * 100

    # VaR/CVaR — loglara yaz
    var_stats = compute_var_cvar(df_merged["price_return_pct"])
    log.info(f"📊 Price Return VaR(95%): {var_stats['VaR_95']}% | CVaR(95%): {var_stats['CVaR_95']}%")

    # Metadata ekle
    df_merged["last_updated"] = pd.Timestamp.now(tz="Europe/Berlin").strftime("%Y-%m-%d %H:%M")

    # Kaydet
    df_merged.to_csv(OUTPUT_F, index=False)
    log.info(f"✅ Merged dataset saved: {len(df_merged)} rows → {OUTPUT_F}")

    # Günlük özet
    df_summary = build_daily_summary(df_merged)
    df_summary.to_csv(SUMMARY_F, index=False)
    log.info(f"✅ Daily summary saved: {len(df_summary)} rows → {SUMMARY_F}")

    # Son 3 günün özeti
    print("\n📋 Son 3 Günün Özeti:")
    print(df_summary.tail(3)[
        ["date", "avg_price", "max_price", "min_price", "negative_hours"]
    ].to_string(index=False))


if __name__ == "__main__":
    main()
