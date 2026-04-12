"""
fetch_prices_all_countries.py
------------------------------
ENTSO-E Transparency Platform'dan AB ülkelerinin
Day-Ahead spot fiyatlarını çeker.

Ülkeler: DE, FR, IT, ES, NL, BE + TR (EPIAS)
"""

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
ENTSO_E_TOKEN = os.environ.get("ENTSO_E_TOKEN", "")
BASE_URL      = "https://web-api.tp.entsoe.eu/api"
OUTPUT_DIR    = os.path.join(os.path.dirname(__file__), "..", "data", "output")
OUTPUT_FILE   = os.path.join(OUTPUT_DIR, "prices_europe.csv")

# Bidding zone EIC kodları
BIDDING_ZONES = {
    "DE": "10Y1001A1001A82H",   # Germany-Luxembourg
    "FR": "10YFR-RTE------C",   # France
    "IT": "10Y1001A1001A73I",   # Italy
    "ES": "10YES-REE------0",   # Spain
    "NL": "10YNL----------L",   # Netherlands
    "BE": "10YBE----------2",   # Belgium
}

NS = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}


def fetch_day_ahead_prices(country: str, domain: str,
                            start: datetime, end: datetime) -> pd.DataFrame:
    """Bir ülkenin day-ahead fiyatlarını çeker."""
    params = {
        "securityToken": ENTSO_E_TOKEN,
        "documentType":  "A44",          # Price document
        "in_Domain":     domain,
        "out_Domain":    domain,
        "periodStart":   start.strftime("%Y%m%d%H%M"),
        "periodEnd":     end.strftime("%Y%m%d%H%M"),
    }

    log.info(f"  Fetching {country}: {start.date()} → {end.date()}")
    try:
        resp = requests.get(BASE_URL, params=params, timeout=60)
        if resp.status_code != 200:
            log.warning(f"  {country} HTTP {resp.status_code}")
            return pd.DataFrame()
        return _parse_price_xml(resp.text, country)
    except Exception as e:
        log.warning(f"  {country} error: {e}")
        return pd.DataFrame()


def _parse_price_xml(xml_text: str, country: str) -> pd.DataFrame:
    """ENTSO-E fiyat XML'ini parse eder."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        log.warning(f"XML parse error for {country}: {e}")
        return pd.DataFrame()

    records = []
    for ts in root.findall(".//ns:TimeSeries", NS):
        currency = ts.findtext("ns:currency_Unit.name", namespaces=NS, default="EUR")
        unit     = ts.findtext("ns:price_Measure_Unit.name", namespaces=NS, default="MWH")

        for period in ts.findall("ns:Period", NS):
            start_str  = period.findtext("ns:timeInterval/ns:start", namespaces=NS)
            resolution = period.findtext("ns:resolution", namespaces=NS, default="PT60M")

            if not start_str:
                continue

            start_dt     = datetime.strptime(start_str, "%Y-%m-%dT%H:%MZ")
            interval_min = 60 if resolution == "PT60M" else 15

            for point in period.findall("ns:Point", NS):
                pos   = int(point.findtext("ns:position", namespaces=NS, default="1"))
                price = point.findtext("ns:price.amount", namespaces=NS)

                if price is not None:
                    ts_utc = start_dt + timedelta(minutes=(pos - 1) * interval_min)
                    records.append({
                        "timestamp_utc": ts_utc,
                        "country":       country,
                        "price_eur_mwh": round(float(price), 2),
                        "currency":      currency,
                        "unit":          unit,
                    })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df["date"]  = df["timestamp_utc"].dt.date
    df["hour"]  = df["timestamp_utc"].dt.hour
    df["year"]  = df["timestamp_utc"].dt.year
    df["month"] = df["timestamp_utc"].dt.month
    df["weekday"] = df["timestamp_utc"].dt.day_name()
    df["is_weekend"] = df["timestamp_utc"].dt.weekday >= 5
    df["negative_price"] = df["price_eur_mwh"] < 0
    df["price_spike"]    = df["price_eur_mwh"] > 200   # Spike threshold

    return df


def fetch_turkey_prices(start: datetime, end: datetime) -> pd.DataFrame:
    """
    Türkiye PTF fiyatlarını çeker (EPIAS Transparency Platform).
    Token gerekmez — public API.
    """
    log.info(f"  Fetching TR (EPIAS): {start.date()} → {end.date()}")
    records = []

    current = start
    while current < end:
        date_str = current.strftime("%d-%m-%Y")
        url = f"https://seffaflik.epias.com.tr/transparency/service/market/ptf-smf?startDate={date_str}&endDate={date_str}"

        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                items = data.get("body", {}).get("ptfSmfList", [])
                for item in items:
                    dt_str = item.get("date", "")
                    ptf    = item.get("ptf", None)
                    if dt_str and ptf is not None:
                        try:
                            dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S+03:00")
                            records.append({
                                "timestamp_utc": pd.Timestamp(dt).tz_localize("Europe/Istanbul").tz_convert("UTC"),
                                "country":       "TR",
                                "price_eur_mwh": round(float(ptf) / 35, 2),  # TRY → EUR (approx)
                                "currency":      "TRY",
                                "unit":          "MWH",
                            })
                        except Exception:
                            pass
        except Exception as e:
            log.warning(f"  TR EPIAS error for {date_str}: {e}")

        current += timedelta(days=1)

    if not records:
        log.warning("  TR: No data fetched")
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["date"]     = pd.to_datetime(df["timestamp_utc"]).dt.date
    df["hour"]     = pd.to_datetime(df["timestamp_utc"]).dt.hour
    df["year"]     = pd.to_datetime(df["timestamp_utc"]).dt.year
    df["month"]    = pd.to_datetime(df["timestamp_utc"]).dt.month
    df["weekday"]  = pd.to_datetime(df["timestamp_utc"]).dt.day_name()
    df["is_weekend"]     = pd.to_datetime(df["timestamp_utc"]).dt.weekday >= 5
    df["negative_price"] = df["price_eur_mwh"] < 0
    df["price_spike"]    = df["price_eur_mwh"] > 200

    return df


def build_pivot_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Power BI heatmap için pivot tablo:
    Rows: date × hour
    Columns: country fiyatları
    """
    if df.empty:
        return df

    pivot = df.pivot_table(
        index=["date", "hour"],
        columns="country",
        values="price_eur_mwh",
        aggfunc="mean"
    ).reset_index()
    pivot.columns.name = None

    # Spread hesapla
    country_cols = [c for c in BIDDING_ZONES.keys() if c in pivot.columns]
    if len(country_cols) > 1:
        pivot["max_price"]     = pivot[country_cols].max(axis=1)
        pivot["min_price"]     = pivot[country_cols].min(axis=1)
        pivot["price_spread"]  = (pivot["max_price"] - pivot["min_price"]).round(2)
        pivot["cheapest"]      = pivot[country_cols].idxmin(axis=1)
        pivot["most_expensive"] = pivot[country_cols].idxmax(axis=1)

    return pivot


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    end_date   = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=90)

    all_frames = []

    # AB ülkeleri — ENTSO-E
    for country, domain in BIDDING_ZONES.items():
        df = fetch_day_ahead_prices(country, domain, start_date, end_date)
        if not df.empty:
            all_frames.append(df)
            log.info(f"  ✅ {country}: {len(df)} rows")
        else:
            log.warning(f"  ⚠️ {country}: No data")


    if not all_frames:
        log.error("No data fetched!")
        return

    # Long format (tüm ülkeler tek tabloda)
    df_all = pd.concat(all_frames, ignore_index=True)
    df_all = df_all.sort_values(["timestamp_utc", "country"])
    df_all.to_csv(OUTPUT_FILE, index=False)
    log.info(f"✅ Long format saved: {len(df_all)} rows → {OUTPUT_FILE}")

    # Pivot format (Power BI heatmap için)
    pivot_file = os.path.join(OUTPUT_DIR, "prices_pivot.csv")
    df_pivot = build_pivot_table(df_all)
    df_pivot.to_csv(pivot_file, index=False)
    log.info(f"✅ Pivot saved: {len(df_pivot)} rows → {pivot_file}")

    # Özet istatistikler
    print("\n📊 Son 24 saatin özeti:")
    recent = df_all[df_all["timestamp_utc"] >= pd.Timestamp.utcnow() - timedelta(hours=24)]
    if not recent.empty:
        summary = recent.groupby("country")["price_eur_mwh"].agg(["mean", "min", "max"])
        print(summary.round(2).to_string())


if __name__ == "__main__":
    main()
