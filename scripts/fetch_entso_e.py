"""
fetch_entso_e.py
----------------
ENTSO-E Transparency Platform'dan üretim karması (generation mix) çeker.
API token: .env dosyasından veya GitHub Secrets'tan okunur.

Kaynak: https://transparency.entsoe.eu/
"""

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import logging

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
ENTSO_E_TOKEN = os.environ.get("ENTSO_E_TOKEN", "")   # GitHub Secret'tan gelir
BASE_URL      = "https://web-api.tp.entsoe.eu/api"
DOMAIN_DE     = "10Y1001A1001A83F"   # Germany (DE-LU bidding zone)
OUTPUT_DIR    = os.path.join(os.path.dirname(__file__), "..", "data", "output")
OUTPUT_FILE   = os.path.join(OUTPUT_DIR, "generation_mix.csv")

# ENTSO-E PsrType kodları → okunabilir isimler
PSR_TYPE_MAP = {
    "B01": "Biomass",
    "B02": "Fossil Brown Coal/Lignite",
    "B03": "Fossil Coal-derived Gas",
    "B04": "Fossil Gas",
    "B05": "Fossil Hard Coal",
    "B06": "Fossil Oil",
    "B09": "Geothermal",
    "B10": "Hydro Pumped Storage",
    "B11": "Hydro Run-of-river",
    "B12": "Hydro Water Reservoir",
    "B14": "Nuclear",
    "B15": "Other Renewable",
    "B16": "Solar",
    "B17": "Waste",
    "B18": "Wind Offshore",
    "B19": "Wind Onshore",
    "B20": "Other",
}

# Renewable kaynak kodları
RENEWABLE_CODES = {"B01", "B09", "B10", "B11", "B12", "B15", "B16", "B18", "B19"}


def fetch_generation_mix(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    ENTSO-E API'den aktüel üretim karmasını çeker.
    Dönüş: saatlik bazda kaynak tipine göre MW üretim
    """
    if not ENTSO_E_TOKEN:
        log.warning("ENTSO_E_TOKEN bulunamadı — mock data üretiliyor")
        return _generate_mock_data(start_date, end_date)

    params = {
        "securityToken": ENTSO_E_TOKEN,
        "documentType":  "A75",           # Actual Generation Per Type
        "processType":   "A16",           # Realised
        "in_Domain":     DOMAIN_DE,
        "periodStart":   start_date.strftime("%Y%m%d%H%M"),
        "periodEnd":     end_date.strftime("%Y%m%d%H%M"),
    }

    log.info(f"Fetching generation mix: {start_date.date()} → {end_date.date()}")
    resp = requests.get(BASE_URL, params=params, timeout=60)

    if resp.status_code != 200:
        log.error(f"ENTSO-E API error {resp.status_code}: {resp.text[:200]}")
        return _generate_mock_data(start_date, end_date)

    return _parse_xml_response(resp.text)


def _parse_xml_response(xml_text: str) -> pd.DataFrame:
    """ENTSO-E XML yanıtını parse eder."""
    ns = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
    root = ET.fromstring(xml_text)
    records = []

    for ts in root.findall(".//ns:TimeSeries", ns):
        psr_type = ts.findtext(".//ns:psrType", namespaces=ns, default="Unknown")
        source_name = PSR_TYPE_MAP.get(psr_type, psr_type)

        for period in ts.findall("ns:Period", ns):
            start_str = period.findtext("ns:timeInterval/ns:start", namespaces=ns)
            resolution = period.findtext("ns:resolution", namespaces=ns)

            if not start_str:
                continue

            start_dt = datetime.strptime(start_str, "%Y-%m-%dT%H:%MZ")
            interval_min = 60 if resolution == "PT60M" else 15

            for point in period.findall("ns:Point", ns):
                pos = int(point.findtext("ns:position", namespaces=ns, default="1"))
                qty = point.findtext("ns:quantity", namespaces=ns)

                if qty is not None:
                    ts_dt = start_dt + timedelta(minutes=(pos - 1) * interval_min)
                    records.append({
                        "timestamp":   pd.to_datetime(ts_dt, utc=True)
                                         .tz_convert("Europe/Berlin"),
                        "source":      source_name,
                        "psr_type":    psr_type,
                        "mw":          float(qty),
                        "is_renewable": psr_type in RENEWABLE_CODES,
                    })

    df = pd.DataFrame(records)
    if df.empty:
        log.warning("XML parse sonucu boş DataFrame")
        return df

    # Saatlik bazda pivot
    df_pivot = df.pivot_table(
        index="timestamp", columns="source", values="mw", aggfunc="mean"
    ).reset_index()
    df_pivot.columns.name = None

    # Aggregate sütunlar
    renewable_cols = [PSR_TYPE_MAP[c] for c in RENEWABLE_CODES if PSR_TYPE_MAP[c] in df_pivot.columns]
    df_pivot["total_renewable_mw"] = df_pivot[renewable_cols].sum(axis=1)
    df_pivot["total_generation_mw"] = df_pivot.select_dtypes("number").sum(axis=1)
    df_pivot["renewable_share_pct"] = (
        df_pivot["total_renewable_mw"] / df_pivot["total_generation_mw"] * 100
    ).round(1)

    return df_pivot


def _generate_mock_data(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Token yokken gerçekçi mock data üretir.
    Token gelince bu fonksiyon otomatik devre dışı kalır.
    """
    import numpy as np
    log.info("Generating mock generation mix data...")
    np.random.seed(42)

    hours = pd.date_range(start=start_date, end=end_date, freq="h", tz="Europe/Berlin")
    n = len(hours)
    hour_of_day = hours.hour

    # Güneş üretimi: gündüz peak
    solar = np.where(
        (hour_of_day >= 6) & (hour_of_day <= 20),
        np.maximum(0, 20000 * np.sin(np.pi * (hour_of_day - 6) / 14) + np.random.normal(0, 1000, n)),
        0
    )

    df = pd.DataFrame({
        "timestamp":             hours,
        "Solar":                 solar.round(0),
        "Wind Onshore":          np.random.normal(15000, 5000, n).clip(0),
        "Wind Offshore":         np.random.normal(5000, 2000, n).clip(0),
        "Fossil Gas":            np.random.normal(8000, 2000, n).clip(0),
        "Fossil Hard Coal":      np.random.normal(5000, 1500, n).clip(0),
        "Fossil Brown Coal/Lignite": np.random.normal(6000, 1000, n).clip(0),
        "Biomass":               np.random.normal(4000, 500, n).clip(0),
        "Hydro Run-of-river":    np.random.normal(2000, 500, n).clip(0),
    })

    renewable_cols = ["Solar", "Wind Onshore", "Wind Offshore", "Biomass", "Hydro Run-of-river"]
    df["total_renewable_mw"]    = df[renewable_cols].sum(axis=1).round(0)
    df["total_generation_mw"]   = df.select_dtypes("number").sum(axis=1).round(0)
    df["renewable_share_pct"]   = (df["total_renewable_mw"] / df["total_generation_mw"] * 100).round(1)
    df["is_mock_data"]          = True   # Power BI'da filtre için

    return df


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    end_date   = datetime.utcnow()
    start_date = end_date - timedelta(days=90)

    df = fetch_generation_mix(start_date, end_date)

    if df.empty:
        log.error("Veri çekilemedi, çıkılıyor.")
        return

    df.to_csv(OUTPUT_FILE, index=False)
    log.info(f"✅ Saved {len(df)} rows → {OUTPUT_FILE}")

    if "renewable_share_pct" in df.columns:
        log.info(f"   Avg renewable share: {df['renewable_share_pct'].mean():.1f}%")


if __name__ == "__main__":
    main()
