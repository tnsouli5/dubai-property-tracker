#!/usr/bin/env python3
"""
Update DLD price/sqm benchmarks from a fresh transactions CSV.

Usage:
  python3 update_benchmarks.py Real-estate_Transactions_2026-03-24.csv

Download the latest CSV from:
  https://www.dubaipulse.gov.ae/data/dld-transactions/dld_transactions-open
  (click the CSV download button — no account needed)
"""

import sys, json, re
import pandas as pd
from pathlib import Path

SCRAPE_FILE = Path(__file__).parent / "scrape.py"

AREA_MAP = {
    "dubai-hills-estate":      "Hadaeq Sheikh Mohammed Bin Rashid",
    "the-springs":             "Al Thanayah Fourth",
    "the-meadows":             "Al Thanayah Fourth",
    "the-lakes":               "Al Thanyah Third",
    "madinat-jumeirah-living": None,
}

BEDS_MAP  = {3: "3 B/R", 4: "4 B/R"}
TYPE_MAP  = {"villa": ["Villa"], "townhouse": ["Unit"]}
SALE_PROCS = ["Sell", "Sell - Pre registration", "Delayed Sell"]

def compute_benchmarks(csv_path):
    print(f"Reading {csv_path}...")
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df):,} rows.")

    results = {}
    for slug, area_en in AREA_MAP.items():
        for beds_n, beds_label in BEDS_MAP.items():
            for ptype, ptype_vals in TYPE_MAP.items():
                key = f"{slug}|{beds_n}|{ptype}"
                if not area_en:
                    results[key] = {"avg_psqm": None, "sample_size": 0}
                    continue

                sub = df[
                    (df["area_name_en"] == area_en) &
                    (df["property_type_en"].isin(ptype_vals)) &
                    (df["procedure_name_en"].isin(SALE_PROCS)) &
                    (df["rooms_en"] == beds_label) &
                    (df["meter_sale_price"].notna()) &
                    (df["meter_sale_price"] > 0)
                ].sort_values("instance_date", ascending=False).head(5)

                if len(sub):
                    avg = round(sub["meter_sale_price"].mean())
                    dr  = f"{sub['instance_date'].min()[:4]}–{sub['instance_date'].max()[:4]}"
                    results[key] = {"avg_psqm": avg, "sample_size": len(sub), "date_range": dr}
                    print(f"  {key}: AED {avg:,}/sqm (n={len(sub)}, {dr})")
                else:
                    results[key] = {"avg_psqm": None, "sample_size": 0}
                    print(f"  {key}: no data")

    return results

def inject_into_scrape(benchmarks):
    code = SCRAPE_FILE.read_text()
    lines = []
    for k, v in benchmarks.items():
        if v["avg_psqm"]:
            dr = v.get("date_range", "")
            lines.append(f'    "{k}": {{"avg_psqm": {v["avg_psqm"]}, "sample_size": {v["sample_size"]}, "date_range": "{dr}"}},')
        else:
            lines.append(f'    "{k}": {{"avg_psqm": None, "sample_size": 0}},')

    new_block = "DLD_BENCHMARKS = {\n" + "\n".join(lines) + "\n}"
    code = re.sub(r"DLD_BENCHMARKS = \{.*?\}", new_block, code, flags=re.DOTALL)
    SCRAPE_FILE.write_text(code)
    print(f"\nUpdated DLD_BENCHMARKS in {SCRAPE_FILE}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 update_benchmarks.py <path-to-csv>")
        sys.exit(1)
    benchmarks = compute_benchmarks(sys.argv[1])
    inject_into_scrape(benchmarks)
    print("Done! Run scrape.py to apply the new benchmarks.")
