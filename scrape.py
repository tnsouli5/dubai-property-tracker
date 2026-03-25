#!/usr/bin/env python3
"""
Dubai Property Scraper — Playwright edition
Uses a real headless browser so Bayut can't detect it as a bot.

FIRST TIME SETUP (run once in Terminal):
  pip3 install playwright
  python3 -m playwright install chromium

To refresh benchmarks: download a new CSV from dubaipulse.gov.ae
(dld_transactions dataset) and run: python3 update_benchmarks.py <file.csv>
"""

import json, time, random, re
from datetime import datetime
from pathlib import Path

# ── Configuration ──────────────────────────────────────────────────────────────

AREAS = [
    "dubai-hills-estate",
    "the-springs",
    "the-meadows",
    "the-lakes",
    "madinat-jumeirah-living",
]

AREA_LABELS = {
    "dubai-hills-estate":      "Dubai Hills Estate",
    "the-springs":             "The Springs",
    "the-meadows":             "The Meadows",
    "the-lakes":               "The Lakes",
    "madinat-jumeirah-living": "Madinat Jumeirah Living",
}

BEDROOMS       = [3, 4]
PROPERTY_TYPES = ["villa", "townhouse"]
PURPOSES       = ["for-sale", "for-rent"]
MAX_AGE_DAYS   = 7

OUTPUT_FILE    = Path(__file__).parent / "listings.json"
DASHBOARD_FILE = Path(__file__).parent / "dashboard.html"

# ── DLD Benchmarks (from Real-estate_Transactions_2026-03-24.csv) ─────────────
# Format: "area-slug|beds|type" -> avg AED/sqm based on last 5 sales
# Re-run update_benchmarks.py with a newer CSV to refresh these.
# NOTE: Some areas/combos have limited data in this sample — upload the full
# DLD dataset from dubaipulse.gov.ae for better coverage.

DLD_BENCHMARKS = {
    # Dubai Hills Estate
    "dubai-hills-estate|3|villa":      {"avg_psqm": 11825, "sample_size": 5, "date_range": "2018–2021"},
    "dubai-hills-estate|3|townhouse":  {"avg_psqm": 20077, "sample_size": 5, "date_range": "2014–2024"},
    "dubai-hills-estate|4|villa":      {"avg_psqm": 13930, "sample_size": 5, "date_range": "2016–2025"},
    "dubai-hills-estate|4|townhouse":  {"avg_psqm": None,  "sample_size": 0},
    # The Springs
    "the-springs|3|villa":             {"avg_psqm": 10238, "sample_size": 5, "date_range": "2013–2024"},
    "the-springs|3|townhouse":         {"avg_psqm": None,  "sample_size": 0},
    "the-springs|4|villa":             {"avg_psqm": None,  "sample_size": 0},
    "the-springs|4|townhouse":         {"avg_psqm": None,  "sample_size": 0},
    # The Meadows (shares cadastral area with Springs in this dataset)
    "the-meadows|3|villa":             {"avg_psqm": 10238, "sample_size": 5, "date_range": "2013–2024"},
    "the-meadows|3|townhouse":         {"avg_psqm": None,  "sample_size": 0},
    "the-meadows|4|villa":             {"avg_psqm": None,  "sample_size": 0},
    "the-meadows|4|townhouse":         {"avg_psqm": None,  "sample_size": 0},
    # The Lakes
    "the-lakes|3|villa":               {"avg_psqm": 9716,  "sample_size": 3, "date_range": "2008–2013"},
    "the-lakes|3|townhouse":           {"avg_psqm": 14381, "sample_size": 5, "date_range": "2013–2023"},
    "the-lakes|4|villa":               {"avg_psqm": None,  "sample_size": 0},
    "the-lakes|4|townhouse":           {"avg_psqm": None,  "sample_size": 0},
    # Madinat Jumeirah Living (not in this dataset — upload full CSV to get data)
    "madinat-jumeirah-living|3|villa":      {"avg_psqm": None, "sample_size": 0},
    "madinat-jumeirah-living|3|townhouse":  {"avg_psqm": None, "sample_size": 0},
    "madinat-jumeirah-living|4|villa":      {"avg_psqm": None, "sample_size": 0},
    "madinat-jumeirah-living|4|townhouse":  {"avg_psqm": None, "sample_size": 0},
}


def enrich_psqm(listing):
    price = listing.get("price", 0)
    sqft  = listing.get("area_sqft", 0)
    sqm   = sqft * 0.0929 if sqft else 0
    psqm  = round(price / sqm) if price and sqm else None
    listing["price_per_sqm"] = psqm

    if listing.get("purpose") == "for-sale" and psqm:
        beds  = listing.get("beds", 0)
        ptype = listing.get("property_type", "villa").lower()
        key   = f"{listing['area_slug']}|{beds}|{ptype}"
        bench = DLD_BENCHMARKS.get(key, {})
        avg   = bench.get("avg_psqm")
        listing["avg_psqm"]        = avg
        listing["dld_sample_size"] = bench.get("sample_size", 0)
        listing["dld_date_range"]  = bench.get("date_range", "")
        listing["vs_avg_pct"]      = round(((psqm - avg) / avg) * 100, 1) if avg else None
    else:
        listing["avg_psqm"] = listing["vs_avg_pct"] = None
        listing["dld_sample_size"] = 0
        listing["dld_date_range"]  = ""

    return listing


# ── Bayut scraping (Playwright) ────────────────────────────────────────────────

def parse_price(text):
    nums = re.findall(r"\d+", text.replace(",", "").replace("\xa0", ""))
    return int("".join(nums[:2])) if nums else 0

def parse_age_days(s):
    s = s.lower().strip()
    if any(x in s for x in ("today", "hour", "minute", "just")): return 0
    if "yesterday" in s: return 1
    m = re.search(r"(\d+)\s*day", s)
    if m: return int(m.group(1))
    m = re.search(r"(\d+)\s*week", s)
    if m: return int(m.group(1)) * 7
    return 999

def extract_from_next_data(html, purpose, area_slug):
    listings = []
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not m:
        return listings
    try:
        data = json.loads(m.group(1))
    except Exception:
        return listings

    props_candidates = []
    try:
        props_candidates.append(
            data["props"]["pageProps"]["searchResult"]["properties"])
    except Exception: pass
    try:
        props_candidates.append(
            data["props"]["pageProps"]["dehydratedState"]["queries"][0]
                ["state"]["data"]["hits"])
    except Exception: pass
    try:
        props_candidates.append(
            data["props"]["pageProps"]["dehydratedState"]["queries"][0]
                ["state"]["data"]["properties"])
    except Exception: pass

    props = next((p for p in props_candidates if p), [])

    for prop in props:
        try:
            added_str = (prop.get("addedOn") or prop.get("createdAt")
                         or prop.get("listingAddedDate") or "")
            age_days = 999
            if added_str:
                try:
                    dt = datetime.fromisoformat(added_str.replace("Z", "+00:00"))
                    age_days = (datetime.now().astimezone() - dt).days
                except Exception:
                    age_days = parse_age_days(str(added_str))
            if age_days > MAX_AGE_DAYS:
                continue

            price_raw = (prop.get("price") or prop.get("rentPrice")
                         or prop.get("salePrice") or 0)
            price = price_raw if isinstance(price_raw, (int, float)) else parse_price(str(price_raw))

            ext_id = prop.get("externalID") or prop.get("id") or ""
            slug   = prop.get("slug") or ""
            if ext_id:
                url = f"https://www.bayut.com/property/details-{ext_id}.html"
            elif slug:
                url = f"https://www.bayut.com/property/details-{slug}.html"
            else:
                url = "https://www.bayut.com"

            image_url = ""
            for img_field in ("coverPhoto", "photos", "mainPhoto"):
                imgs = prop.get(img_field)
                if not imgs: continue
                if isinstance(imgs, dict):
                    image_url = imgs.get("url") or imgs.get("src") or ""
                elif isinstance(imgs, list) and imgs:
                    first = imgs[0]
                    image_url = (first.get("url") or first.get("src") or "") if isinstance(first, dict) else (first if isinstance(first, str) else "")
                if image_url: break

            loc_list = prop.get("location") or []
            if isinstance(loc_list, list) and loc_list:
                parts = [l.get("name", "") for l in loc_list if l.get("name")]
                location_str = ", ".join(parts[-2:]) if parts else AREA_LABELS[area_slug]
            else:
                location_str = AREA_LABELS[area_slug]

            listing = {
                "id":            str(ext_id or slug),
                "title":         prop.get("title") or prop.get("name") or "Villa/Townhouse",
                "price":         int(price),
                "currency":      "AED",
                "purpose":       purpose,
                "beds":          prop.get("beds") or prop.get("bedrooms") or 0,
                "baths":         prop.get("baths") or prop.get("bathrooms") or 0,
                "area_sqft":     prop.get("area") or prop.get("size") or 0,
                "property_type": prop.get("type") or prop.get("propertyType") or "Villa",
                "location":      location_str,
                "area_slug":     area_slug,
                "image_url":     image_url,
                "url":           url,
                "age_days":      age_days,
                "added_str":     str(added_str),
                "scraped_at":    datetime.now().isoformat(),
            }
            listings.append(listing)
        except Exception:
            continue
    return listings


def scrape_with_playwright():
    from playwright.sync_api import sync_playwright

    all_listings = []
    seen_ids     = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            locale="en-US",
        )
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = { runtime: {} };
        """)
        page = context.new_page()

        for purpose in PURPOSES:
            for area in AREAS:
                for beds in BEDROOMS:
                    for ptype in PROPERTY_TYPES:
                        url = (f"https://www.bayut.com/{purpose}/{ptype}/"
                               f"{area}/?beds={beds}&sort=date_desc")
                        print(f"  Fetching: {purpose} | {ptype} | {AREA_LABELS[area]} | {beds}bd")
                        try:
                            page.goto(url, wait_until="domcontentloaded", timeout=30000)
                            page.wait_for_timeout(random.randint(2000, 3500))
                            html = page.content()
                        except Exception as e:
                            print(f"    Page load failed: {e}")
                            continue

                        found = extract_from_next_data(html, purpose, area)
                        print(f"    Found {len(found)} recent listings.")

                        for lst in found:
                            lid = lst["id"]
                            if lid and lid not in seen_ids:
                                seen_ids.add(lid)
                                all_listings.append(lst)

                        time.sleep(random.uniform(2, 4))

        browser.close()
    return all_listings


# ── Dashboard injection ────────────────────────────────────────────────────────

def inject_into_dashboard(data):
    if not DASHBOARD_FILE.exists():
        print("WARNING: dashboard.html not found.")
        return
    html  = DASHBOARD_FILE.read_text(encoding="utf-8")
    block = f"const EMBEDDED_DATA = {json.dumps(data, ensure_ascii=False)};"
    html  = re.sub(r"const EMBEDDED_DATA = \{.*?\};", block, html, flags=re.DOTALL)
    DASHBOARD_FILE.write_text(html, encoding="utf-8")
    print(f"Dashboard updated: {DASHBOARD_FILE}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("Dubai Property Scraper")
    print("=" * 45)
    print(f"Areas : {', '.join(AREA_LABELS.values())}")
    print(f"Beds  : {BEDROOMS}")
    print(f"DLD   : Benchmarks loaded from CSV ({sum(1 for v in DLD_BENCHMARKS.values() if v['avg_psqm'])} combos with data)")
    print("=" * 45)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("\nERROR: Playwright not installed.")
        print("Run these two commands first, then try again:")
        print("  pip3 install playwright")
        print("  python3 -m playwright install chromium\n")
        return

    listings = scrape_with_playwright()

    print(f"\nEnriching {len(listings)} listings with price/sqm benchmarks...")
    for lst in listings:
        enrich_psqm(lst)

    listings.sort(key=lambda x: x.get("age_days", 999))

    output = {
        "last_updated":   datetime.now().isoformat(),
        "total":          len(listings),
        "dld_configured": True,
        "listings":       listings,
    }

    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    print(f"Done! {len(listings)} listings saved.")
    inject_into_dashboard(output)


if __name__ == "__main__":
    main()
