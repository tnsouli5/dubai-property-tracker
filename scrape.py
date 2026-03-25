#!/usr/bin/env python3
"""
Dubai Property Scraper — bayut14.p.rapidapi.com
GET /search-property with query params.
Requires RAPIDAPI_KEY environment variable (set as GitHub secret).
"""

import json, time, random, re, os, urllib.request, urllib.parse
from datetime import datetime, timezone
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

# Bayut location external IDs
AREA_IDS = {
    "dubai-hills-estate":      "5002",
    "the-springs":             "660",
    "the-meadows":             "661",
    "the-lakes":               "662",
    "madinat-jumeirah-living": "53536",
}

BEDROOMS       = [3, 4]
PURPOSES       = ["for-sale", "for-rent"]
PROPERTY_TYPES = ["villas", "townhouses"]
MAX_AGE_DAYS   = 7

RAPIDAPI_KEY   = os.environ.get("RAPIDAPI_KEY", "")
RAPIDAPI_HOST  = "bayut14.p.rapidapi.com"

OUTPUT_FILE    = Path(__file__).parent / "listings.json"
DASHBOARD_FILE = Path(__file__).parent / "dashboard.html"

# ── DLD Benchmarks ─────────────────────────────────────────────────────────────

DLD_BENCHMARKS = {
    "dubai-hills-estate|3|villas":     {"avg_psqm": 11825, "sample_size": 5, "date_range": "2018–2021"},
    "dubai-hills-estate|3|townhouses": {"avg_psqm": 20077, "sample_size": 5, "date_range": "2014–2024"},
    "dubai-hills-estate|4|villas":     {"avg_psqm": 13930, "sample_size": 5, "date_range": "2016–2025"},
    "dubai-hills-estate|4|townhouses": {"avg_psqm": None,  "sample_size": 0},
    "the-springs|3|villas":            {"avg_psqm": 10238, "sample_size": 5, "date_range": "2013–2024"},
    "the-springs|3|townhouses":        {"avg_psqm": None,  "sample_size": 0},
    "the-springs|4|villas":            {"avg_psqm": None,  "sample_size": 0},
    "the-springs|4|townhouses":        {"avg_psqm": None,  "sample_size": 0},
    "the-meadows|3|villas":            {"avg_psqm": 10238, "sample_size": 5, "date_range": "2013–2024"},
    "the-meadows|3|townhouses":        {"avg_psqm": None,  "sample_size": 0},
    "the-meadows|4|villas":            {"avg_psqm": None,  "sample_size": 0},
    "the-meadows|4|townhouses":        {"avg_psqm": None,  "sample_size": 0},
    "the-lakes|3|villas":              {"avg_psqm": 9716,  "sample_size": 3, "date_range": "2008–2013"},
    "the-lakes|3|townhouses":          {"avg_psqm": 14381, "sample_size": 5, "date_range": "2013–2023"},
    "the-lakes|4|villas":              {"avg_psqm": None,  "sample_size": 0},
    "the-lakes|4|townhouses":          {"avg_psqm": None,  "sample_size": 0},
    "madinat-jumeirah-living|3|villas":     {"avg_psqm": None, "sample_size": 0},
    "madinat-jumeirah-living|3|townhouses": {"avg_psqm": None, "sample_size": 0},
    "madinat-jumeirah-living|4|villas":     {"avg_psqm": None, "sample_size": 0},
    "madinat-jumeirah-living|4|townhouses": {"avg_psqm": None, "sample_size": 0},
}

# ── API ────────────────────────────────────────────────────────────────────────

def search_properties(purpose, area_slug, beds, prop_type):
    """GET /search-property — returns list of property dicts."""
    params = urllib.parse.urlencode({
        "purpose":       purpose,
        "location_ids":  AREA_IDS[area_slug],
        "property_type": prop_type,
        "rooms":         str(beds),
        "sort_order":    "latest",
        "page":          1,
        "langs":         "en",
    })
    url = f"https://{RAPIDAPI_HOST}/search-property?{params}"
    headers = {
        "x-rapidapi-key":  RAPIDAPI_KEY,
        "x-rapidapi-host": RAPIDAPI_HOST,
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        if data.get("success") and "data" in data:
            return data["data"].get("properties") or []
        return data.get("properties") or data.get("hits") or []
    except urllib.error.HTTPError as e:
        txt = ""
        try: txt = e.read().decode()[:200]
        except: pass
        print(f"    HTTP {e.code}: {txt}")
        return []
    except Exception as e:
        print(f"    Error: {e}")
        return []

# ── Parsing ────────────────────────────────────────────────────────────────────

def parse_age_days(val):
    if not val and val != 0: return 999
    if isinstance(val, (int, float)) and val > 1_000_000_000:
        dt = datetime.fromtimestamp(val, tz=timezone.utc)
        return (datetime.now(tz=timezone.utc) - dt).days
    try:
        dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        return (datetime.now().astimezone() - dt).days
    except: pass
    s = str(val).lower()
    if any(x in s for x in ("today","hour","minute","just")): return 0
    if "yesterday" in s: return 1
    m = re.search(r"(\d+)\s*day", s)
    if m: return int(m.group(1))
    m = re.search(r"(\d+)\s*week", s)
    if m: return int(m.group(1)) * 7
    return 999

def parse_hit(hit, purpose, area_slug, prop_type):
    try:
        created_at = hit.get("createdAt")
        age_days   = parse_age_days(created_at)
        if age_days > MAX_AGE_DAYS:
            return None

        # Price
        price = int(float(hit.get("price") or 0))

        # URL
        ext_id = str(hit.get("externalID") or hit.get("id") or "")
        slug   = hit.get("slug") or {}
        slug_en = slug.get("en","") if isinstance(slug, dict) else str(slug or "")
        url = (f"https://www.bayut.com/property/details-{ext_id}.html"
               if ext_id else f"https://www.bayut.com/{slug_en}/"
               if slug_en else "https://www.bayut.com")

        # Image
        image_url = ""
        cover = hit.get("coverPhoto")
        if isinstance(cover, dict):
            image_url = cover.get("url") or cover.get("src") or ""
        elif isinstance(cover, str):
            image_url = cover

        # Location
        loc = hit.get("location") or []
        if isinstance(loc, list) and loc:
            parts = [l.get("name","") for l in loc if isinstance(l,dict) and l.get("name")]
            location_str = ", ".join(parts[-2:]) if parts else AREA_LABELS[area_slug]
        else:
            location_str = AREA_LABELS[area_slug]

        # Title (multilingual object {"en": "...", "ar": "..."})
        title_raw = hit.get("title") or {}
        if isinstance(title_raw, dict):
            title = title_raw.get("en") or title_raw.get("ar") or ""
        else:
            title = str(title_raw)

        # Category
        cat_raw = hit.get("category") or []
        if isinstance(cat_raw, list) and cat_raw:
            ptype = cat_raw[0].get("name","") if isinstance(cat_raw[0],dict) else prop_type
        else:
            ptype = prop_type

        beds  = hit.get("rooms") or 0
        baths = hit.get("baths") or 0
        area  = hit.get("area") or 0  # sqm

        if isinstance(created_at, (int, float)) and created_at > 0:
            added_str = datetime.fromtimestamp(created_at, tz=timezone.utc).strftime("%Y-%m-%d")
        else:
            added_str = str(created_at or "")

        return {
            "id":            ext_id or slug_en,
            "title":         title or f"{beds}BR {prop_type.rstrip('s').title()}",
            "price":         price,
            "currency":      "AED",
            "purpose":       purpose,
            "beds":          beds,
            "baths":         baths,
            "area_sqft":     area,   # sqm despite field name
            "property_type": ptype or prop_type,
            "location":      location_str,
            "area_slug":     area_slug,
            "image_url":     image_url,
            "url":           url,
            "age_days":      age_days,
            "added_str":     added_str,
            "scraped_at":    datetime.now().isoformat(),
        }
    except Exception as e:
        print(f"    Parse error: {e}")
        return None

# ── Price/sqm enrichment ───────────────────────────────────────────────────────

def enrich_psqm(listing):
    price = listing.get("price", 0)
    sqm   = listing.get("area_sqft", 0)  # already sqm
    psqm  = round(price / sqm) if price and sqm else None
    listing["price_per_sqm"] = psqm

    if listing.get("purpose") == "for-sale" and psqm:
        beds  = listing.get("beds", 0)
        ptype = listing.get("property_type", "").lower()
        # normalise to villas/townhouses
        if "villa" in ptype: ptype = "villas"
        elif "town" in ptype: ptype = "townhouses"
        key   = f"{listing['area_slug']}|{beds}|{ptype}"
        bench = DLD_BENCHMARKS.get(key, {})
        avg   = bench.get("avg_psqm")
        listing["avg_psqm"]        = avg
        listing["dld_sample_size"] = bench.get("sample_size", 0)
        listing["dld_date_range"]  = bench.get("date_range", "")
        listing["vs_avg_pct"]      = round(((psqm-avg)/avg)*100,1) if avg else None
    else:
        listing["avg_psqm"] = listing["vs_avg_pct"] = None
        listing["dld_sample_size"] = 0
        listing["dld_date_range"]  = ""
    return listing

# ── Dashboard injection ────────────────────────────────────────────────────────

def inject_into_dashboard(data):
    if not DASHBOARD_FILE.exists():
        print("WARNING: dashboard.html not found.")
        return
    html  = DASHBOARD_FILE.read_text(encoding="utf-8")
    block = f"const EMBEDDED_DATA = {json.dumps(data, ensure_ascii=False)};"
    html  = re.sub(r"const EMBEDDED_DATA = \{.*?\};", block, html, flags=re.DOTALL)
    DASHBOARD_FILE.write_text(html, encoding="utf-8")
    print("Dashboard updated.")

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    if not RAPIDAPI_KEY:
        print("ERROR: RAPIDAPI_KEY not set. Add it as a GitHub secret.")
        return

    print("Dubai Property Scraper")
    print("=" * 45)
    print(f"Areas : {', '.join(AREA_LABELS.values())}")
    print(f"Beds  : {BEDROOMS}")
    print(f"Host  : {RAPIDAPI_HOST}")
    print(f"Key   : {RAPIDAPI_KEY[:8]}...{RAPIDAPI_KEY[-4:]}")
    print("=" * 45)

    all_listings = []
    seen_ids     = set()

    for purpose in PURPOSES:
        for area_slug in AREAS:
            for beds in BEDROOMS:
                for prop_type in PROPERTY_TYPES:
                    label = f"{purpose} | {prop_type} | {AREA_LABELS[area_slug]} | {beds}bd"
                    print(f"  Fetching: {label}")
                    hits = search_properties(purpose, area_slug, beds, prop_type)
                    print(f"    Got {len(hits)} hits")
                    found = 0
                    for hit in hits:
                        listing = parse_hit(hit, purpose, area_slug, prop_type)
                        if listing:
                            lid = listing["id"]
                            if lid and lid not in seen_ids:
                                seen_ids.add(lid)
                                all_listings.append(listing)
                                found += 1
                    print(f"    {found} within last {MAX_AGE_DAYS} days")
                    time.sleep(random.uniform(0.3, 0.8))

    print(f"\nEnriching {len(all_listings)} listings...")
    for lst in all_listings:
        enrich_psqm(lst)

    all_listings.sort(key=lambda x: x.get("age_days", 999))

    output = {
        "last_updated":   datetime.now().isoformat(),
        "total":          len(all_listings),
        "dld_configured": True,
        "listings":       all_listings,
    }

    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    print(f"Done! {len(all_listings)} listings saved.")
    inject_into_dashboard(output)

if __name__ == "__main__":
    main()
