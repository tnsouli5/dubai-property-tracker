#!/usr/bin/env python3
"""
Dubai Property Scraper — RapidAPI / Bayut edition
Uses the Bayut API on RapidAPI (bayut14.p.rapidapi.com).
Requires RAPIDAPI_KEY environment variable (set as GitHub secret).
"""

import json, time, random, re, os, urllib.request, urllib.parse
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

# Bayut location IDs (used by the apidojo API)
AREA_IDS = {
    "dubai-hills-estate":      "5002",
    "the-springs":             "660",
    "the-meadows":             "661",
    "the-lakes":               "662",
    "madinat-jumeirah-living": "53536",
}

BEDROOMS       = [3, 4]
PURPOSES       = ["for-sale", "for-rent"]
# apidojo uses categoryExternalID: 3=villa, 38=townhouse
CATEGORIES     = {"villa": "3", "townhouse": "38"}

MAX_AGE_DAYS   = 7
RAPIDAPI_KEY   = os.environ.get("RAPIDAPI_KEY", "")
RAPIDAPI_HOST  = "bayut14.p.rapidapi.com"

OUTPUT_FILE    = Path(__file__).parent / "listings.json"
DASHBOARD_FILE = Path(__file__).parent / "dashboard.html"

# ── DLD Benchmarks ─────────────────────────────────────────────────────────────

DLD_BENCHMARKS = {
    "dubai-hills-estate|3|villa":      {"avg_psqm": 11825, "sample_size": 5, "date_range": "2018–2021"},
    "dubai-hills-estate|3|townhouse":  {"avg_psqm": 20077, "sample_size": 5, "date_range": "2014–2024"},
    "dubai-hills-estate|4|villa":      {"avg_psqm": 13930, "sample_size": 5, "date_range": "2016–2025"},
    "dubai-hills-estate|4|townhouse":  {"avg_psqm": None,  "sample_size": 0},
    "the-springs|3|villa":             {"avg_psqm": 10238, "sample_size": 5, "date_range": "2013–2024"},
    "the-springs|3|townhouse":         {"avg_psqm": None,  "sample_size": 0},
    "the-springs|4|villa":             {"avg_psqm": None,  "sample_size": 0},
    "the-springs|4|townhouse":         {"avg_psqm": None,  "sample_size": 0},
    "the-meadows|3|villa":             {"avg_psqm": 10238, "sample_size": 5, "date_range": "2013–2024"},
    "the-meadows|3|townhouse":         {"avg_psqm": None,  "sample_size": 0},
    "the-meadows|4|villa":             {"avg_psqm": None,  "sample_size": 0},
    "the-meadows|4|townhouse":         {"avg_psqm": None,  "sample_size": 0},
    "the-lakes|3|villa":               {"avg_psqm": 9716,  "sample_size": 3, "date_range": "2008–2013"},
    "the-lakes|3|townhouse":           {"avg_psqm": 14381, "sample_size": 5, "date_range": "2013–2023"},
    "the-lakes|4|villa":               {"avg_psqm": None,  "sample_size": 0},
    "the-lakes|4|townhouse":           {"avg_psqm": None,  "sample_size": 0},
    "madinat-jumeirah-living|3|villa":     {"avg_psqm": None, "sample_size": 0},
    "madinat-jumeirah-living|3|townhouse": {"avg_psqm": None, "sample_size": 0},
    "madinat-jumeirah-living|4|villa":     {"avg_psqm": None, "sample_size": 0},
    "madinat-jumeirah-living|4|townhouse": {"avg_psqm": None, "sample_size": 0},
}

# ── API call ───────────────────────────────────────────────────────────────────

def search_properties(purpose, area_slug, beds, category_slug):
    """Call the Bayut RapidAPI and return raw hits list."""
    params = urllib.parse.urlencode({
        "locationExternalIDs": AREA_IDS[area_slug],
        "purpose":             purpose,
        "categoryExternalID":  CATEGORIES[category_slug],
        "bedrooms":            beds,
        "hitsPerPage":         25,
        "page":                0,
        "sort":                "date_desc",
        "lang":                "en",
    })
    url = f"https://{RAPIDAPI_HOST}/properties/list?{params}"
    headers = {
        "x-rapidapi-key":  RAPIDAPI_KEY,
        "x-rapidapi-host": RAPIDAPI_HOST,
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())

        # API returns hits nested under various keys — try all
        hits = (data.get("hits") or
                data.get("properties") or
                data.get("results") or [])
        if isinstance(hits, dict):
            hits = hits.get("hits") or hits.get("properties") or []

        # Some versions wrap in a top-level object
        if not hits and isinstance(data, dict):
            for v in data.values():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    if any(k in v[0] for k in ["externalID", "price", "beds", "title"]):
                        hits = v
                        break

        return hits

    except urllib.error.HTTPError as e:
        body = ""
        try: body = e.read().decode()[:300]
        except: pass
        print(f"    HTTP {e.code}: {e.reason} — {body}")
        return []
    except Exception as e:
        print(f"    Error: {e}")
        return []

# ── Parsing ────────────────────────────────────────────────────────────────────

def parse_age_days(added_str):
    if not added_str:
        return 999
    try:
        dt = datetime.fromisoformat(str(added_str).replace("Z", "+00:00"))
        return (datetime.now().astimezone() - dt).days
    except Exception:
        pass
    s = str(added_str).lower().strip()
    if any(x in s for x in ("today", "hour", "minute", "just")): return 0
    if "yesterday" in s: return 1
    m = re.search(r"(\d+)\s*day", s)
    if m: return int(m.group(1))
    m = re.search(r"(\d+)\s*week", s)
    if m: return int(m.group(1)) * 7
    return 999

def parse_hit(hit, purpose, area_slug, category_slug):
    try:
        added_str = (hit.get("addedOn") or hit.get("createdAt")
                     or hit.get("listingAddedDate") or "")
        age_days = parse_age_days(added_str)
        if age_days > MAX_AGE_DAYS:
            return None

        # Price
        price = 0
        for f in ("price", "rentPrice", "salePrice", "monthlyRentPrice"):
            v = hit.get(f)
            if v:
                try: price = int(float(str(v).replace(",", "")))
                except: pass
                if price: break

        # URL
        ext_id = str(hit.get("externalID") or hit.get("id") or "")
        slug   = hit.get("slug") or ""
        url    = (f"https://www.bayut.com/property/details-{ext_id}.html"
                  if ext_id else f"https://www.bayut.com/{slug}/" if slug
                  else "https://www.bayut.com")

        # Image
        image_url = ""
        for field in ("coverPhoto", "photos", "mainPhoto", "heroImage"):
            img = hit.get(field)
            if not img: continue
            if isinstance(img, dict):
                image_url = img.get("url") or img.get("src") or ""
            elif isinstance(img, list) and img:
                f0 = img[0]
                image_url = (f0.get("url") or f0.get("src") or "") if isinstance(f0, dict) else (f0 if isinstance(f0, str) else "")
            if image_url: break

        # Location
        loc = hit.get("location") or []
        if isinstance(loc, list) and loc:
            parts = [l.get("name","") for l in loc if isinstance(l,dict) and l.get("name")]
            location_str = ", ".join(parts[-2:]) if parts else AREA_LABELS[area_slug]
        else:
            location_str = AREA_LABELS[area_slug]

        beds  = hit.get("beds") or hit.get("bedrooms") or 0
        baths = hit.get("baths") or hit.get("bathrooms") or 0
        sqft  = hit.get("area") or hit.get("size") or hit.get("areaSqft") or 0
        ptype = hit.get("type") or hit.get("propertyType") or category_slug.title()

        return {
            "id":            ext_id or slug,
            "title":         hit.get("title") or hit.get("name") or f"{beds}BR {ptype}",
            "price":         price,
            "currency":      "AED",
            "purpose":       purpose,
            "beds":          beds,
            "baths":         baths,
            "area_sqft":     sqft,
            "property_type": ptype,
            "location":      location_str,
            "area_slug":     area_slug,
            "image_url":     image_url,
            "url":           url,
            "age_days":      age_days,
            "added_str":     str(added_str),
            "scraped_at":    datetime.now().isoformat(),
        }
    except Exception as e:
        print(f"    Parse error: {e}")
        return None

# ── Price/sqm enrichment ───────────────────────────────────────────────────────

def enrich_psqm(listing):
    price = listing.get("price", 0)
    sqft  = listing.get("area_sqft", 0)
    sqm   = sqft * 0.0929 if sqft else 0
    psqm  = round(price / sqm) if price and sqm else None
    listing["price_per_sqm"] = psqm

    if listing.get("purpose") == "for-sale" and psqm:
        beds  = listing.get("beds", 0)
        ptype = str(listing.get("property_type", "villa")).lower()
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
    print(f"Dashboard updated.")

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    if not RAPIDAPI_KEY:
        print("ERROR: RAPIDAPI_KEY environment variable not set.")
        print("Add it as a GitHub secret named RAPIDAPI_KEY.")
        return

    print("Dubai Property Scraper")
    print("=" * 45)
    print(f"Areas : {', '.join(AREA_LABELS.values())}")
    print(f"Beds  : {BEDROOMS}")
    print(f"Key   : {RAPIDAPI_KEY[:8]}...{RAPIDAPI_KEY[-4:]}")
    print("=" * 45)

    all_listings = []
    seen_ids     = set()

    for purpose in PURPOSES:
        for area_slug in AREAS:
            for beds in BEDROOMS:
                for cat_slug in CATEGORIES:
                    label = f"{purpose} | {cat_slug} | {AREA_LABELS[area_slug]} | {beds}bd"
                    print(f"  Fetching: {label}")

                    hits = search_properties(purpose, area_slug, beds, cat_slug)
                    print(f"    Got {len(hits)} hits from API")

                    found = 0
                    for hit in hits:
                        listing = parse_hit(hit, purpose, area_slug, cat_slug)
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
