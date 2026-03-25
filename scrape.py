#!/usr/bin/env python3
"""
Dubai Property Scraper — Bayut API edition
Uses Bayut's internal API (same one their mobile app uses).
Works from any IP including GitHub Actions cloud servers.
"""

import json, time, random, re, urllib.request, urllib.parse
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

# Bayut internal location IDs for each area
# These are stable IDs used by Bayut's API
AREA_IDS = {
    "dubai-hills-estate":      "5002",
    "the-springs":             "660",
    "the-meadows":             "661",
    "the-lakes":               "662",
    "madinat-jumeirah-living": "53536",
}

BEDROOMS       = [3, 4]
PROPERTY_TYPES = {"villa": "3", "townhouse": "38"}  # Bayut type IDs
PURPOSES       = {"for-sale": "sale", "for-rent": "rent"}
MAX_AGE_DAYS   = 7

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

# ── Bayut API ──────────────────────────────────────────────────────────────────

API_BASE = "https://www.bayut.com/api/v1"

API_HEADERS = {
    "User-Agent":      "Bayut/20.0.0 (iPhone; iOS 16.0; Scale/3.00)",
    "Accept":          "application/json",
    "Accept-Language": "en",
    "X-Forwarded-For": "5.195.0.1",  # UAE IP hint
}

def bayut_search(purpose_key, area_slug, beds, ptype_slug):
    """
    Call Bayut's listing search API.
    Returns list of raw property dicts.
    """
    purpose  = PURPOSES[purpose_key]
    area_id  = AREA_IDS[area_slug]
    type_id  = PROPERTY_TYPES[ptype_slug]

    params = {
        "purpose":       purpose,
        "categoryExternalID": type_id,
        "locationExternalIDs": area_id,
        "bedrooms":      beds,
        "sort":          "date_desc",
        "hitsPerPage":   25,
        "page":          0,
        "lang":          "en",
    }

    url = f"{API_BASE}/listings/search?" + urllib.parse.urlencode(params)

    try:
        req = urllib.request.Request(url, headers=API_HEADERS)
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())

        # API returns hits under various keys
        hits = (data.get("hits") or
                data.get("properties") or
                data.get("results") or
                data.get("data") or [])

        if isinstance(hits, dict):
            hits = hits.get("hits") or hits.get("properties") or []

        return hits

    except Exception as e:
        print(f"    API error: {e}")
        return []


def parse_age_days(added_str):
    if not added_str:
        return 999
    # Try ISO date string
    try:
        dt = datetime.fromisoformat(str(added_str).replace("Z", "+00:00"))
        return (datetime.now().astimezone() - dt).days
    except Exception:
        pass
    # Try relative string
    s = str(added_str).lower().strip()
    if any(x in s for x in ("today", "hour", "minute", "just")): return 0
    if "yesterday" in s: return 1
    m = re.search(r"(\d+)\s*day", s)
    if m: return int(m.group(1))
    m = re.search(r"(\d+)\s*week", s)
    if m: return int(m.group(1)) * 7
    return 999


def parse_hit(hit, purpose_key, area_slug, ptype_slug):
    """Convert a raw API hit into our standard listing dict."""
    try:
        added_str = (hit.get("addedOn") or hit.get("createdAt")
                     or hit.get("listingAddedDate") or "")
        age_days = parse_age_days(added_str)
        if age_days > MAX_AGE_DAYS:
            return None

        # Price
        price = 0
        for pf in ("price", "rentPrice", "salePrice", "monthlyRentPrice"):
            v = hit.get(pf)
            if v:
                price = int(float(str(v).replace(",", "")))
                break

        # URL
        ext_id = hit.get("externalID") or hit.get("id") or ""
        slug   = hit.get("slug") or ""
        if ext_id:
            url = f"https://www.bayut.com/property/details-{ext_id}.html"
        elif slug:
            url = f"https://www.bayut.com/{slug}/"
        else:
            url = "https://www.bayut.com"

        # Image
        image_url = ""
        for img_field in ("coverPhoto", "photos", "mainPhoto", "heroImage"):
            imgs = hit.get(img_field)
            if not imgs: continue
            if isinstance(imgs, dict):
                image_url = imgs.get("url") or imgs.get("src") or ""
            elif isinstance(imgs, list) and imgs:
                first = imgs[0]
                image_url = (first.get("url") or first.get("src") or "") if isinstance(first, dict) else (first if isinstance(first, str) else "")
            if image_url: break

        # Location
        loc_list = hit.get("location") or []
        if isinstance(loc_list, list) and loc_list:
            parts = [l.get("name", "") for l in loc_list if isinstance(l, dict) and l.get("name")]
            location_str = ", ".join(parts[-2:]) if parts else AREA_LABELS[area_slug]
        else:
            location_str = AREA_LABELS[area_slug]

        # Beds / baths / size
        beds  = hit.get("beds") or hit.get("bedrooms") or 0
        baths = hit.get("baths") or hit.get("bathrooms") or 0
        sqft  = hit.get("area") or hit.get("size") or hit.get("areaSqft") or 0

        ptype = hit.get("type") or hit.get("propertyType") or ptype_slug.title()

        return {
            "id":            str(ext_id or slug),
            "title":         hit.get("title") or hit.get("name") or f"{beds}BR {ptype.title()}",
            "price":         price,
            "currency":      "AED",
            "purpose":       purpose_key,
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
        listing["vs_avg_pct"]      = round(((psqm - avg) / avg) * 100, 1) if avg else None
    else:
        listing["avg_psqm"]        = None
        listing["vs_avg_pct"]      = None
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
    print(f"Dashboard updated: {DASHBOARD_FILE}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("Dubai Property Scraper (API mode)")
    print("=" * 45)
    print(f"Areas : {', '.join(AREA_LABELS.values())}")
    print(f"Beds  : {BEDROOMS}")
    print("=" * 45)

    all_listings = []
    seen_ids     = set()

    for purpose_key in PURPOSES:
        for area_slug in AREAS:
            for beds in BEDROOMS:
                for ptype_slug in PROPERTY_TYPES:
                    label = f"{purpose_key} | {ptype_slug} | {AREA_LABELS[area_slug]} | {beds}bd"
                    print(f"  Fetching: {label}")

                    hits = bayut_search(purpose_key, area_slug, beds, ptype_slug)
                    print(f"    API returned {len(hits)} hits")

                    found = 0
                    for hit in hits:
                        listing = parse_hit(hit, purpose_key, area_slug, ptype_slug)
                        if listing:
                            lid = listing["id"]
                            if lid and lid not in seen_ids:
                                seen_ids.add(lid)
                                all_listings.append(listing)
                                found += 1

                    print(f"    {found} within last {MAX_AGE_DAYS} days")
                    time.sleep(random.uniform(0.5, 1.5))

    print(f"\nEnriching {len(all_listings)} listings with price/sqm...")
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
