from __future__ import annotations

import csv
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = Path(os.getenv("PG_DB_PATH", str(DATA / "pgs.sqlite")))
CSV_PATH = Path(os.getenv("PG_CSV_PATH", str(DATA / "pgs.csv")))
CITIES_PATH = ROOT / "cities.json"

USER_AGENT = os.getenv(
    "PG_USER_AGENT",
    "IndiaPGDiscovery/1.2 (+https://github.com/K-hitesh/india-pg-discovery)",
)
OVERPASS_ENDPOINTS = [
    item.strip()
    for item in os.getenv(
        "OVERPASS_ENDPOINTS",
        "https://overpass.nchc.org.tw/api/interpreter,"
        "https://overpass-api.de/api/interpreter,"
        "https://overpass.kumi.systems/api/interpreter,"
        "https://overpass.private.coffee/api/interpreter",
    ).split(",")
    if item.strip()
]
REQUEST_TIMEOUT = int(os.getenv("OVERPASS_TIMEOUT_SECONDS", "20"))
MAX_ATTEMPTS_PER_ENDPOINT = int(os.getenv("OVERPASS_RETRIES", "1"))
CITY_DELAY_SECONDS = float(os.getenv("CITY_DELAY_SECONDS", "3"))

SESSION = requests.Session()
SESSION.headers.update(
    {"User-Agent": USER_AGENT, "Accept": "application/json"}
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS pgs (
  id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  source_id TEXT NOT NULL,
  name TEXT,
  address TEXT,
  locality TEXT,
  city TEXT,
  state TEXT,
  postcode TEXT,
  latitude REAL,
  longitude REAL,
  phone TEXT,
  email TEXT,
  website TEXT,
  monthly_rent TEXT,
  room_type TEXT,
  amenities TEXT,
  photo_urls TEXT,
  nearby_locations TEXT,
  source_url TEXT,
  first_seen TEXT NOT NULL,
  last_seen TEXT NOT NULL,
  raw_tags TEXT,
  UNIQUE(source, source_id)
);
CREATE TABLE IF NOT EXISTS scan_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  city TEXT NOT NULL,
  started_at TEXT NOT NULL,
  status TEXT NOT NULL,
  discovered INTEGER DEFAULT 0,
  message TEXT
);
CREATE INDEX IF NOT EXISTS idx_pgs_city ON pgs(city);
CREATE INDEX IF NOT EXISTS idx_pgs_name ON pgs(name);
"""

COLUMNS = [
    "id", "source", "source_id", "name", "address", "locality", "city",
    "state", "postcode", "latitude", "longitude", "phone", "email",
    "website", "monthly_rent", "room_type", "amenities", "photo_urls",
    "nearby_locations", "source_url", "first_seen", "last_seen",
]


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def clean(value: Any) -> str | None:
    if value is None:
        return None
    value = re.sub(r"\s+", " ", str(value)).strip()
    return value or None


def connect() -> sqlite3.Connection:
    DATA.mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(SCHEMA)
    return con


def choose(tags: dict, *keys: str) -> str | None:
    for key in keys:
        value = clean(tags.get(key))
        if value:
            return value
    return None


def looks_like_pg(tags: dict) -> bool:
    name = (tags.get("name") or "").lower()
    return (
        tags.get("amenity") == "hostel"
        or tags.get("tourism") == "hostel"
        or tags.get("building") == "hostel"
        or bool(
            re.search(
                r"\b(pg|paying guest|paying-guest|co.?living|hostel|"
                r"ladies hostel|mens hostel|women hostel|gents hostel)\b",
                name,
            )
        )
    )


def address_from_tags(tags: dict) -> str | None:
    keys = (
        "addr:housenumber", "addr:street", "addr:suburb",
        "addr:neighbourhood", "addr:city", "addr:district",
        "addr:state", "addr:postcode",
    )
    parts = [clean(tags.get(key)) for key in keys]
    parts = [part for part in parts if part]
    return ", ".join(dict.fromkeys(parts)) or None


def safe_alias_regex(city: dict) -> str:
    aliases = city.get("aliases") or [city["name"]]
    escaped = [re.escape(str(alias)) for alias in aliases if clean(alias)]
    return "|".join(escaped) or re.escape(city["name"])


# Bounded pilot areas for the eight requested metro markets.
# Values are south, west, north, east. They are approximate collection windows,
# not official municipal boundaries; records are attributed to the configured city.
PILOT_BBOXES = {
    "Bengaluru": (12.80, 77.35, 13.15, 77.80),
    "Hyderabad": (17.20, 78.25, 17.60, 78.70),
    "Mumbai": (18.85, 72.75, 19.35, 73.05),
    "Pune": (18.35, 73.65, 18.70, 74.00),
    "Chennai": (12.80, 80.05, 13.25, 80.35),
    "Noida": (28.40, 77.30, 28.65, 77.55),
    "Delhi": (28.40, 76.85, 28.90, 77.35),
    "Gurugram": (28.25, 76.85, 28.55, 77.20),
}


def build_query(city: dict, include_pois: bool = False) -> str:
    """Use a bounded bbox query rather than expensive administrative-area lookup."""
    bbox = PILOT_BBOXES.get(city["name"])
    if bbox is None:
        raise ValueError(f"No pilot bounding box configured for {city['name']}")
    bounds = ",".join(str(v) for v in bbox)
    if include_pois:
        return (
            "[out:json][timeout:20];"
            f'(nwr["amenity"~"^(hospital|clinic|college|university|marketplace|bus_station)$"]({bounds});'
            f'nwr["railway"~"^(station|halt)$"]({bounds}););out center tags;'
        )
    # Cheap exact-tag query first; broad name regexes are expensive on metro-sized boxes.
    return (
        "[out:json][timeout:15];"
        f'(nwr["amenity"="hostel"]({bounds});'
        f'nwr["tourism"="hostel"]({bounds});'
        f'nwr["building"="hostel"]({bounds}););'
        "out center tags;"
    )


def build_name_fallback_query(city: dict) -> str:
    """Run only if the exact-tag query succeeds but finds no records."""
    south, west, north, east = PILOT_BBOXES[city["name"]]
    mid_lat, mid_lon = (south + north) / 2, (west + east) / 2
    boxes = [
        (south, west, mid_lat, mid_lon),
        (south, mid_lon, mid_lat, east),
        (mid_lat, west, north, mid_lon),
        (mid_lat, mid_lon, north, east),
    ]
    clauses = "".join(
        f'nwr["name"~"PG|paying guest|hostel|co.?living",i]({",".join(str(v) for v in box)});'
        for box in boxes
    )
    return f"[out:json][timeout:15];({clauses});out center tags;"


def overpass_request(query: str, label: str) -> dict:
    """Try configured public Overpass instances with bounded retries."""
    errors: list[str] = []
    for endpoint in OVERPASS_ENDPOINTS:
        for attempt in range(1, MAX_ATTEMPTS_PER_ENDPOINT + 1):
            try:
                response = SESSION.post(
                    endpoint,
                    data={"data": query},
                    timeout=(15, REQUEST_TIMEOUT),
                )
                if response.status_code in (429, 500, 502, 503, 504):
                    errors.append(
                        f"{endpoint} HTTP {response.status_code} "
                        f"(attempt {attempt}/{MAX_ATTEMPTS_PER_ENDPOINT})"
                    )
                    # Respect Retry-After when supplied; cap the wait.
                    retry_after = response.headers.get("Retry-After")
                    try:
                        wait = min(20, max(2, int(retry_after))) if retry_after else 2 * attempt
                    except ValueError:
                        wait = 2 * attempt
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                payload = response.json()
                elements = payload.get("elements")
                if not isinstance(elements, list):
                    raise ValueError("Overpass response did not contain an elements list")
                return payload
            except (requests.RequestException, ValueError) as exc:
                errors.append(f"{endpoint} {type(exc).__name__}: {str(exc)[:180]}")
                time.sleep(min(5, attempt * 2))
    raise RuntimeError(f"{label}: all Overpass endpoints failed. " + " | ".join(errors[-8:]))


def get_city_data(city: dict) -> tuple[list[dict], str]:
    """Return PG-like elements and diagnostic information for one city."""
    query = build_query(city, include_pois=False)
    payload = overpass_request(query, f"{city['name']} tagged-hostel query")
    elements = payload.get("elements", [])
    fallback_note = ""
    if not elements:
        fallback_payload = overpass_request(
            build_name_fallback_query(city),
            f"{city['name']} tiled name-search fallback",
        )
        elements = fallback_payload.get("elements", [])
        payload = fallback_payload
        fallback_note = "; used tiled name-search fallback"
    area_count = sum(1 for element in elements if element.get("type") == "area")
    note = (
        f"pg_query_elements={len(elements)}; "
        f"area_objects_returned={area_count}; "
        f"generator={payload.get('generator', 'unknown')}{fallback_note}"
    )
    return elements, note


def get_city_pois(city: dict) -> tuple[list[dict], str]:
    """Optional separate query for nearby places; failure does not discard PGs."""
    query = build_query(city, include_pois=True)
    payload = overpass_request(query, f"{city['name']} nearby-places query")
    return payload.get("elements", []), f"poi_query_elements={len(payload.get('elements', []))}"


def normalize_osm(el: dict, city_name: str) -> dict | None:
    tags = el.get("tags") or {}
    if not looks_like_pg(tags):
        return None

    lat = el.get("lat")
    lon = el.get("lon")
    if lat is None or lon is None:
        center = el.get("center") or {}
        lat, lon = center.get("lat"), center.get("lon")

    source_id = f"{el.get('type')}/{el.get('id')}"
    image = choose(tags, "image", "wikimedia_commons")
    photo_urls = [image] if image else []
    amenities = sorted(
        key.removeprefix("amenity:")
        for key, value in tags.items()
        if key.startswith("amenity:") and value == "yes"
    )

    return {
        "kind": "pg",
        "id": f"osm:{source_id}",
        "source": "openstreetmap",
        "source_id": source_id,
        "name": choose(tags, "name", "official_name", "operator"),
        "address": address_from_tags(tags),
        "locality": choose(
            tags, "addr:suburb", "addr:neighbourhood",
            "addr:quarter", "addr:district", "addr:city",
        ),
        # Attribute records to the configured scan area. OSM addr:city tags are
        # frequently inconsistent (e.g. "Mumbai", spelling variants, or districts).
        "city": city_name,
        "state": choose(tags, "addr:state"),
        "postcode": choose(tags, "addr:postcode"),
        "latitude": lat,
        "longitude": lon,
        "phone": choose(tags, "contact:phone", "phone"),
        "email": choose(tags, "contact:email", "email"),
        "website": choose(tags, "contact:website", "website"),
        "monthly_rent": choose(tags, "charge", "fee"),
        "room_type": choose(tags, "rooms", "capacity", "beds"),
        "amenities": json.dumps(amenities),
        "photo_urls": json.dumps(photo_urls),
        "nearby_locations": json.dumps([]),
        "source_url": f"https://www.openstreetmap.org/{el.get('type')}/{el.get('id')}",
        "raw_tags": json.dumps(tags, ensure_ascii=False),
    }


def distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    from math import asin, cos, radians, sin, sqrt
    dlat, dlon = radians(lat2 - lat1), radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 6371 * 2 * asin(sqrt(min(1, a)))


def normalize_poi(el: dict, city_name: str) -> dict | None:
    tags = el.get("tags") or {}
    category = None
    if tags.get("amenity") in {
        "hospital", "clinic", "university", "college", "school",
        "marketplace", "bus_station",
    }:
        category = tags["amenity"]
    elif tags.get("railway") in {"station", "halt", "subway_entrance"}:
        category = "railway_" + tags["railway"]
    elif tags.get("public_transport") == "station":
        category = "public_transport_station"
    elif tags.get("shop") == "supermarket":
        category = "supermarket"
    if not category:
        return None

    lat, lon = el.get("lat"), el.get("lon")
    if lat is None or lon is None:
        center = el.get("center") or {}
        lat, lon = center.get("lat"), center.get("lon")
    if lat is None or lon is None or not tags.get("name"):
        return None
    return {
        "name": clean(tags.get("name")),
        "category": category,
        "city": city_name,
        "latitude": lat,
        "longitude": lon,
    }


def attach_nearby_locations(pg_rows: list[dict], poi_rows: list[dict]) -> None:
    for row in pg_rows:
        if row.get("latitude") is None or row.get("longitude") is None:
            continue
        candidates = []
        for poi in poi_rows:
            km = distance_km(
                float(row["latitude"]), float(row["longitude"]),
                float(poi["latitude"]), float(poi["longitude"]),
            )
            if km <= 2:
                candidates.append({
                    "name": poi["name"],
                    "type": poi["category"],
                    "distance_km": round(km, 2),
                    "source": "OpenStreetMap",
                })
        candidates.sort(key=lambda item: item["distance_km"])
        row["nearby_locations"] = json.dumps(candidates[:5], ensure_ascii=False)


def upsert_pg(con: sqlite3.Connection, row: dict) -> None:
    timestamp = now()
    old = con.execute(
        "SELECT first_seen FROM pgs WHERE source=? AND source_id=?",
        (row["source"], row["source_id"]),
    ).fetchone()
    values = {column: row.get(column) for column in COLUMNS}
    values["first_seen"] = old[0] if old else timestamp
    values["last_seen"] = timestamp
    values["raw_tags"] = row.get("raw_tags")
    columns = COLUMNS + ["raw_tags"]
    placeholders = ",".join("?" for _ in columns)
    updates = ",".join(
        f"{column}=excluded.{column}"
        for column in columns
        if column not in ("id", "source", "source_id", "first_seen")
    )
    con.execute(
        f"INSERT INTO pgs ({','.join(columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT(source,source_id) DO UPDATE SET {updates}, "
        "last_seen=excluded.last_seen",
        [values.get(column) for column in columns],
    )


def import_authorized_csv(con: sqlite3.Connection) -> int:
    """Import licensed/authorized CSV exports placed in data/incoming/*.csv."""
    incoming = DATA / "incoming"
    if not incoming.exists():
        return 0

    count = 0
    aliases = {
        "property_name": "name", "pg_name": "name",
        "address_full": "address", "phone_number": "phone",
        "contact": "phone", "lat": "latitude", "lon": "longitude",
        "lng": "longitude", "url": "source_url", "image_url": "photo_urls",
        "rent": "monthly_rent",
    }
    for file in incoming.glob("*.csv"):
        with file.open("r", encoding="utf-8-sig", newline="") as handle:
            for raw in csv.DictReader(handle):
                raw = {
                    aliases.get(key.strip().lower(), key.strip().lower()): value
                    for key, value in raw.items()
                }
                name = clean(raw.get("name"))
                if not name:
                    continue
                source = clean(raw.get("source")) or f"authorized_csv:{file.stem}"
                source_id = (
                    clean(raw.get("source_id"))
                    or clean(raw.get("source_url"))
                    or re.sub(r"\W+", "-", name.lower()).strip("-")
                )
                photos = raw.get("photo_urls")
                if photos and not photos.startswith("["):
                    photos = json.dumps([photos])
                try:
                    latitude = float(raw["latitude"]) if clean(raw.get("latitude")) else None
                    longitude = float(raw["longitude"]) if clean(raw.get("longitude")) else None
                except ValueError:
                    latitude, longitude = None, None

                row = {
                    "id": f"{source}:{source_id}", "source": source,
                    "source_id": source_id, "name": name,
                    "address": clean(raw.get("address")),
                    "locality": clean(raw.get("locality")),
                    "city": clean(raw.get("city")), "state": clean(raw.get("state")),
                    "postcode": clean(raw.get("postcode")),
                    "latitude": latitude, "longitude": longitude,
                    "phone": clean(raw.get("phone")), "email": clean(raw.get("email")),
                    "website": clean(raw.get("website")),
                    "monthly_rent": clean(raw.get("monthly_rent")),
                    "room_type": clean(raw.get("room_type")),
                    "amenities": raw.get("amenities") or "[]",
                    "photo_urls": photos or "[]",
                    "nearby_locations": raw.get("nearby_locations") or "[]",
                    "source_url": clean(raw.get("source_url")),
                    "raw_tags": json.dumps(raw, ensure_ascii=False),
                }
                upsert_pg(con, row)
                count += 1
    return count


def export_csv(con: sqlite3.Connection) -> int:
    rows = con.execute(
        f"SELECT {','.join(COLUMNS)} FROM pgs ORDER BY city, name"
    ).fetchall()
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CSV_PATH.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(COLUMNS)
        writer.writerows(rows)
    return len(rows)


def record_scan(
    con: sqlite3.Connection, city: str, started: str,
    status: str, discovered: int, message: str,
) -> None:
    con.execute(
        "INSERT INTO scan_log(city,started_at,status,discovered,message) "
        "VALUES(?,?,?,?,?)",
        (city, started, status, discovered, message[:1000]),
    )
    con.commit()


def main() -> int:
    DATA.mkdir(parents=True, exist_ok=True)
    configured_cities = json.loads(CITIES_PATH.read_text(encoding="utf-8"))
    cities = [city for city in configured_cities if city.get("name") in PILOT_BBOXES]
    missing = sorted(set(PILOT_BBOXES) - {city.get("name") for city in cities})
    if missing:
        raise RuntimeError(f"Requested pilot cities missing from cities.json: {missing}")
    print("METRO PILOT: " + ", ".join(city["name"] for city in cities), flush=True)
    con = connect()
    new_seen = 0
    errors = 0
    empty_cities = 0

    print(f"Starting PG discovery for {len(cities)} configured city seeds.")
    print(f"Overpass endpoints configured: {len(OVERPASS_ENDPOINTS)}")
    print("Note: results only include PG-like records present in OpenStreetMap.")

    for index, city in enumerate(cities, start=1):
        city_name = city.get("name", f"city-{index}")
        started = now()
        try:
            elements, pg_note = get_city_data(city)
            pg_rows = []
            for element in elements:
                row = normalize_osm(element, city_name)
                if row:
                    pg_rows.append(row)

            # Nearby-POI enrichment doubles Overpass traffic. Keep it opt-in so
            # the core PG collection can finish within GitHub Actions limits.
            poi_note = "nearby lookup disabled (set PG_ENRICH_NEARBY=1 to enable)"
            if os.getenv("PG_ENRICH_NEARBY", "0").strip() == "1":
                try:
                    poi_elements, poi_note = get_city_pois(city)
                    poi_rows = [
                        poi for element in poi_elements
                        if (poi := normalize_poi(element, city_name)) is not None
                    ]
                    attach_nearby_locations(pg_rows, poi_rows)
                    poi_note += f"; nearby_pois={len(poi_rows)}"
                except Exception as poi_exc:
                    poi_note = f"nearby lookup failed: {str(poi_exc)[:250]}"
                    print(f"  WARNING {city_name}: {poi_note}", file=sys.stderr)

            for row in pg_rows:
                upsert_pg(con, row)
            con.commit()

            if not pg_rows:
                empty_cities += 1
                status = "empty"
            else:
                status = "ok"

            message = (
                f"{pg_note}; pg_like_records={len(pg_rows)}; {poi_note}; "
                "source=OpenStreetMap"
            )
            record_scan(con, city_name, started, status, len(pg_rows), message)
            new_seen += len(pg_rows)
            print(f"[{index}/{len(cities)}] {city_name}: {len(pg_rows)} PG-like records. {message}")

        except Exception as exc:
            errors += 1
            message = f"{type(exc).__name__}: {str(exc)}"
            record_scan(con, city_name, started, "error", 0, message)
            print(f"[{index}/{len(cities)}] ERROR {city_name}: {message}", file=sys.stderr)

        if index < len(cities):
            time.sleep(CITY_DELAY_SECONDS)

    try:
        imported = import_authorized_csv(con)
        con.commit()
        total = export_csv(con)
        target = int(os.getenv("PG_TARGET_LISTINGS", "1000"))
        city_counts = con.execute(
            "SELECT COALESCE(city, 'Unknown'), COUNT(*) FROM pgs GROUP BY COALESCE(city, 'Unknown') ORDER BY COUNT(*) DESC"
        ).fetchall()
        print(f"TARGET CHECK: {total}/{target} unique records; target_met={total >= target}")
        print("LISTINGS BY CITY: " + json.dumps(city_counts, ensure_ascii=False))
        print(
            f"Run complete. PG-like records observed this run: {new_seen}; "
            f"authorized CSV rows processed: {imported}; "
            f"unique database records: {total}; empty cities: {empty_cities}; "
            f"city errors: {errors}."
        )
        print(f"Database: {DB_PATH}")
        print(f"CSV: {CSV_PATH}")
        print("If every city is empty, inspect boundary names/aliases and Overpass responses.")
        return 1 if errors == len(cities) and cities else 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
