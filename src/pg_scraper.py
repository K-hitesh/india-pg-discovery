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
        "https://overpass-api.de/api/interpreter,"
        "https://overpass.private.coffee/api/interpreter,"
        "https://overpass.kumi.systems/api/interpreter",
    ).split(",")
    if item.strip()
]
REQUEST_TIMEOUT = int(os.getenv("OVERPASS_TIMEOUT_SECONDS", "10"))
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


# Approximate collection windows for every configured city in cities.json.
# Bounds are south, west, north, east; they are discovery windows, not legal borders.
CITY_BBOXES = {
    "Bengaluru": (12.70, 77.30, 13.20, 77.85),
    "Hyderabad": (17.10, 78.20, 17.65, 78.75),
    "Chennai": (12.70, 79.95, 13.30, 80.40),
    "Pune": (18.25, 73.60, 18.80, 74.05),
    "Mumbai": (18.80, 72.70, 19.40, 73.10),
    "Delhi": (28.35, 76.80, 28.95, 77.40),
    "Noida": (28.35, 77.25, 28.70, 77.60),
    "Gurugram": (28.20, 76.80, 28.60, 77.25),
    "Mysuru": (12.20, 76.45, 12.45, 76.75),
    "Kochi": (9.80, 76.15, 10.15, 76.45),
    "Thiruvananthapuram": (8.35, 76.80, 8.65, 77.00),
    "Kozhikode": (11.15, 75.70, 11.45, 76.00),
    "Thrissur": (10.40, 76.10, 10.65, 76.35),
    "Coimbatore": (10.90, 76.85, 11.15, 77.15),
    "Visakhapatnam": (17.55, 83.05, 17.90, 83.45),
    "Vijayawada": (16.40, 80.50, 16.65, 80.75),
    "Ahmedabad": (22.90, 72.40, 23.20, 72.75),
    "Jaipur": (26.75, 75.65, 27.05, 76.00),
    "Kolkata": (22.40, 88.20, 22.70, 88.50),
    "Indore": (22.60, 75.70, 22.85, 76.00),
    "Lucknow": (26.70, 80.80, 27.00, 81.10),
    "Guwahati": (26.05, 91.55, 26.30, 91.85),
    "Chandigarh": (30.60, 76.65, 30.80, 76.90),
    "Mohali": (30.55, 76.60, 30.80, 76.85),
    "Greater Noida": (28.35, 77.40, 28.65, 77.75),
    "Faridabad": (28.30, 77.20, 28.55, 77.45),
    "Ghaziabad": (28.55, 77.30, 28.80, 77.55),
    "Nagpur": (21.00, 78.95, 21.25, 79.20),
    "Nashik": (19.90, 73.65, 20.10, 73.95),
    "Surat": (21.05, 72.70, 21.30, 73.00),
    "Vadodara": (22.20, 73.05, 22.45, 73.30),
    "Bhopal": (23.10, 77.25, 23.35, 77.55),
    "Patna": (25.50, 85.05, 25.70, 85.25),
    "Bhubaneswar": (20.15, 85.70, 20.40, 85.95),
    "Ranchi": (23.25, 85.20, 23.50, 85.45),
    "Dehradun": (30.20, 77.90, 30.45, 78.15),
    "Amritsar": (31.55, 74.75, 31.80, 75.00),
    "Jalandhar": (31.25, 75.45, 31.55, 75.70),
    "Mangaluru": (12.75, 74.75, 13.05, 75.05),
    "Hubballi": (15.25, 75.05, 15.55, 75.35),
    "Tiruchirappalli": (10.70, 78.55, 10.95, 78.85),
    "Madurai": (9.80, 78.00, 10.05, 78.25),
    "Salem": (11.55, 78.05, 11.85, 78.35),
    "Tirupati": (13.55, 79.30, 13.80, 79.65),
    "Warangal": (17.90, 79.45, 18.15, 79.75),
    "Navi Mumbai": (18.90, 72.90, 19.20, 73.15),
    "Thane": (19.10, 72.90, 19.40, 73.20),
}

def build_query(city: dict, include_pois: bool = False) -> str:
    """Use a bounded bbox query rather than expensive administrative-area lookup."""
    bbox = CITY_BBOXES.get(city["name"])
    if bbox is None:
        raise ValueError(f"No city bounding box configured for {city['name']}")
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
    south, west, north, east = CITY_BBOXES[city["name"]]
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
    """Try the cheap city query, then one fallback request; keep all cities moving."""
    notes: list[str] = []
    elements: list[dict] = []
    generator = "unknown"
    try:
        payload = overpass_request(build_query(city, include_pois=False), f"{city['name']} tagged-hostel query")
        elements = payload.get("elements", [])
        generator = payload.get("generator", "unknown")
    except RuntimeError as exc:
        notes.append(f"tagged query failed: {str(exc)[:250]}")

    if not elements:
        try:
            payload = overpass_request(build_name_fallback_query(city), f"{city['name']} name fallback")
            elements = payload.get("elements", [])
            generator = payload.get("generator", generator)
            notes.append("used name-search fallback")
        except RuntimeError as exc:
            notes.append(f"name fallback failed: {str(exc)[:250]}")

    unique: dict[tuple[str, int], dict] = {}
    for element in elements:
        unique[(str(element.get("type")), int(element.get("id", 0)))] = element
    elements = list(unique.values())
    note = f"pg_query_elements={len(elements)}; generator={generator}; " + "; ".join(notes)
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
    cities = configured_cities
    missing = [city.get("name", "?") for city in cities if city.get("name") not in CITY_BBOXES]
    if missing:
        raise RuntimeError(f"Cities missing discovery bounds: {missing}")
    print(f"ALL-CITY MODE: scanning all {len(cities)} configured cities.", flush=True)
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
