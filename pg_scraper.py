from __future__ import annotations
import csv, json, os, re, sqlite3, sys, time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import requests

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = Path(os.getenv("PG_DB_PATH", DATA / "pgs.sqlite"))
CSV_PATH = Path(os.getenv("PG_CSV_PATH", DATA / "pgs.csv"))
CITIES_PATH = ROOT / "cities.json"
USER_AGENT = os.getenv("PG_USER_AGENT", "IndiaPGDiscovery/1.0 (research project; contact: set PG_USER_AGENT to your contact email)")
OVERPASS_ENDPOINTS = [
    x.strip() for x in os.getenv("OVERPASS_ENDPOINTS", "https://overpass-api.de/api/interpreter,https://overpass.kumi.systems/api/interpreter").split(",") if x.strip()
]
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})

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

COLUMNS = ["id","source","source_id","name","address","locality","city","state","postcode","latitude","longitude","phone","email","website","monthly_rent","room_type","amenities","photo_urls","nearby_locations","source_url","first_seen","last_seen"]


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect() -> sqlite3.Connection:
    DATA.mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(SCHEMA)
    return con


def clean(value: Any) -> str | None:
    if value is None:
        return None
    value = re.sub(r"\s+", " ", str(value)).strip()
    return value or None


def choose(tags: dict, *keys: str) -> str | None:
    for key in keys:
        val = clean(tags.get(key))
        if val:
            return val
    return None


def looks_like_pg(tags: dict) -> bool:
    name = (tags.get("name") or "").lower()
    # OSM tagging is inconsistent: hostel tags are included; name matching catches some PGs.
    return (tags.get("amenity") == "hostel" or tags.get("tourism") == "hostel"
            or tags.get("building") == "hostel"
            or bool(re.search(r"\b(pg|paying guest|paying-guest|co.?living|hostel|ladies hostel|mens hostel)\b", name)))


def address_from_tags(tags: dict) -> str | None:
    parts = [tags.get(k) for k in ("addr:housenumber", "addr:street", "addr:suburb", "addr:neighbourhood", "addr:city", "addr:district", "addr:state", "addr:postcode")]
    parts = [clean(x) for x in parts if clean(x)]
    return ", ".join(dict.fromkeys(parts)) or None


def get_city_data(city: dict) -> list[dict]:
    """Query OSM's Overpass API once per city; no systematic public Nominatim geocoding."""
    aliases = [re.escape(a) for a in city["aliases"]]
    alias_regex = "|".join(aliases)
    # City boundary may be named either the city or a known alias. Query localities and PG-like POIs
    # within the matching administrative area. Data coverage depends on OSM tagging completeness.
    query = f'''[out:json][timeout:90];
    area["name"~"^({alias_regex})$",i]["boundary"="administrative"]->.cityArea;
    (
      nwr(area.cityArea)["amenity"="hostel"];
      nwr(area.cityArea)["tourism"="hostel"];
      nwr(area.cityArea)["building"="hostel"];
      nwr(area.cityArea)["name"~"\\b(PG|paying guest|paying-guest|co.?living|hostel|ladies hostel|mens hostel)\\b",i];
      node(area.cityArea)["place"~"^(suburb|neighbourhood|quarter|district)$"];
      way(area.cityArea)["place"~"^(suburb|neighbourhood|quarter|district)$"];
      relation(area.cityArea)["place"~"^(suburb|neighbourhood|quarter|district)$"];
      nwr(area.cityArea)["amenity"~"^(hospital|clinic|university|college|school|marketplace|bus_station)$"];
      nwr(area.cityArea)["railway"~"^(station|halt|subway_entrance)$"];
      nwr(area.cityArea)["public_transport"="station"];
      nwr(area.cityArea)["shop"="supermarket"];
    ); out center tags;'''
    last_error = None
    for endpoint in OVERPASS_ENDPOINTS:
        try:
            response = SESSION.post(endpoint, data={"data": query}, timeout=(15, 110))
            if response.status_code in (429, 502, 503, 504):
                last_error = RuntimeError(f"Overpass HTTP {response.status_code} at {endpoint}")
                time.sleep(3)
                continue
            response.raise_for_status()
            payload = response.json()
            return payload.get("elements", [])
        except Exception as exc:
            last_error = exc
            time.sleep(2)
    raise RuntimeError(str(last_error or "No Overpass endpoint succeeded"))


def normalize_osm(el: dict, city_name: str) -> dict | None:
    tags = el.get("tags") or {}
    is_pg = looks_like_pg(tags)
    is_locality = tags.get("place") in {"suburb", "neighbourhood", "quarter", "district"}
    poi_category = None
    if tags.get("amenity") in {"hospital", "clinic", "university", "college", "school", "marketplace", "bus_station"}:
        poi_category = tags.get("amenity")
    elif tags.get("railway") in {"station", "halt", "subway_entrance"}:
        poi_category = "railway_" + tags.get("railway")
    elif tags.get("public_transport") == "station":
        poi_category = "public_transport_station"
    elif tags.get("shop") == "supermarket":
        poi_category = "supermarket"
    if not (is_pg or is_locality or poi_category):
        return None
    lat = el.get("lat")
    lon = el.get("lon")
    if lat is None or lon is None:
        center = el.get("center") or {}
        lat, lon = center.get("lat"), center.get("lon")
    # Locality and POI objects enrich nearby-location fields but are not stored as PG properties.
    if is_locality:
        return {"kind": "locality", "name": clean(tags.get("name")), "city": city_name,
                "latitude": lat, "longitude": lon, "source_id": f"{el.get('type')}/{el.get('id')}"}
    if poi_category:
        return {"kind": "poi", "name": clean(tags.get("name")), "category": poi_category, "city": city_name,
                "latitude": lat, "longitude": lon, "source_id": f"{el.get('type')}/{el.get('id')}"}
    source_id = f"{el.get('type')}/{el.get('id')}"
    image = choose(tags, "image", "wikimedia_commons")
    photo_urls = [image] if image else []
    amenities = sorted(k.removeprefix("amenity:") for k, v in tags.items() if k.startswith("amenity:") and v == "yes")
    # Keep only explicit, source-provided photo references. Do not download/rehost image files.
    return {"kind": "pg", "id": f"osm:{source_id}", "source": "openstreetmap", "source_id": source_id,
        "name": choose(tags, "name", "official_name", "operator"), "address": address_from_tags(tags),
        "locality": choose(tags, "addr:suburb", "addr:neighbourhood", "addr:quarter", "addr:district"),
        "city": choose(tags, "addr:city") or city_name, "state": choose(tags, "addr:state"),
        "postcode": choose(tags, "addr:postcode"), "latitude": lat, "longitude": lon,
        "phone": choose(tags, "contact:phone", "phone"), "email": choose(tags, "contact:email", "email"),
        "website": choose(tags, "contact:website", "website"), "monthly_rent": choose(tags, "charge", "fee"),
        "room_type": choose(tags, "rooms", "capacity", "beds"), "amenities": json.dumps(amenities),
        "photo_urls": json.dumps(photo_urls), "nearby_locations": json.dumps([]),
        "source_url": f"https://www.openstreetmap.org/{el.get('type')}/{el.get('id')}",
        "raw_tags": json.dumps(tags, ensure_ascii=False)}


def distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    from math import asin, cos, radians, sin, sqrt
    dlat, dlon = radians(lat2-lat1), radians(lon2-lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
    return 6371 * 2 * asin(sqrt(min(1, a)))


def upsert_pg(con: sqlite3.Connection, row: dict) -> None:
    timestamp = now()
    old = con.execute("SELECT first_seen FROM pgs WHERE source=? AND source_id=?", (row["source"], row["source_id"])).fetchone()
    first_seen = old[0] if old else timestamp
    values = {col: row.get(col) for col in COLUMNS}
    values["first_seen"] = first_seen
    values["last_seen"] = timestamp
    cols = COLUMNS + ["raw_tags"]
    values["raw_tags"] = row.get("raw_tags")
    placeholders = ",".join("?" for _ in cols)
    updates = ",".join(f"{c}=excluded.{c}" for c in cols if c not in ("id", "source", "source_id", "first_seen"))
    con.execute(f"INSERT INTO pgs ({','.join(cols)}) VALUES ({placeholders}) ON CONFLICT(source,source_id) DO UPDATE SET {updates}, last_seen=excluded.last_seen",
                [values.get(c) for c in cols])


def import_authorized_csv(con: sqlite3.Connection) -> int:
    """Optional connector for licensed/authorized directory exports in data/incoming/*.csv.
    Expected headers are flexible; source URL and a stable source ID are strongly recommended.
    """
    incoming = DATA / "incoming"
    if not incoming.exists():
        return 0
    count = 0
    aliases = {"property_name":"name", "pg_name":"name", "address_full":"address", "phone_number":"phone", "contact":"phone", "lat":"latitude", "lon":"longitude", "lng":"longitude", "url":"source_url", "image_url":"photo_urls", "rent":"monthly_rent"}
    for file in incoming.glob("*.csv"):
        with file.open("r", encoding="utf-8-sig", newline="") as fh:
            for raw in csv.DictReader(fh):
                raw = {aliases.get(k.strip().lower(), k.strip().lower()): v for k, v in raw.items()}
                name = clean(raw.get("name"))
                if not name:
                    continue
                source = clean(raw.get("source")) or f"authorized_csv:{file.stem}"
                source_id = clean(raw.get("source_id")) or clean(raw.get("source_url")) or re.sub(r"\W+", "-", name.lower()).strip("-")
                stable_id = f"{source}:{source_id}"
                photos = raw.get("photo_urls")
                if photos and not photos.startswith("["):
                    photos = json.dumps([photos])
                row = {"id": stable_id, "source": source, "source_id": source_id, "name": name,
                       "address": clean(raw.get("address")), "locality": clean(raw.get("locality")),
                       "city": clean(raw.get("city")), "state": clean(raw.get("state")), "postcode": clean(raw.get("postcode")),
                       "latitude": float(raw["latitude"]) if clean(raw.get("latitude")) else None,
                       "longitude": float(raw["longitude"]) if clean(raw.get("longitude")) else None,
                       "phone": clean(raw.get("phone")), "email": clean(raw.get("email")), "website": clean(raw.get("website")),
                       "monthly_rent": clean(raw.get("monthly_rent")), "room_type": clean(raw.get("room_type")),
                       "amenities": raw.get("amenities") or "[]", "photo_urls": photos or "[]", "nearby_locations": raw.get("nearby_locations") or "[]",
                       "source_url": clean(raw.get("source_url")), "raw_tags": json.dumps(raw, ensure_ascii=False)}
                upsert_pg(con, row)
                count += 1
    return count


def export_csv(con: sqlite3.Connection) -> int:
    rows = con.execute(f"SELECT {','.join(COLUMNS)} FROM pgs ORDER BY city, name").fetchall()
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CSV_PATH.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(COLUMNS)
        writer.writerows(rows)
    return len(rows)


def main() -> int:
    DATA.mkdir(parents=True, exist_ok=True)
    cities = json.loads(CITIES_PATH.read_text(encoding="utf-8"))
    con = connect()
    pg_total = 0
    errors = 0
    for idx, city in enumerate(cities, start=1):
        started = now()
        try:
            elements = get_city_data(city)
            pg_count = 0
            locality_count = 0
            pg_rows = []
            nearby_points = []
            for el in elements:
                row = normalize_osm(el, city["name"])
                if not row:
                    continue
                if row.get("kind") == "locality":
                    locality_count += 1
                    continue
                if row.get("kind") == "poi":
                    if row.get("latitude") is not None and row.get("longitude") is not None and row.get("name"):
                        nearby_points.append(row)
                    continue
                pg_rows.append(row)
            for row in pg_rows:
                if row.get("latitude") is not None and row.get("longitude") is not None:
                    candidates = []
                    for poi in nearby_points:
                        km = distance_km(float(row["latitude"]), float(row["longitude"]), float(poi["latitude"]), float(poi["longitude"]))
                        if km <= 2.0:
                            candidates.append({"name": poi["name"], "type": poi["category"], "distance_km": round(km, 2), "source": "OpenStreetMap"})
                    candidates.sort(key=lambda x: x["distance_km"])
                    row["nearby_locations"] = json.dumps(candidates[:5], ensure_ascii=False)
                upsert_pg(con, row)
                pg_count += 1
            con.execute("INSERT INTO scan_log(city,started_at,status,discovered,message) VALUES(?,?,?,?,?)",
                        (city["name"], started, "ok", pg_count, f"localities_seen={locality_count}; nearby_pois={len(nearby_points)}; elements={len(elements)}"))
            con.commit()
            pg_total += pg_count
            print(f"[{idx}/{len(cities)}] {city['name']}: {pg_count} PG-like records; {locality_count} locality objects")
        except Exception as exc:
            errors += 1
            con.execute("INSERT INTO scan_log(city,started_at,status,discovered,message) VALUES(?,?,?,?,?)",
                        (city["name"], started, "error", 0, str(exc)[:1000]))
            con.commit()
            print(f"[{idx}/{len(cities)}] ERROR {city['name']}: {exc}", file=sys.stderr)
        # Be polite to shared community infrastructure; the workflow is deliberately sequential.
        if idx < len(cities):
            time.sleep(float(os.getenv("CITY_DELAY_SECONDS", "2.5")))
    imported = import_authorized_csv(con)
    con.commit()
    total = export_csv(con)
    con.close()
    print(f"Run complete. New/updated OSM PG-like records observed this run: {pg_total}; authorized CSV rows processed: {imported}; unique database records: {total}; city errors: {errors}")
    print(f"Database: {DB_PATH}; CSV: {CSV_PATH}")
    # Non-zero only if every city failed; partial results are still valuable and committed.
    return 1 if errors == len(cities) else 0

if __name__ == "__main__":
    raise SystemExit(main())
