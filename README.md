# India PG Discovery (GitHub Actions, low-cost starter)

A source-aware PG discovery collector with SQLite deduplication, CSV export, scheduled GitHub Actions runs, OpenStreetMap/Overpass collection, and an import adapter for directory exports you are licensed or authorized to use.

## Important coverage limit

This is a working starter, not a promise to find every PG in India. OpenStreetMap coverage for paying-guest accommodation is incomplete and tagging is inconsistent. The collector finds OSM objects tagged as hostels or named like PGs/hostels within administrative areas that Overpass can resolve. It also logs locality objects for coverage auditing, but does not automatically query every locality separately (that would put excessive load on shared community infrastructure). To improve coverage, add permitted source connectors or import authorized listings.

## What it stores

- Property name, OSM source ID and URL, address/locality/city/state/postcode, coordinates
- Public phone/email/website if explicitly published in OSM tags
- Explicit rent/fee tags when present; no invented pricing
- Amenity tags and explicit image/Wikimedia references (URLs only; images are not downloaded or rehosted)
- First/last seen timestamps, SQLite unique constraint and update-in-place deduplication
- CSV export and scan log

Fields missing from a source stay empty. Nearby POIs, room-level availability and photos are usually not present in OSM; these require a source that supplies them and permits their reuse.

## Run locally

Requires Python 3.10+.

```bash
python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
python -u src/pg_scraper.py
```

Outputs: `data/pgs.sqlite` and `data/pgs.csv`.

To contact the shared Overpass service responsibly, set `PG_USER_AGENT` to a descriptive value with a contact email. Runs are sequential with a delay. If endpoints are overloaded, reduce frequency, increase `CITY_DELAY_SECONDS`, or use a contracted/provider-hosted Overpass service. Do not distribute queries across multiple runners to evade limits.

## GitHub Actions deployment

1. Create a GitHub repository, e.g. `india-pg-discovery`.
2. Upload this project and push the `main` branch.
3. Open **Settings → Actions → General → Workflow permissions** and allow read/write repository contents (the workflow also declares `contents: write`).
4. Edit `.github/workflows/pg-discovery.yml` and replace the example contact email in `PG_USER_AGENT` with an email you control.
5. Open **Actions → India PG Discovery → Run workflow** to run it manually.
6. The schedule runs weekly. Change the cron only after considering source policies and actual run duration.
7. The run uploads CSV and SQLite as a 30-day artifact and commits the latest snapshot to `main` so data survives ephemeral GitHub runners.

The SQLite file grows over time; periodically export/archive it or move to managed Postgres when you need concurrent users or a public application. Avoid storing personal data or private owner contact details.

## Authorized CSV connector

Place CSV exports from sources you are licensed/authorized to use in `data/incoming/`. Supported columns include:

`name,source,source_id,address,locality,city,state,postcode,latitude,longitude,phone,email,website,monthly_rent,room_type,amenities,photo_urls,nearby_locations,source_url`

Aliases include `pg_name`, `property_name`, `phone_number`, `contact`, `lat`, `lon`, `lng`, `url`, `image_url`, and `rent`. Use a stable source ID or URL to deduplicate. The workflow ignores incoming files by default via `.gitignore`; if you use this connector in Actions, obtain the data at runtime from an authorized source or deliberately manage the files securely.

## Adding source connectors

Build each portal connector as a separate module and normalize its records to the fields used by `upsert_pg`. Use only official APIs, licensed feeds, or sites where automated collection is permitted. Respect terms, robots directives where applicable, authentication/access controls, rate limits, privacy rules, and photo licensing. Do not bypass CAPTCHAs, logins, anti-bot controls, or blocks. Preserve source URL and retrieval timestamp. Avoid rehosting photos unless the license/terms allow it.

Google Places API is intentionally not enabled by default: it requires a Google Cloud project, API key and billing, and its storage/display/photo restrictions must be reviewed before writing data into a persistent independent database. See official India pricing and terms before enabling it.

## Data sources and licensing

OpenStreetMap data is under the Open Database License (ODbL), requires attribution, and may trigger share-alike obligations for a derived database. Read the official policies before commercial use:

- Overpass API: https://wiki.openstreetmap.org/wiki/Overpass_API#Public_Overpass_API_instances
- Nominatim policy (this project does not use public Nominatim for bulk geocoding): https://operations.osmfoundation.org/policies/nominatim/
- OSM copyright/license: https://www.openstreetmap.org/copyright
- Google Maps Platform India pricing: https://developers.google.com/maps/billing-and-pricing/pricing-india

## Known limitations / next steps

- Administrative boundary resolution differs across cities and Overpass instances; review `scan_log` after each run.
- OSM does not contain every PG and often lacks phone, rent and photos.
- The script stores photo references only, not image binaries.
- Nearby landmarks and distances are left empty unless a connector supplies them; don't infer them from locality names.
- Production-grade all-India coverage requires multiple permitted sources, a durable database, monitoring, retries, and a data licensing plan.
