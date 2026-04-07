"""  # pylint: disable=logging-fstring-interpolation,logging-not-lazy,line-too-long,import-outside-toplevel,f-string-without-interpolation,unused-variable
etl/scrapers/city_permits.py

US City Building Permit Scrapers — private construction project data
All use Socrata Open Data APIs (free, no key required for basic access)

1. NYC DOB       — New York City Dept of Buildings (best dataset in the US)
2. Chicago       — City of Chicago Building Permits
3. Los Angeles   — LA Dept of Building & Safety
4. Houston       — City of Houston Permits
5. Philadelphia  — City of Philadelphia Licenses & Inspections
6. USACE         — US Army Corps of Engineers (major civil/infrastructure)

All return ProjectRecord objects compatible with the CSTGlobal pipeline.
"""

import re
import time
import logging
from typing import Optional
from datetime import datetime, timedelta, timezone

import os
import requests

from etl.base_scraper import BaseScraper, ProjectRecord

logger = logging.getLogger(__name__)


# ── Shared utilities ──────────────────────────────────────────

SECTOR_KEYWORDS = {
    "Transport":       ["transit", "rail", "road", "highway", "bridge", "tunnel", "airport", "port", "parking garage", "parking structure"],
    "Energy":          ["solar", "wind", "power", "energy", "generator", "substation", "fuel", "utility"],
    "Water":           ["water", "sewer", "sewage", "drainage", "stormwater", "plumbing", "flood"],
    "Commercial":      ["office", "retail", "store", "shop", "hotel", "motel", "restaurant", "bank", "commercial", "business"],
    "Healthcare":      ["hospital", "clinic", "medical", "health", "dental", "pharmacy", "care facility"],
    "Education":       ["school", "university", "college", "library", "education", "daycare", "nursery"],
    "Residential":     ["apartment", "condo", "condominium", "dwelling", "residential", "housing", "single family", "multi family", "townhouse", "house"],
    "Sport & Leisure": ["stadium", "arena", "gym", "fitness", "sport", "recreation", "pool", "theater", "cinema"],
    "Infrastructure":  ["infrastructure", "data center", "warehouse", "logistics", "industrial", "manufacturing", "facility"],
    "Mixed Use":       ["mixed use", "mixed-use", "development", "mixed"],
}

MIN_VALUE_USD = 500_000   # Filter out trivial permits under $500k


def infer_sector(text: str) -> str:
    t = (text or "").lower()
    for sector, kws in SECTOR_KEYWORDS.items():
        if any(k in t for k in kws):
            return sector
    return "Commercial"


def parse_value(v) -> Optional[float]:
    if v is None:
        return None
    s = str(v).replace(",", "").replace("$", "").strip()
    try:
        return float(re.sub(r"[^\d.]", "", s))
    except (ValueError, TypeError):
        return None


def socrata_fetch(base_url: str, dataset_id: str, where: str, select: str,
                  order: str = "issued_date DESC", limit: int = 500,
                  app_token: Optional[str] = None) -> list[dict]:
    """Generic Socrata Open Data API fetcher."""
    url = f"{base_url}/resource/{dataset_id}.json"
    headers = {"Accept": "application/json"}
    if app_token:
        headers["X-App-Token"] = app_token

    results = []
    offset = 0
    page_size = min(limit, 1000)

    while offset < limit:
        try:
            resp = requests.get(
                url,
                params={
                    "$where":  where,
                    "$select": select,
                    "$order":  order,
                    "$limit":  page_size,
                    "$offset": offset,
                },
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            results.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
            time.sleep(0.3)
        except requests.RequestException as e:
            logger.warning(f"Socrata fetch error at offset {offset}: {e}")
            break

    return results


def days_ago(n: int) -> str:
    """Return ISO date string for n days ago, formatted for Socrata WHERE clause."""
    dt = datetime.now(timezone.utc) - timedelta(days=n)
    return dt.strftime("%Y-%m-%dT00:00:00")


# ═══════════════════════════════════════════════════════════════
# SCRAPER 1 — NYC Department of Buildings
# Best municipal permit dataset in the US
# ~50,000+ permits/month, includes job value, address, type
# Dataset: DOB Job Application Filings
# ═══════════════════════════════════════════════════════════════

class NYCPermitScraper(BaseScraper):
    source_name = "NYC DOB"

    DATASET_ID = "ic3t-wcy2"  # DOB Job Application Filings (confirmed working)
    BASE_URL   = "https://data.cityofnewyork.us"

    def fetch_raw(self) -> list[dict]:
        app_token = os.environ.get("SOCRATA_APP_TOKEN") or os.environ.get("NYC_APP_TOKEN", "")
        try:
            resp = requests.get(
                f"{self.BASE_URL}/resource/{self.DATASET_ID}.json",
                params={
                    "$limit": 1000,
                    "$where": "job_status_descrp NOT IN ('COMPLETED','SIGNED OFF')",
                    "$$app_token": app_token,
                },
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            records = resp.json()
            filtered = [
                r for r in records
                if r.get("job_type") in ("NB", "A1")
                and self._get_cost(r) >= MIN_VALUE_USD
            ]
            logger.info(f"[NYC DOB] Fetched {len(records)} raw, {len(filtered)} above ${MIN_VALUE_USD:,}")
            return filtered
        except requests.RequestException as e:
            logger.warning(f"[NYC DOB] Fetch failed: {e}")
            return []


    def _get_cost(self, raw: dict) -> float:
        for field in ("initial_cost", "estimated_job_costs", "total_construction_floor_area"):
            v = raw.get(field)
            if v:
                try:
                    cleaned = str(v).replace(",", "").replace("$", "").strip()
                    val = float(cleaned)
                    # initial_cost is in dollars, floor area is sqft — only use dollar fields
                    if field != "total_construction_floor_area":
                        return val
                except (ValueError, TypeError):
                    pass
        return 0

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        # Confirmed ID field from CSV: job__ (displays as "Job #" in the UI)
        job_id = str(raw.get("job__") or raw.get("job_s1_no") or raw.get(":id") or "").strip()
        if not job_id:
            return None

        value = self._get_cost(raw)
        if value < MIN_VALUE_USD:
            return None

        # Title — confirmed column names from CSV
        job_type   = raw.get("job_type", "")
        bldg_type  = raw.get("building_class") or raw.get("existing_occupancy") or raw.get("proposed_occupancy") or ""
        desc       = raw.get("job_description") or raw.get("work_type") or ""
        type_label = "New Building" if job_type == "NB" else "Major Alteration"
        title = f"{type_label} - {bldg_type}" if bldg_type else type_label
        if desc:
            title = f"{type_label}: {str(desc)[:120]}"

        # Location — confirmed column names: house__, street_name, borough
        house   = raw.get("house__") or raw.get("house_no") or ""
        street  = raw.get("street_name") or ""
        borough = raw.get("borough") or ""
        address = f"{house} {street}".strip()
        location = f"{address}, {borough}, New York City, NY".strip(", ")

        # Coordinates — confirmed: gis_latitude, gis_longitude
        try:
            lat = float(raw["gis_latitude"])  if raw.get("gis_latitude")  else None
            lng = float(raw["gis_longitude"]) if raw.get("gis_longitude") else None
        except (ValueError, TypeError):
            lat, lng = None, None

        # Stage — confirmed status values from CSV
        status = (raw.get("job_status_descrp") or raw.get("job_status") or "").upper()
        if any(s in status for s in ("SIGNED OFF", "COMPLETED", "CERTIFICATE OF OCCUPANCY")):
            return None  # skip non-actionable
        stage = "Tender" if "PERMIT ISSUED" in status else "Planning"

        # Owner — confirmed column names
        biz   = raw.get("owner_s_business_name") or ""
        first = raw.get("owner_s_first_name") or ""
        last  = raw.get("owner_s_last_name") or ""
        owner = biz or f"{first} {last}".strip() or ""

        filing_date = (raw.get("dobrundate") or raw.get("pre_filing_date") or "")[:10]

        return ProjectRecord(
            external_id      = f"nyc-{job_id}",
            source_name      = self.source_name,
            source_url       = f"https://a810-bisweb.nyc.gov/bisweb/JobsQueryByNumberServlet?passjobnumber={job_id}",
            title            = title[:500],
            description      = str(desc)[:2000] if desc else None,
            value_usd        = value,
            value_currency   = "USD",
            location_display = location[:500],
            location_country = "US",
            region           = "Americas",
            sector           = infer_sector(f"{title} {bldg_type} {desc}"),
            stage            = stage,
            timeline_display = filing_date,
            lat              = lat,
            lng              = lng,
            stakeholders     = [{"name": owner, "role": "Owner"}] if owner else [],
        )


# ═══════════════════════════════════════════════════════════════
# SCRAPER 2 — City of Chicago Building Permits
# Comprehensive permit data including commercial and large residential
# Dataset: Building Permits
# ═══════════════════════════════════════════════════════════════

class ChicagoPermitScraper(BaseScraper):
    source_name = "Chicago Permits"

    DATASET_ID = "ydr8-5enu"
    BASE_URL   = "https://data.cityofchicago.org"

    def fetch_raw(self) -> list[dict]:
        app_token = os.environ.get("SOCRATA_APP_TOKEN") or os.environ.get("NYC_APP_TOKEN", "")
        try:
            resp = requests.get(
                f"{self.BASE_URL}/resource/{self.DATASET_ID}.json",
                params={"$limit": 1000, "$where": "permit_status NOT IN ('COMPLETE','ISSUED')", "$$app_token": app_token},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            records = resp.json()
            filtered = [
                r for r in records
                if "NEW CONSTRUCTION" in (r.get("permit_type") or "").upper()
                or "RENOVATION" in (r.get("permit_type") or "").upper()
            ]
            filtered = [r for r in filtered if self._parse_cost(r) >= MIN_VALUE_USD]
            logger.info(f"[Chicago Permits] Fetched {len(records)} raw, {len(filtered)} filtered")
            return filtered
        except requests.RequestException as e:
            logger.warning(f"[Chicago Permits] Fetch failed: {e}")
            return []

    def _parse_cost(self, r):
        for f in ("reported_cost", "estimated_cost", "total_fee"):
            v = r.get(f)
            if v:
                try: return float(str(v).replace(",","").replace("$",""))
                except: pass
        return 0

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        permit_id = raw.get("permit_") or raw.get("id", "")
        if not permit_id:
            return None

        value = parse_value(raw.get("reported_cost"))
        if value and value < MIN_VALUE_USD:
            return None

        permit_type = raw.get("permit_type", "")
        desc        = raw.get("work_description", "")
        type_label  = "New Construction" if "NEW" in permit_type.upper() else "Renovation"
        title       = f"{type_label}: {str(desc)[:120]}" if desc else type_label

        # Address
        num  = raw.get("street_number", "")
        dir_ = raw.get("street_direction", "")
        name = raw.get("street_name", "")
        suf  = raw.get("suffix", "")
        area = raw.get("community_area", "")
        address  = f"{num} {dir_} {name} {suf}".strip()
        location = f"{address}, {area}, Chicago, IL".strip(", ")

        try:
            lat = float(raw["latitude"])  if raw.get("latitude")  else None
            lng = float(raw["longitude"]) if raw.get("longitude") else None
        except (ValueError, TypeError):
            lat, lng = None, None

        contact = raw.get("contact_1_name", "")
        issue_date = (raw.get("issue_date") or "")[:10]

        return ProjectRecord(
            external_id      = f"chi-{permit_id}",
            source_name      = self.source_name,
            source_url       = "https://www.chicago.gov/city/en/depts/bldgs.html",
            title            = title[:500],
            description      = str(desc)[:2000] if desc else None,
            value_usd        = value,
            value_currency   = "USD",
            location_display = location[:500],
            location_country = "US",
            region           = "Americas",
            sector           = infer_sector(f"{title} {desc}"),
            stage            = "Planning",
            timeline_display = issue_date,
            lat              = lat,
            lng              = lng,
            stakeholders     = [{"name": contact, "role": "Owner"}] if contact else [],
        )


# ═══════════════════════════════════════════════════════════════
# SCRAPER 3 — Los Angeles Building & Safety
# LA permit data — large city, strong commercial and mixed-use coverage
# Dataset: Building and Safety Permit Information
# ═══════════════════════════════════════════════════════════════

class LosAngelesPermitScraper(BaseScraper):
    source_name = "LA Building & Safety"

    DATASET_ID = "bi25-emib"  # LA City Permits (updated 2025)
    BASE_URL   = "https://data.lacity.org"

    def fetch_raw(self) -> list[dict]:
        app_token = os.environ.get("SOCRATA_APP_TOKEN") or os.environ.get("NYC_APP_TOKEN", "")
        try:
            resp = requests.get(
                f"{self.BASE_URL}/resource/{self.DATASET_ID}.json",
                params={"$limit": 1000, "$$app_token": app_token},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            records = resp.json()
            filtered = [
                r for r in records
                if any(t in (r.get("permit_type") or "").upper()
                       for t in ("BLDG-NEW", "BLDG-ADD", "BLDG-ALTER", "NEW", "ADDITION"))
            ]
            filtered = [r for r in filtered if self._parse_cost(r) >= MIN_VALUE_USD]
            logger.info(f"[LA Building & Safety] Fetched {len(records)} raw, {len(filtered)} filtered")
            return filtered
        except requests.RequestException as e:
            logger.warning(f"[LA Building & Safety] Fetch failed: {e}")
            return []

    def _parse_cost(self, r):
        for f in ("valuation", "estimated_value", "permit_value"):
            v = r.get(f)
            if v:
                try: return float(str(v).replace(",","").replace("$",""))
                except: pass
        return 0

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        permit_id = (raw.get("permit_number") or raw.get("permit_nbr") or
                     raw.get("pcis_permit") or raw.get("permit") or "").strip()
        if not permit_id:
            # Log keys to diagnose
            logger.info(f"[LA] No permit_id found, available keys: {list(raw.keys())[:10]}")
            return None

        value = self._parse_cost(raw)
        if value < MIN_VALUE_USD:
            return None

        permit_type = raw.get("permit_type") or raw.get("permit_type_description") or ""
        desc        = raw.get("work_description") or raw.get("permit_description") or raw.get("description") or ""
        type_label  = "New Building" if any(t in permit_type.upper() for t in ("NEW","BLDG-N")) else "Alteration/Addition"
        title       = f"{type_label}: {str(desc)[:120]}" if desc else f"{type_label} - {permit_type}"

        address  = raw.get("address") or raw.get("site_address") or ""
        zip_code = raw.get("zip_code") or raw.get("zip") or ""
        location = f"{address}, Los Angeles, CA {zip_code}".strip(", ")

        try:
            lat = float(raw["latitude"]) if raw.get("latitude") else (float(raw["lat"]) if raw.get("lat") else None)
            lng = float(raw["longitude"]) if raw.get("longitude") else (float(raw["lon"]) if raw.get("lon") else None)
        except (ValueError, TypeError):
            lat, lng = None, None

        owner      = raw.get("applicant_name") or raw.get("owner") or ""
        issue_date = (raw.get("issue_date") or raw.get("permit_date") or "")[:10]

        return ProjectRecord(
            external_id      = f"la-{permit_id}",
            source_name      = self.source_name,
            source_url       = "https://www.ladbs.org/services/check-status/building-permit",
            title            = title[:500],
            description      = str(desc)[:2000] if desc else None,
            value_usd        = value,
            value_currency   = "USD",
            location_display = location[:500],
            location_country = "US",
            region           = "Americas",
            sector           = infer_sector(f"{title} {desc}"),
            stage            = "Awarded",
            timeline_display = issue_date,
            lat              = lat,
            lng              = lng,
            stakeholders     = [{"name": str(owner), "role": "Owner"}] if owner else [],
        )


# ═══════════════════════════════════════════════════════════════
# SCRAPER 4 — City of Austin Permits
# Austin TX — fastest growing US city, excellent Socrata API
# Dataset: Issued Construction Permits (3syk-w9eu)
# ═══════════════════════════════════════════════════════════════

class HoustonPermitScraper(BaseScraper):
    source_name = "Houston Permits"  # kept for runner compatibility

    DATASET_ID = "3syk-w9eu"
    BASE_URL   = "https://data.austintexas.gov"

    def fetch_raw(self) -> list[dict]:
        app_token = os.environ.get("SOCRATA_APP_TOKEN") or os.environ.get("NYC_APP_TOKEN", "")
        try:
            resp = requests.get(
                f"{self.BASE_URL}/resource/{self.DATASET_ID}.json",
                params={"$limit": 1000, "$$app_token": app_token},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            records = resp.json()

            filtered = [r for r in records if self._parse_cost(r) >= MIN_VALUE_USD]
            logger.info(f"[Austin Permits] Fetched {len(records)} raw, {len(filtered)} above ${MIN_VALUE_USD:,}")
            return filtered
        except requests.RequestException as e:
            logger.warning(f"[Austin Permits] Fetch failed: {e}")
            return []

    def _parse_cost(self, r):
        # Confirmed field name from Austin API: total_job_valuation
        for f in ("total_job_valuation", "total_valuation", "valuation", "declared_valuation",
                  "job_value", "estimated_cost"):
            v = r.get(f)
            if v:
                try:
                    return float(str(v).replace(",","").replace("$","").strip())
                except: pass
        return 0

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        permit_id = (raw.get("permit_number") or raw.get("permit_num") or raw.get("permitnumber") or "").strip()
        if not permit_id:
            return None

        value = self._parse_cost(raw)
        if value < MIN_VALUE_USD:
            return None

        permit_type = raw.get("permit_type_desc") or raw.get("permit_type") or ""
        desc        = raw.get("description_of_work") or raw.get("work_description") or raw.get("description") or ""
        title       = f"{permit_type}: {str(desc)[:120]}" if desc else permit_type or "Building Permit"

        address  = raw.get("address") or raw.get("site_address") or ""
        zip_code = raw.get("zip") or raw.get("zip_code") or ""
        location = f"{address}, Austin, TX {zip_code}".strip(", ")

        try:
            lat = float(raw["latitude"])  if raw.get("latitude")  else None
            lng = float(raw["longitude"]) if raw.get("longitude") else None
        except (ValueError, TypeError):
            lat, lng = None, None

        contractor = raw.get("contractor_company_name") or raw.get("contractor_name") or ""
        issue_date = (raw.get("issued_date") or raw.get("date_issued") or "")[:10]

        stakeholders = []
        if contractor:
            stakeholders.append({"name": contractor, "role": "Main Contractor"})

        return ProjectRecord(
            external_id      = f"aus-{permit_id}",
            source_name      = self.source_name,
            source_url       = "https://abc.austintexas.gov/",
            title            = title[:500],
            description      = str(desc)[:2000] if desc else None,
            value_usd        = value,
            value_currency   = "USD",
            location_display = location[:500],
            location_country = "US",
            region           = "Americas",
            sector           = infer_sector(f"{title} {desc}"),
            stage            = "Awarded",
            timeline_display = issue_date,
            lat              = lat,
            lng              = lng,
            stakeholders     = stakeholders,
        )


# ═══════════════════════════════════════════════════════════════
# SCRAPER 5 — City of Seattle Building Permits
# Active development scene, clean Socrata API
# Dataset: Building Permits (76t5-zqzr)
# ═══════════════════════════════════════════════════════════════

class PhiladelphiaPermitScraper(BaseScraper):
    source_name = "Seattle Permits"  # key kept for runner/UI compatibility

    DATASET_ID = "76t5-zqzr"
    BASE_URL   = "https://data.seattle.gov"

    def fetch_raw(self) -> list[dict]:
        app_token = os.environ.get("SOCRATA_APP_TOKEN") or os.environ.get("NYC_APP_TOKEN", "")
        try:
            resp = requests.get(
                f"{self.BASE_URL}/resource/{self.DATASET_ID}.json",
                params={"$limit": 1000, "$$app_token": app_token},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            records = resp.json()
            filtered = [r for r in records if self._parse_cost(r) >= MIN_VALUE_USD]
            logger.info(f"[Seattle Permits] Fetched {len(records)} raw, {len(filtered)} filtered")
            return filtered
        except requests.RequestException as e:
            logger.warning(f"[Seattle Permits] Fetch failed: {e}")
            return []

    def _parse_cost(self, r):
        for f in ("value", "estprojectcost", "declared_valuation", "permit_value"):
            v = r.get(f)
            if v:
                try: return float(str(v).replace(",","").replace("$",""))
                except: pass
        return 0

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        permit_id = (raw.get("permitnum") or raw.get("permit_number") or raw.get("application_permit_number") or "").strip()
        if not permit_id:
            return None

        value = self._parse_cost(raw)
        if value < MIN_VALUE_USD:
            return None

        category = raw.get("category") or raw.get("permit_type") or ""
        desc     = raw.get("description") or raw.get("work_type") or ""
        title    = f"{category}: {str(desc)[:120]}" if desc else category or "Building Permit"

        address  = raw.get("address") or raw.get("original_address1") or ""
        location = f"{address}, Seattle, WA".strip(", ")

        try:
            lat = float(raw["latitude"])  if raw.get("latitude")  else None
            lng = float(raw["longitude"]) if raw.get("longitude") else None
        except (ValueError, TypeError):
            lat, lng = None, None

        contractor = (raw.get("contractorcompanyname") or raw.get("contractorname") or raw.get("contractor") or "").strip()
        issue_date = (raw.get("issueddate") or raw.get("issue_date") or raw.get("applieddate") or "")[:10]

        status = (raw.get("statuscurrent") or raw.get("status") or "").lower()
        if any(s in status for s in ("complete", "cancelled", "expired", "withdrawn")):
            return None  # skip non-actionable
        stage  = "Awarded" if "issued" in status else "Planning"

        return ProjectRecord(
            external_id      = f"sea-{permit_id}",
            source_name      = self.source_name,
            source_url       = f"https://cosaccela.seattle.gov/portal/customize/LinkToRecord.aspx?altId={permit_id}",
            title            = title[:500],
            description      = str(desc)[:2000] if desc else None,
            value_usd        = value,
            value_currency   = "USD",
            location_display = location[:500],
            location_country = "US",
            region           = "Americas",
            sector           = infer_sector(f"{title} {desc}"),
            stage            = stage,
            timeline_display = issue_date,
            lat              = lat,
            lng              = lng,
            stakeholders     = [{"name": contractor, "role": "Main Contractor"}] if contractor else [],
        )


# ═══════════════════════════════════════════════════════════════
# SCRAPER 6 — US Army Corps of Engineers (USACE)
# Major US civil infrastructure — dams, levees, ports, waterways
# Free RSS/JSON feed from USACE public website
# ═══════════════════════════════════════════════════════════════

class USACEScraper(BaseScraper):
    """US Army Corps of Engineers civil infrastructure contracts via SAM.gov."""

    source_name = "USACE"

    # USACE Civil Works contracts via SAM.gov subset (construction only)
    # Uses a dedicated NAICS filter for heavy civil construction
    SAM_URL = "https://api.sam.gov/opportunities/v2/search"

    CIVIL_NAICS = [
        "237110",  # Water & Sewer Line Construction
        "237120",  # Oil & Gas Pipeline Construction
        "237310",  # Highway, Street, Bridge Construction
        "237990",  # Other Heavy Construction
        "236210",  # Industrial Building Construction
    ]

    def fetch_raw(self) -> list[dict]:
        import os
        api_key = os.environ.get("SAM_GOV_API_KEY", "")
        if not api_key:
            logger.warning("[USACE] SAM_GOV_API_KEY not set — skipping")
            return []

        results = []
        for naics in self.CIVIL_NAICS:
            try:
                resp = requests.get(
                    self.SAM_URL,
                    params={
                        "api_key":    api_key,
                        "naicsCode":  naics,
                        "limit":      100,
                        "offset":     0,
                        "postedFrom": "01/01/2025",
                        "postedTo":   datetime.now(timezone.utc).strftime("%m/%d/%Y"),
                        "deptname":   "DEPT OF DEFENSE",
                    },
                    timeout=30,
                )
                if resp.status_code == 429:
                    logger.warning(f"[USACE] Rate limited on NAICS {naics}")
                    break
                resp.raise_for_status()
                data = resp.json()
                batch = data.get("opportunitiesData", [])
                results.extend(batch)
                logger.info(f"[USACE] NAICS {naics}: {len(batch)} opportunities")
                time.sleep(1)
            except requests.RequestException as e:
                logger.warning(f"[USACE] NAICS {naics} failed: {e}")

        return results

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        notice_id = raw.get("noticeId") or raw.get("solicitationNumber", "")
        if not notice_id:
            return None

        title = (raw.get("title") or "").strip()
        if not title:
            return None

        # Location
        pop = raw.get("placeOfPerformance") or {}
        city    = pop.get("city",    {}).get("name", "")    if isinstance(pop, dict) else ""
        state   = pop.get("state",   {}).get("code", "")    if isinstance(pop, dict) else ""
        country = pop.get("country", {}).get("code", "US")  if isinstance(pop, dict) else "US"
        location = f"{city}, {state}".strip(", ") or "United States"

        # Org
        org_hier = raw.get("organizationHierarchy") or []
        agency   = org_hier[0].get("name", "USACE") if org_hier and isinstance(org_hier, list) else "USACE"

        posted = (raw.get("postedDate") or "")[:10]
        close  = (raw.get("responseDeadLine") or "")[:10]
        timeline = f"{posted} – {close}" if posted and close else posted

        notice_type = (raw.get("type") or "").lower()
        stage = "Tender" if "solicitation" in notice_type or "combine" in notice_type else "Planning"

        return ProjectRecord(
            external_id      = f"usace-{notice_id}",
            source_name      = self.source_name,
            source_url       = raw.get("uiLink") or f"https://sam.gov/opp/{notice_id}",
            title            = title[:500],
            description      = raw.get("description", "")[:2000] if raw.get("description") else None,
            value_usd        = None,
            value_currency   = "USD",
            location_display = location,
            location_country = country if len(country) == 2 else "US",
            region           = "Americas",
            sector           = infer_sector(title),
            stage            = stage,
            timeline_display = timeline,
            stakeholders     = [{"name": agency, "role": "Owner"}],
        )

# ═══════════════════════════════════════════════════════════════
# SCRAPER 7 — City of Philadelphia (restored)
# Uses eCLIPSE open data via ArcGIS REST API
# ═══════════════════════════════════════════════════════════════

class PhillyPermitScraper(BaseScraper):
    """Philadelphia L&I building permits via ArcGIS REST API."""

    source_name = "Philadelphia L&I"

    # ArcGIS REST endpoint — no auth required, public dataset
    BASE_URL = "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/li_permits/FeatureServer/0/query"

    def fetch_raw(self) -> list[dict]:
        try:
            resp = requests.get(
                self.BASE_URL,
                params={
                    "where":         "1=1",
                    "outFields":     "permitnumber,typeofwork,status,permitissuedate,address,zip,estprojectcost,contractorname,ownername,x,y",
                    "returnGeometry":"false",
                    "resultRecordCount": 500,
                    "orderByFields": "permitissuedate DESC",
                    "f":             "json",
                },
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            features = data.get("features", [])
            records = [f.get("attributes", {}) for f in features]
            filtered = [r for r in records if self._parse_cost(r) >= MIN_VALUE_USD]
            logger.info(f"[Philadelphia L&I] Fetched {len(records)} raw, {len(filtered)} filtered")
            return filtered
        except requests.RequestException as e:
            logger.warning(f"[Philadelphia L&I] Fetch failed: {e}")
            return []

    def _parse_cost(self, r):
        for f in ("estprojectcost", "estimated_cost"):
            v = r.get(f)
            if v:
                try: return float(str(v).replace(",","").replace("$",""))
                except: pass
        return 0

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        permit_id = (raw.get("permitnumber") or "").strip()
        if not permit_id:
            return None

        value = self._parse_cost(raw)
        if value < MIN_VALUE_USD:
            return None

        work_type  = raw.get("typeofwork") or ""
        type_label = "New Construction" if "NEW" in work_type.upper() else "Alteration/Repair"
        address    = raw.get("address") or ""
        title      = f"{type_label} - {address}" if address else type_label

        zip_code   = raw.get("zip") or ""
        location   = f"{address}, Philadelphia, PA {zip_code}".strip(", ")

        try:
            lat = float(raw["y"]) if raw.get("y") else None
            lng = float(raw["x"]) if raw.get("x") else None
        except (ValueError, TypeError):
            lat, lng = None, None

        status = (raw.get("status") or "").lower()
        stage  = "Awarded" if "issued" in status or "completed" in status else "Planning"

        stakeholders = []
        if raw.get("ownername"):
            stakeholders.append({"name": raw["ownername"], "role": "Owner"})
        if raw.get("contractorname"):
            stakeholders.append({"name": raw["contractorname"], "role": "Main Contractor"})

        issue_date = (raw.get("permitissuedate") or "")
        if isinstance(issue_date, (int, float)):
            from datetime import datetime, timezone
            issue_date = datetime.fromtimestamp(issue_date/1000, tz=timezone.utc).strftime("%Y-%m-%d")
        else:
            issue_date = str(issue_date)[:10]

        return ProjectRecord(
            external_id      = f"phl-{permit_id}",
            source_name      = self.source_name,
            source_url       = f"https://li.phila.gov/property-history/permit/{permit_id}",
            title            = title[:500],
            description      = None,
            value_usd        = value,
            value_currency   = "USD",
            location_display = location[:500],
            location_country = "US",
            region           = "Americas",
            sector           = infer_sector(title),
            stage            = stage,
            timeline_display = issue_date,
            lat              = lat,
            lng              = lng,
            stakeholders     = stakeholders,
        )


# ═══════════════════════════════════════════════════════════════
# SCRAPER 8 — City of Dallas Building Permits
# Texas commercial hub, clean Socrata API
# Dataset: Building Permits (e7gq-4sah)
# ═══════════════════════════════════════════════════════════════

class DenverPermitScraper(BaseScraper):
    """Dallas building permits via Socrata."""

    source_name = "Dallas Permits"

    DATASET_ID = "e7gq-4sah"
    BASE_URL   = "https://www.dallasopendata.com"

    def fetch_raw(self) -> list[dict]:
        app_token = os.environ.get("SOCRATA_APP_TOKEN") or os.environ.get("NYC_APP_TOKEN", "")
        try:
            resp = requests.get(
                f"{self.BASE_URL}/resource/{self.DATASET_ID}.json",
                params={"$limit": 1000, "$$app_token": app_token},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            records = resp.json()
            filtered = [r for r in records if self._parse_cost(r) >= MIN_VALUE_USD]
            logger.info(f"[Denver Permits] Fetched {len(records)} raw, {len(filtered)} filtered")
            return filtered
        except requests.RequestException as e:
            logger.warning(f"[Denver Permits] Fetch failed: {e}")
            return []

    def _parse_cost(self, r):
        for f in ("declared_valuation", "cost_of_construction", "estimated_cost", "total_cost", "job_cost", "cost"):
            v = r.get(f)
            if v:
                try: return float(str(v).replace(",","").replace("$",""))
                except: pass
        return 0

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        permit_id = (raw.get("permit_num") or raw.get("permitnum") or raw.get("permit_number") or "").strip()
        if not permit_id:
            return None

        value = self._parse_cost(raw)
        if value < MIN_VALUE_USD:
            return None

        work_type = raw.get("work_type") or raw.get("worktype") or raw.get("permit_type") or ""
        desc      = raw.get("description") or raw.get("work_desc") or raw.get("comments") or ""
        title     = f"{work_type}: {str(desc)[:120]}" if desc else work_type or "Building Permit"

        address  = raw.get("address") or raw.get("full_address") or raw.get("street_address") or ""
        zip_code = raw.get("zip_code") or raw.get("zip") or ""
        location = f"{address}, Dallas, TX {zip_code}".strip(", ")

        try:
            lat = float(raw["latitude"])  if raw.get("latitude")  else None
            lng = float(raw["longitude"]) if raw.get("longitude") else None
        except (ValueError, TypeError):
            lat, lng = None, None

        contractor = raw.get("contractor_name") or raw.get("contractorname") or raw.get("contractor") or ""
        issue_date = (raw.get("issue_date") or raw.get("issueddate") or raw.get("permit_date") or "")[:10]
        status     = (raw.get("status") or raw.get("statuscurrent") or "").lower()
        if any(s in status for s in ("complete", "cancelled", "expired", "withdrawn", "final")):
            return None  # skip non-actionable
        stage      = "Awarded" if "issued" in status or "approved" in status else "Planning"

        return ProjectRecord(
            external_id      = f"dal-{permit_id}",
            source_name      = self.source_name,
            source_url       = "https://www.dallasopendata.com/",
            title            = title[:500],
            description      = str(desc)[:2000] if desc else None,
            value_usd        = value,
            value_currency   = "USD",
            location_display = location[:500],
            location_country = "US",
            region           = "Americas",
            sector           = infer_sector(f"{title} {desc}"),
            stage            = stage,
            timeline_display = issue_date,
            lat              = lat,
            lng              = lng,
            stakeholders     = [{"name": contractor, "role": "Main Contractor"}] if contractor else [],
        )


# ═══════════════════════════════════════════════════════════════
# SCRAPER 9 — City of San Francisco Building Permits
# Most active tech/commercial construction in the US
# Dataset: Building Permits (i98e-djp9)
# ═══════════════════════════════════════════════════════════════

class SanFranciscoPermitScraper(BaseScraper):
    """San Francisco building permits via Socrata."""

    source_name = "SF Building Permits"

    DATASET_ID = "i98e-djp9"
    BASE_URL   = "https://data.sfgov.org"

    def fetch_raw(self) -> list[dict]:
        app_token = os.environ.get("SOCRATA_APP_TOKEN") or os.environ.get("NYC_APP_TOKEN", "")
        try:
            resp = requests.get(
                f"{self.BASE_URL}/resource/{self.DATASET_ID}.json",
                params={"$limit": 1000, "$$app_token": app_token},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            records = resp.json()
            filtered = [r for r in records if self._parse_cost(r) >= MIN_VALUE_USD]
            logger.info(f"[SF Building Permits] Fetched {len(records)} raw, {len(filtered)} filtered")
            return filtered
        except requests.RequestException as e:
            logger.warning(f"[SF Building Permits] Fetch failed: {e}")
            return []

    def _parse_cost(self, r):
        for f in ("estimated_cost", "revised_cost", "existing_cost"):
            v = r.get(f)
            if v:
                try: return float(str(v).replace(",","").replace("$",""))
                except: pass
        return 0

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        permit_id = (raw.get("permit_number") or raw.get("permitnumber") or "").strip()
        if not permit_id:
            return None

        value = self._parse_cost(raw)
        if value < MIN_VALUE_USD:
            return None

        permit_type = raw.get("permit_type_definition") or raw.get("permit_type") or ""
        desc        = raw.get("description") or raw.get("work_type") or ""
        title       = f"{permit_type}: {str(desc)[:120]}" if desc else permit_type or "Building Permit"

        street_num  = raw.get("street_number") or ""
        street_name = raw.get("street_name") or ""
        zip_code    = raw.get("zipcode") or raw.get("zip_code") or ""
        address     = f"{street_num} {street_name}".strip()
        location    = f"{address}, San Francisco, CA {zip_code}".strip(", ")

        try:
            lat = float(raw["latitude"])  if raw.get("latitude")  else None
            lng = float(raw["longitude"]) if raw.get("longitude") else None
        except (ValueError, TypeError):
            lat, lng = None, None

        status = (raw.get("status") or raw.get("current_status") or "").lower()
        if any(s in status for s in ("complete", "cancelled", "expired", "withdrawn", "revoked")):
            return None  # skip non-actionable
        stage  = "Awarded" if any(s in status for s in ("issued", "approved")) else "Planning"

        # SF dataset fields: contractor_company_name, applicant_name
        contractor = (raw.get("contractor_company_name") or raw.get("contractor") or "").strip()
        applicant  = (raw.get("applicant_name") or raw.get("applicant") or "").strip()
        issue_date = (raw.get("issued_date") or raw.get("filed_date") or "")[:10]

        stakeholders = []
        if contractor:
            stakeholders.append({"name": contractor, "role": "Main Contractor"})
        if applicant and applicant != contractor:
            stakeholders.append({"name": applicant, "role": "Owner"})

        return ProjectRecord(
            external_id      = f"sf-{permit_id}",
            source_name      = self.source_name,
            source_url       = "https://dbiweb02.sfgov.org/dbipts/default.aspx?Page=AddressPermits",
            title            = title[:500],
            description      = str(desc)[:2000] if desc else None,
            value_usd        = value,
            value_currency   = "USD",
            location_display = location[:500],
            location_country = "US",
            region           = "Americas",
            sector           = infer_sector(f"{title} {desc}"),
            stage            = stage,
            timeline_display = issue_date,
            lat              = lat,
            lng              = lng,
            stakeholders     = stakeholders,
        )

# ═══════════════════════════════════════════════════════════════
# SCRAPER 10 — City of Boston Building Permits
# Dense commercial market, strong healthcare/education/mixed-use
# Dataset: Building Permits (w9zt-3krx)
# ═══════════════════════════════════════════════════════════════

class BostonPermitScraper(BaseScraper):
    """Boston approved building permits via CKAN datastore API."""

    source_name = "Boston Permits"
    # Boston uses CKAN with datastore API (not classic Socrata)
    DATASTORE_URL = "https://data.boston.gov/api/3/action/datastore_search"
    RESOURCE_ID   = "6ddcd912-32a0-43df-9908-63574f8c7e77"

    def fetch_raw(self) -> list[dict]:
        try:
            resp = requests.get(
                self.DATASTORE_URL,
                params={"resource_id": self.RESOURCE_ID, "limit": 1000},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            records = data.get("result", {}).get("records", [])
            if records:
                logger.info(f"[Boston Permits] Sample fields: {list(records[0].keys())[:15]}")
            filtered = [r for r in records if self._parse_cost(r) >= MIN_VALUE_USD]
            logger.info(f"[Boston Permits] Fetched {len(records)} raw, {len(filtered)} filtered")
            return filtered
        except requests.RequestException as e:
            logger.warning(f"[Boston Permits] Fetch failed: {e}")
            return []

    def _parse_cost(self, r):
        for f in ("declared_valuation", "total_value", "estimated_value", "value", "cost"):
            v = r.get(f)
            if v:
                try: return float(str(v).replace(",","").replace("$",""))
                except: pass
        return 0

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        permit_id = (raw.get("permitnumber") or raw.get("permit_number") or raw.get("bl_id") or "").strip()
        if not permit_id:
            return None
        value = self._parse_cost(raw)
        if value < MIN_VALUE_USD:
            return None
        work_type = raw.get("description") or raw.get("worktype") or raw.get("permit_type") or ""
        desc      = raw.get("comments") or raw.get("work_description") or ""
        title     = f"{work_type}: {str(desc)[:120]}" if desc else work_type or "Building Permit"
        address   = raw.get("address") or raw.get("full_address") or ""
        zip_code  = raw.get("zip") or raw.get("zip_code") or ""
        location  = f"{address}, Boston, MA {zip_code}".strip(", ")
        try:
            lat = float(raw["latitude"])  if raw.get("latitude")  else None
            lng = float(raw["longitude"]) if raw.get("longitude") else None
        except (ValueError, TypeError):
            lat, lng = None, None
        contractor = raw.get("contractor_company") or raw.get("contractor") or raw.get("applicant") or ""
        issue_date = (raw.get("issued_date") or raw.get("issue_date") or raw.get("permitdate") or "")[:10]
        return ProjectRecord(
            external_id      = f"bos-{permit_id}",
            source_name      = self.source_name,
            source_url       = "https://www.boston.gov/departments/inspectional-services",
            title            = title[:500],
            description      = str(desc)[:2000] if desc else None,
            value_usd        = value,
            value_currency   = "USD",
            location_display = location[:500],
            location_country = "US",
            region           = "Americas",
            sector           = infer_sector(f"{title} {desc}"),
            stage            = "Awarded",
            timeline_display = issue_date,
            lat              = lat,
            lng              = lng,
            stakeholders     = [{"name": contractor, "role": "Main Contractor"}] if contractor else [],
        )


# ═══════════════════════════════════════════════════════════════
# SCRAPER 11 — City of San Jose Building Permits
# Silicon Valley — tech campus, commercial, large residential
# Dataset: Building Permits (bc2c-7vn5)
# ═══════════════════════════════════════════════════════════════

class SanJosePermitScraper(BaseScraper):
    """San Jose active building permits via CKAN datastore API."""

    source_name = "San Jose Permits"
    # San Jose uses CKAN datastore (active permits last 30 days)
    DATASTORE_URL = "https://data.sanjoseca.gov/api/3/action/datastore_search"
    RESOURCE_ID   = "045b3678-e923-4002-b696-300955bc6d06"

    def fetch_raw(self) -> list[dict]:
        try:
            resp = requests.get(
                self.DATASTORE_URL,
                params={"resource_id": self.RESOURCE_ID, "limit": 1000},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            records = data.get("result", {}).get("records", [])
            if records:
                logger.info(f"[San Jose Permits] Sample fields: {list(records[0].keys())[:15]}")
            filtered = [r for r in records if self._parse_cost(r) >= MIN_VALUE_USD]
            logger.info(f"[San Jose Permits] Fetched {len(records)} raw, {len(filtered)} filtered")
            return filtered
        except requests.RequestException as e:
            logger.warning(f"[San Jose Permits] Fetch failed: {e}")
            return []

    def _parse_cost(self, r):
        # San Jose field names are uppercase
        for f in ("VALUATION", "JOB_VALUE", "DECLARED_VALUATION", "valuation", "job_value"):
            v = r.get(f)
            if v:
                try: return float(str(v).replace(",","").replace("$",""))
                except: pass
        return 0

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        # Confirmed field names from API: FOLDERNUMBER, WORKDESCRIPTION, ISSUEDATE, CONTRACTOR
        permit_id = (raw.get("FOLDERNUMBER") or raw.get("permit_number") or raw.get("permitnumber") or "").strip()
        if not permit_id:
            return None
        # San Jose has no valuation field — include all permits above $0 and use description to infer value
        # Use permit type to filter out minor permits
        permit_type = raw.get("SUBTYPEDESCRIPTION") or raw.get("FOLDERDESC") or raw.get("FOLDERNAME") or ""
        desc        = raw.get("WORKDESCRIPTION") or raw.get("description") or ""
        # Only include commercial/large construction types
        if not any(k in (permit_type + desc).upper() for k in (
            "COMMERCIAL", "NEW CONSTRUCTION", "ADDITION", "INDUSTRIAL",
            "MULTI", "APARTMENT", "OFFICE", "RETAIL", "HOTEL", "MIXED"
        )):
            return None
        value = None  # San Jose doesn't publish valuations
        title = f"{permit_type}: {str(desc)[:120]}" if desc else permit_type or "Building Permit"
        address  = raw.get("address") or ""
        location = f"{address}, San Jose, CA".strip(", ")
        lat, lng = None, None
        contractor = raw.get("CONTRACTOR") or raw.get("APPLICANT") or ""
        issue_date = (raw.get("ISSUEDATE") or raw.get("issue_date") or "")[:10]
        status     = (raw.get("Status") or raw.get("status") or "").lower()
        stage      = "Awarded" if any(s in status for s in ("issued", "approved", "final")) else "Planning"
        return ProjectRecord(
            external_id      = f"sj-{permit_id}",
            source_name      = self.source_name,
            source_url       = "https://www.sjpermits.org/",
            title            = title[:500],
            description      = str(desc)[:2000] if desc else None,
            value_usd        = value,
            value_currency   = "USD",
            location_display = location[:500],
            location_country = "US",
            region           = "Americas",
            sector           = infer_sector(f"{title} {desc}"),
            stage            = stage,
            timeline_display = issue_date,
            lat              = lat,
            lng              = lng,
            stakeholders     = [{"name": contractor, "role": "Main Contractor"}] if contractor else [],
        )


# ═══════════════════════════════════════════════════════════════
# SCRAPER 12 — City of Baltimore Building Permits
# Active urban development, strong healthcare/education
# Dataset: Building Permits (fesm-tgxf)
# ═══════════════════════════════════════════════════════════════

class BaltimorePermitScraper(BaseScraper):
    """Baltimore building permits via Socrata."""

    source_name = "Baltimore Permits"
    DATASET_ID  = "y8pj-nxgx"  # Building Permits (Baltimore)
    BASE_URL    = "https://data.baltimorecity.gov"

    def fetch_raw(self) -> list[dict]:
        app_token = os.environ.get("SOCRATA_APP_TOKEN") or os.environ.get("NYC_APP_TOKEN", "")
        try:
            resp = requests.get(
                f"{self.BASE_URL}/resource/{self.DATASET_ID}.json",
                params={"$limit": 1000, "$$app_token": app_token},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            records = resp.json()
            if records:
                logger.info(f"[Baltimore Permits] Sample fields: {list(records[0].keys())[:15]}")
            filtered = [r for r in records if self._parse_cost(r) >= MIN_VALUE_USD]
            logger.info(f"[Baltimore Permits] Fetched {len(records)} raw, {len(filtered)} filtered")
            return filtered
        except requests.RequestException as e:
            logger.warning(f"[Baltimore Permits] Fetch failed: {e}")
            return []

    def _parse_cost(self, r):
        for f in ("job_value", "declared_valuation", "estimated_cost", "cost_of_construction", "value"):
            v = r.get(f)
            if v:
                try: return float(str(v).replace(",","").replace("$",""))
                except: pass
        return 0

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        permit_id = (raw.get("permitnumber") or raw.get("permit_number") or raw.get("bl_id") or "").strip()
        if not permit_id:
            return None
        value = self._parse_cost(raw)
        if value < MIN_VALUE_USD:
            return None
        permit_type = raw.get("permittype") or raw.get("permit_type") or ""
        desc        = raw.get("description") or raw.get("work_type") or ""
        title       = f"{permit_type}: {str(desc)[:120]}" if desc else permit_type or "Building Permit"
        address     = raw.get("address") or raw.get("fulladdress") or ""
        zip_code    = raw.get("zip") or raw.get("zip_code") or ""
        location    = f"{address}, Baltimore, MD {zip_code}".strip(", ")
        try:
            lat = float(raw["latitude"])  if raw.get("latitude")  else None
            lng = float(raw["longitude"]) if raw.get("longitude") else None
        except (ValueError, TypeError):
            lat, lng = None, None
        contractor = raw.get("contractor_name") or raw.get("contractorname") or raw.get("contractor") or ""
        issue_date = (raw.get("issued_date") or raw.get("issue_date") or raw.get("permitissueddate") or "")[:10]
        status     = (raw.get("status") or raw.get("statusdate") or "").lower()
        if any(s in status for s in ("complete", "cancelled", "expired", "withdrawn")):
            return None  # skip non-actionable
        stage      = "Awarded" if any(s in status for s in ("issued", "approved")) else "Planning"
        return ProjectRecord(
            external_id      = f"bal-{permit_id}",
            source_name      = self.source_name,
            source_url       = "https://permits.baltimorecity.gov/",
            title            = title[:500],
            description      = str(desc)[:2000] if desc else None,
            value_usd        = value,
            value_currency   = "USD",
            location_display = location[:500],
            location_country = "US",
            region           = "Americas",
            sector           = infer_sector(f"{title} {desc}"),
            stage            = stage,
            timeline_display = issue_date,
            lat              = lat,
            lng              = lng,
            stakeholders     = [{"name": contractor, "role": "Main Contractor"}] if contractor else [],
        )


# ═══════════════════════════════════════════════════════════════
# SCRAPER 13 — City of Nashville Building Permits
# Fast-growing sunbelt city, strong commercial/mixed-use
# Dataset: Building Permits (3h5w-q8b7)
# ═══════════════════════════════════════════════════════════════

class NashvillePermitScraper(BaseScraper):
    """Nashville building permits via Socrata."""

    source_name = "Nashville Permits"
    DATASET_ID  = "3h5w-q8b7"
    BASE_URL    = "https://data.nashville.gov"

    def fetch_raw(self) -> list[dict]:
        app_token = os.environ.get("SOCRATA_APP_TOKEN", "")
        try:
            resp = requests.get(
                f"{self.BASE_URL}/resource/{self.DATASET_ID}.json",
                params={"$limit": 1000, "$$app_token": app_token},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            records = resp.json()
            if records:
                logger.info(f"[Nashville Permits] Sample fields: {list(records[0].keys())[:15]}")
            filtered = [r for r in records if self._parse_cost(r) >= MIN_VALUE_USD]
            logger.info(f"[Nashville Permits] Fetched {len(records)} raw, {len(filtered)} filtered")
            return filtered
        except requests.RequestException as e:
            logger.warning(f"[Nashville Permits] Fetch failed: {e}")
            return []

    def _parse_cost(self, r):
        for f in ("const_cost", "construction_cost", "estimated_cost", "job_value", "cost"):
            v = r.get(f)
            if v:
                try: return float(str(v).replace(",", "").replace("$", ""))
                except: pass
        return 0

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        permit_id = (raw.get("permit_number") or raw.get("permitnumber") or raw.get("permitnum") or "").strip()
        if not permit_id:
            return None
        value = self._parse_cost(raw)
        if value < MIN_VALUE_USD:
            return None
        permit_type = raw.get("permit_type_description") or raw.get("permit_type") or raw.get("permittype") or ""
        desc        = raw.get("description") or raw.get("work_description") or ""
        title       = f"{permit_type}: {str(desc)[:120]}" if desc else permit_type or "Building Permit"
        address     = raw.get("address") or raw.get("location_address") or ""
        zip_code    = raw.get("zip_code") or raw.get("zip") or ""
        location    = f"{address}, Nashville, TN {zip_code}".strip(", ")
        try:
            lat = float(raw["latitude"])  if raw.get("latitude")  else None
            lng = float(raw["longitude"]) if raw.get("longitude") else None
        except (ValueError, TypeError):
            lat, lng = None, None
        status = (raw.get("status") or raw.get("permit_status") or "").lower()
        if any(s in status for s in ("final", "complete", "cancelled", "expired", "withdrawn")):
            return None
        stage      = "Awarded" if "issued" in status or "approved" in status else "Planning"
        contractor = (raw.get("contractor_name") or raw.get("contractor") or raw.get("applicant_name") or "").strip()
        owner      = (raw.get("owner_name") or raw.get("property_owner") or "").strip()
        issue_date = (raw.get("date_issued") or raw.get("issue_date") or raw.get("issued_date") or "")[:10]
        stakeholders = []
        if owner: stakeholders.append({"name": owner, "role": "Owner"})
        if contractor and contractor != owner: stakeholders.append({"name": contractor, "role": "Main Contractor"})
        return ProjectRecord(
            external_id      = f"nas-{permit_id}",
            source_name      = self.source_name,
            source_url       = f"https://permits.nashville.gov/",
            title            = title[:500],
            description      = str(desc)[:2000] if desc else None,
            value_usd        = value,
            value_currency   = "USD",
            location_display = location[:500],
            location_country = "US",
            region           = "Americas",
            sector           = infer_sector(f"{title} {desc}"),
            stage            = stage,
            timeline_display = issue_date,
            lat              = lat,
            lng              = lng,
            stakeholders     = stakeholders,
        )


# ═══════════════════════════════════════════════════════════════
# SCRAPER 14 — City of New Orleans Building Permits
# Growing Gulf Coast market, major post-Katrina redevelopment
# Dataset: Permits BLDS (72f9-bi28)
# ═══════════════════════════════════════════════════════════════

class NewOrleansPermitScraper(BaseScraper):
    """New Orleans building permits via Socrata."""

    source_name = "New Orleans Permits"
    DATASET_ID  = "72f9-bi28"
    BASE_URL    = "https://data.nola.gov"

    def fetch_raw(self) -> list[dict]:
        app_token = os.environ.get("SOCRATA_APP_TOKEN", "")
        try:
            resp = requests.get(
                f"{self.BASE_URL}/resource/{self.DATASET_ID}.json",
                params={"$limit": 1000, "$$app_token": app_token},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            records = resp.json()
            if records:
                logger.info(f"[New Orleans Permits] Sample fields: {list(records[0].keys())[:15]}")
            filtered = [r for r in records if self._parse_cost(r) >= MIN_VALUE_USD]
            logger.info(f"[New Orleans Permits] Fetched {len(records)} raw, {len(filtered)} filtered")
            return filtered
        except requests.RequestException as e:
            logger.warning(f"[New Orleans Permits] Fetch failed: {e}")
            return []

    def _parse_cost(self, r):
        for f in ("estimated_cost", "declared_valuation", "job_value", "cost", "value"):
            v = r.get(f)
            if v:
                try: return float(str(v).replace(",", "").replace("$", ""))
                except: pass
        return 0

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        permit_id = (raw.get("permit_number") or raw.get("permitnumber") or raw.get("id") or "").strip()
        if not permit_id:
            return None
        value = self._parse_cost(raw)
        if value < MIN_VALUE_USD:
            return None
        permit_type = raw.get("permit_type") or raw.get("work_type") or raw.get("permittype") or ""
        desc        = raw.get("description") or raw.get("work_description") or ""
        title       = f"{permit_type}: {str(desc)[:120]}" if desc else permit_type or "Building Permit"
        address     = raw.get("address") or raw.get("location") or ""
        zip_code    = raw.get("zip") or raw.get("zip_code") or ""
        location    = f"{address}, New Orleans, LA {zip_code}".strip(", ")
        try:
            lat = float(raw["latitude"])  if raw.get("latitude")  else None
            lng = float(raw["longitude"]) if raw.get("longitude") else None
        except (ValueError, TypeError):
            lat, lng = None, None
        status = (raw.get("status") or raw.get("permit_status") or "").lower()
        if any(s in status for s in ("final", "complete", "cancelled", "expired", "withdrawn", "closed")):
            return None
        stage      = "Awarded" if "issued" in status or "approved" in status else "Planning"
        contractor = (raw.get("contractor_name") or raw.get("contractor") or "").strip()
        owner      = (raw.get("owner_name") or raw.get("applicant") or "").strip()
        issue_date = (raw.get("date_issued") or raw.get("issue_date") or raw.get("issued_date") or "")[:10]
        stakeholders = []
        if owner: stakeholders.append({"name": owner, "role": "Owner"})
        if contractor and contractor != owner: stakeholders.append({"name": contractor, "role": "Main Contractor"})
        return ProjectRecord(
            external_id      = f"nola-{permit_id}",
            source_name      = self.source_name,
            source_url       = "https://data.nola.gov/Housing-Land-Use-and-Blight/Permits-BLDS/72f9-bi28",
            title            = title[:500],
            description      = str(desc)[:2000] if desc else None,
            value_usd        = value,
            value_currency   = "USD",
            location_display = location[:500],
            location_country = "US",
            region           = "Americas",
            sector           = infer_sector(f"{title} {desc}"),
            stage            = stage,
            timeline_display = issue_date,
            lat              = lat,
            lng              = lng,
            stakeholders     = stakeholders,
        )


# ═══════════════════════════════════════════════════════════════
# SCRAPER 15 — City of Las Vegas Building Permits
# Major resort/commercial construction hub
# Dataset: Building Permits (wpyf-qpia)
# ═══════════════════════════════════════════════════════════════

class LasVegasPermitScraper(BaseScraper):
    """Las Vegas building permits via Socrata."""

    source_name = "Las Vegas Permits"
    DATASET_ID  = "wpyf-qpia"
    BASE_URL    = "https://opendata.lasvegasnevada.gov"

    def fetch_raw(self) -> list[dict]:
        app_token = os.environ.get("SOCRATA_APP_TOKEN", "")
        try:
            resp = requests.get(
                f"{self.BASE_URL}/resource/{self.DATASET_ID}.json",
                params={"$limit": 1000, "$$app_token": app_token},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            records = resp.json()
            if records:
                logger.info(f"[Las Vegas Permits] Sample fields: {list(records[0].keys())[:15]}")
            filtered = [r for r in records if self._parse_cost(r) >= MIN_VALUE_USD]
            logger.info(f"[Las Vegas Permits] Fetched {len(records)} raw, {len(filtered)} filtered")
            return filtered
        except requests.RequestException as e:
            logger.warning(f"[Las Vegas Permits] Fetch failed: {e}")
            return []

    def _parse_cost(self, r):
        for f in ("declared_valuation", "estimated_cost", "job_value", "valuation", "cost"):
            v = r.get(f)
            if v:
                try: return float(str(v).replace(",", "").replace("$", ""))
                except: pass
        return 0

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        permit_id = (raw.get("permit_number") or raw.get("permitnumber") or raw.get("permit_num") or "").strip()
        if not permit_id:
            return None
        value = self._parse_cost(raw)
        if value < MIN_VALUE_USD:
            return None
        permit_type = raw.get("permit_type") or raw.get("work_type") or ""
        desc        = raw.get("description") or raw.get("work_description") or ""
        title       = f"{permit_type}: {str(desc)[:120]}" if desc else permit_type or "Building Permit"
        address     = raw.get("address") or raw.get("site_address") or ""
        zip_code    = raw.get("zip_code") or raw.get("zip") or ""
        location    = f"{address}, Las Vegas, NV {zip_code}".strip(", ")
        try:
            lat = float(raw["latitude"])  if raw.get("latitude")  else None
            lng = float(raw["longitude"]) if raw.get("longitude") else None
        except (ValueError, TypeError):
            lat, lng = None, None
        status = (raw.get("status") or raw.get("permit_status") or "").lower()
        if any(s in status for s in ("final", "complete", "cancelled", "expired", "withdrawn")):
            return None
        stage      = "Awarded" if "issued" in status or "approved" in status else "Planning"
        contractor = (raw.get("contractor_name") or raw.get("contractor") or raw.get("license_holder") or "").strip()
        owner      = (raw.get("owner_name") or raw.get("applicant_name") or "").strip()
        issue_date = (raw.get("issue_date") or raw.get("date_issued") or raw.get("issued_date") or "")[:10]
        stakeholders = []
        if owner: stakeholders.append({"name": owner, "role": "Owner"})
        if contractor and contractor != owner: stakeholders.append({"name": contractor, "role": "Main Contractor"})
        return ProjectRecord(
            external_id      = f"lv-{permit_id}",
            source_name      = self.source_name,
            source_url       = "https://opendata.lasvegasnevada.gov/Building-and-Safety/Building-Permits/wpyf-qpia",
            title            = title[:500],
            description      = str(desc)[:2000] if desc else None,
            value_usd        = value,
            value_currency   = "USD",
            location_display = location[:500],
            location_country = "US",
            region           = "Americas",
            sector           = infer_sector(f"{title} {desc}"),
            stage            = stage,
            timeline_display = issue_date,
            lat              = lat,
            lng              = lng,
            stakeholders     = stakeholders,
        )


# ═══════════════════════════════════════════════════════════════
# SCRAPER 16 — City of Raleigh Building Permits
# Fast-growing Research Triangle, strong tech/commercial
# Dataset: Building Permits with Parcel ID (b9nv-68kk)
# ═══════════════════════════════════════════════════════════════

class RaleighPermitScraper(BaseScraper):
    """Raleigh building permits via Socrata."""

    source_name = "Raleigh Permits"
    DATASET_ID  = "b9nv-68kk"
    BASE_URL    = "https://data.raleighnc.gov"

    def fetch_raw(self) -> list[dict]:
        app_token = os.environ.get("SOCRATA_APP_TOKEN", "")
        try:
            resp = requests.get(
                f"{self.BASE_URL}/resource/{self.DATASET_ID}.json",
                params={"$limit": 1000, "$$app_token": app_token},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            records = resp.json()
            if records:
                logger.info(f"[Raleigh Permits] Sample fields: {list(records[0].keys())[:15]}")
            filtered = [r for r in records if self._parse_cost(r) >= MIN_VALUE_USD]
            logger.info(f"[Raleigh Permits] Fetched {len(records)} raw, {len(filtered)} filtered")
            return filtered
        except requests.RequestException as e:
            logger.warning(f"[Raleigh Permits] Fetch failed: {e}")
            return []

    def _parse_cost(self, r):
        for f in ("job_value", "declared_valuation", "estimated_cost", "cost", "value"):
            v = r.get(f)
            if v:
                try: return float(str(v).replace(",", "").replace("$", ""))
                except: pass
        return 0

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        permit_id = (raw.get("objectid") or raw.get("permit_number") or raw.get("permitnum") or "").strip()
        if not permit_id:
            return None
        value = self._parse_cost(raw)
        if value < MIN_VALUE_USD:
            return None
        permit_type = raw.get("work_class") or raw.get("permit_type") or raw.get("permittype") or ""
        desc        = raw.get("description") or raw.get("work_description") or ""
        title       = f"{permit_type}: {str(desc)[:120]}" if desc else permit_type or "Building Permit"
        address     = raw.get("address") or raw.get("site_address") or ""
        zip_code    = raw.get("zip") or raw.get("zip_code") or ""
        location    = f"{address}, Raleigh, NC {zip_code}".strip(", ")
        try:
            lat = float(raw["latitude"])  if raw.get("latitude")  else None
            lng = float(raw["longitude"]) if raw.get("longitude") else None
        except (ValueError, TypeError):
            lat, lng = None, None
        status = (raw.get("status") or raw.get("permit_status") or "").lower()
        if any(s in status for s in ("final", "complete", "cancelled", "expired", "withdrawn")):
            return None
        stage      = "Awarded" if "issued" in status or "approved" in status else "Planning"
        contractor = (raw.get("contractor_name") or raw.get("contractor") or "").strip()
        owner      = (raw.get("owner") or raw.get("applicant_name") or "").strip()
        issue_date = (raw.get("issued_date") or raw.get("issue_date") or raw.get("date_issued") or "")[:10]
        stakeholders = []
        if owner: stakeholders.append({"name": owner, "role": "Owner"})
        if contractor and contractor != owner: stakeholders.append({"name": contractor, "role": "Main Contractor"})
        return ProjectRecord(
            external_id      = f"ral-{permit_id}",
            source_name      = self.source_name,
            source_url       = "https://data.raleighnc.gov/Permits/Building-Permits-with-Parcel-ID/b9nv-68kk",
            title            = title[:500],
            description      = str(desc)[:2000] if desc else None,
            value_usd        = value,
            value_currency   = "USD",
            location_display = location[:500],
            location_country = "US",
            region           = "Americas",
            sector           = infer_sector(f"{title} {desc}"),
            stage            = stage,
            timeline_display = issue_date,
            lat              = lat,
            lng              = lng,
            stakeholders     = stakeholders,
        )

# ═══════════════════════════════════════════════════════════════
# SCRAPER 13 — City of Nashville Building Permits
# Fast-growing city, strong commercial/mixed-use pipeline
# Dataset: Building Permits (3h5w-q8b7)
# ═══════════════════════════════════════════════════════════════

class NashvillePermitScraper(BaseScraper):
    """Nashville building permits via Socrata."""

    source_name = "Nashville Permits"
    DATASET_ID  = "3h5w-q8b7"
    BASE_URL    = "https://data.nashville.gov"

    def fetch_raw(self) -> list[dict]:
        app_token = os.environ.get("SOCRATA_APP_TOKEN") or os.environ.get("NYC_APP_TOKEN", "")
        try:
            resp = requests.get(
                f"{self.BASE_URL}/resource/{self.DATASET_ID}.json",
                params={"$limit": 1000, "$$app_token": app_token},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            records = resp.json()
            if records:
                logger.info(f"[Nashville Permits] Sample fields: {list(records[0].keys())[:15]}")
            filtered = [r for r in records if self._parse_cost(r) >= MIN_VALUE_USD]
            logger.info(f"[Nashville Permits] Fetched {len(records)} raw, {len(filtered)} filtered")
            return filtered
        except requests.RequestException as e:
            logger.warning(f"[Nashville Permits] Fetch failed: {e}")
            return []

    def _parse_cost(self, r):
        for f in ("const_cost", "const_value", "estimated_cost", "job_value", "cost"):
            v = r.get(f)
            if v:
                try: return float(str(v).replace(",","").replace("$",""))
                except: pass
        return 0

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        permit_id = (raw.get("permit_number") or raw.get("permitnumber") or raw.get("permit_num") or "").strip()
        if not permit_id:
            return None
        value = self._parse_cost(raw)
        if value < MIN_VALUE_USD:
            return None
        permit_type = raw.get("permit_type_description") or raw.get("permit_type") or raw.get("type") or ""
        desc        = raw.get("description") or raw.get("work_description") or ""
        title       = f"{permit_type}: {str(desc)[:120]}" if desc else permit_type or "Building Permit"
        address     = raw.get("address") or raw.get("location_address") or ""
        zip_code    = raw.get("zip_code") or raw.get("zip") or ""
        location    = f"{address}, Nashville, TN {zip_code}".strip(", ")
        try:
            lat = float(raw["latitude"])  if raw.get("latitude")  else None
            lng = float(raw["longitude"]) if raw.get("longitude") else None
        except (ValueError, TypeError):
            lat, lng = None, None
        status = (raw.get("status") or raw.get("permit_status") or "").lower()
        if any(s in status for s in ("finaled", "cancelled", "expired", "withdrawn", "complete")):
            return None
        stage      = "Awarded" if "issued" in status or "approved" in status else "Planning"
        contractor = (raw.get("contractor_name") or raw.get("contractor") or "").strip()
        applicant  = (raw.get("applicant_name") or raw.get("applicant") or "").strip()
        issue_date = (raw.get("issue_date") or raw.get("issued_date") or "")[:10]
        stakeholders = []
        if contractor:
            stakeholders.append({"name": contractor, "role": "Main Contractor"})
        if applicant and applicant != contractor:
            stakeholders.append({"name": applicant, "role": "Owner"})
        return ProjectRecord(
            external_id      = f"nas-{permit_id}",
            source_name      = self.source_name,
            source_url       = f"https://data.nashville.gov/resource/{self.DATASET_ID}",
            title            = title[:500],
            description      = str(desc)[:2000] if desc else None,
            value_usd        = value,
            value_currency   = "USD",
            location_display = location[:500],
            location_country = "US",
            region           = "Americas",
            sector           = infer_sector(f"{title} {desc}"),
            stage            = stage,
            timeline_display = issue_date,
            lat              = lat,
            lng              = lng,
            stakeholders     = stakeholders,
        )


# ═══════════════════════════════════════════════════════════════
# SCRAPER 14 — City of New Orleans Building Permits
# Growing Gulf Coast market, strong hospitality/mixed-use
# Dataset: Permits BLDS (72f9-bi28)
# ═══════════════════════════════════════════════════════════════

class NewOrleansPermitScraper(BaseScraper):
    """New Orleans building permits via Socrata."""

    source_name = "New Orleans Permits"
    DATASET_ID  = "72f9-bi28"
    BASE_URL    = "https://data.nola.gov"

    def fetch_raw(self) -> list[dict]:
        app_token = os.environ.get("SOCRATA_APP_TOKEN") or os.environ.get("NYC_APP_TOKEN", "")
        try:
            resp = requests.get(
                f"{self.BASE_URL}/resource/{self.DATASET_ID}.json",
                params={"$limit": 1000, "$$app_token": app_token},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            records = resp.json()
            if records:
                logger.info(f"[New Orleans Permits] Sample fields: {list(records[0].keys())[:15]}")
            filtered = [r for r in records if self._parse_cost(r) >= MIN_VALUE_USD]
            logger.info(f"[New Orleans Permits] Fetched {len(records)} raw, {len(filtered)} filtered")
            return filtered
        except requests.RequestException as e:
            logger.warning(f"[New Orleans Permits] Fetch failed: {e}")
            return []

    def _parse_cost(self, r):
        for f in ("estimated_cost", "declared_valuation", "const_cost", "job_value", "cost"):
            v = r.get(f)
            if v:
                try: return float(str(v).replace(",","").replace("$",""))
                except: pass
        return 0

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        permit_id = (raw.get("permit_number") or raw.get("permitnumber") or raw.get("permit_no") or "").strip()
        if not permit_id:
            return None
        value = self._parse_cost(raw)
        if value < MIN_VALUE_USD:
            return None
        permit_type = raw.get("permit_type") or raw.get("type") or ""
        desc        = raw.get("description") or raw.get("work_type") or ""
        title       = f"{permit_type}: {str(desc)[:120]}" if desc else permit_type or "Building Permit"
        address     = raw.get("address") or raw.get("location_address") or ""
        zip_code    = raw.get("zip_code") or raw.get("zip") or ""
        location    = f"{address}, New Orleans, LA {zip_code}".strip(", ")
        try:
            lat = float(raw["latitude"])  if raw.get("latitude")  else None
            lng = float(raw["longitude"]) if raw.get("longitude") else None
        except (ValueError, TypeError):
            lat, lng = None, None
        status = (raw.get("status") or raw.get("permit_status") or "").lower()
        if any(s in status for s in ("finaled", "cancelled", "expired", "withdrawn", "complete", "closed")):
            return None
        stage      = "Awarded" if "issued" in status or "approved" in status else "Planning"
        contractor = (raw.get("contractor_name") or raw.get("contractor") or "").strip()
        applicant  = (raw.get("owner_name") or raw.get("applicant") or "").strip()
        issue_date = (raw.get("issue_date") or raw.get("issued_date") or "")[:10]
        stakeholders = []
        if contractor:
            stakeholders.append({"name": contractor, "role": "Main Contractor"})
        if applicant and applicant != contractor:
            stakeholders.append({"name": applicant, "role": "Owner"})
        return ProjectRecord(
            external_id      = f"nol-{permit_id}",
            source_name      = self.source_name,
            source_url       = f"https://data.nola.gov/resource/{self.DATASET_ID}",
            title            = title[:500],
            description      = str(desc)[:2000] if desc else None,
            value_usd        = value,
            value_currency   = "USD",
            location_display = location[:500],
            location_country = "US",
            region           = "Americas",
            sector           = infer_sector(f"{title} {desc}"),
            stage            = stage,
            timeline_display = issue_date,
            lat              = lat,
            lng              = lng,
            stakeholders     = stakeholders,
        )


# ═══════════════════════════════════════════════════════════════
# SCRAPER 15 — City of Las Vegas Building Permits
# Major commercial/hospitality construction hub
# Dataset: Building Permits (wpyf-qpia)
# ═══════════════════════════════════════════════════════════════

class LasVegasPermitScraper(BaseScraper):
    """Las Vegas building permits via Socrata."""

    source_name = "Las Vegas Permits"
    DATASET_ID  = "wpyf-qpia"
    BASE_URL    = "https://opendata.lasvegasnevada.gov"

    def fetch_raw(self) -> list[dict]:
        app_token = os.environ.get("SOCRATA_APP_TOKEN") or os.environ.get("NYC_APP_TOKEN", "")
        try:
            resp = requests.get(
                f"{self.BASE_URL}/resource/{self.DATASET_ID}.json",
                params={"$limit": 1000, "$$app_token": app_token},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            records = resp.json()
            if records:
                logger.info(f"[Las Vegas Permits] Sample fields: {list(records[0].keys())[:15]}")
            filtered = [r for r in records if self._parse_cost(r) >= MIN_VALUE_USD]
            logger.info(f"[Las Vegas Permits] Fetched {len(records)} raw, {len(filtered)} filtered")
            return filtered
        except requests.RequestException as e:
            logger.warning(f"[Las Vegas Permits] Fetch failed: {e}")
            return []

    def _parse_cost(self, r):
        for f in ("declared_valuation", "estimated_cost", "job_value", "valuation", "cost"):
            v = r.get(f)
            if v:
                try: return float(str(v).replace(",","").replace("$",""))
                except: pass
        return 0

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        permit_id = (raw.get("permit_number") or raw.get("permitnumber") or raw.get("permit_num") or "").strip()
        if not permit_id:
            return None
        value = self._parse_cost(raw)
        if value < MIN_VALUE_USD:
            return None
        permit_type = raw.get("permit_type") or raw.get("type_of_work") or ""
        desc        = raw.get("description") or raw.get("work_description") or ""
        title       = f"{permit_type}: {str(desc)[:120]}" if desc else permit_type or "Building Permit"
        address     = raw.get("address") or raw.get("location_address") or ""
        zip_code    = raw.get("zip_code") or raw.get("zip") or ""
        location    = f"{address}, Las Vegas, NV {zip_code}".strip(", ")
        try:
            lat = float(raw["latitude"])  if raw.get("latitude")  else None
            lng = float(raw["longitude"]) if raw.get("longitude") else None
        except (ValueError, TypeError):
            lat, lng = None, None
        status = (raw.get("status") or raw.get("permit_status") or "").lower()
        if any(s in status for s in ("finaled", "cancelled", "expired", "withdrawn", "complete", "closed")):
            return None
        stage      = "Awarded" if "issued" in status or "approved" in status else "Planning"
        contractor = (raw.get("contractor_name") or raw.get("contractor") or "").strip()
        applicant  = (raw.get("owner_name") or raw.get("applicant") or "").strip()
        issue_date = (raw.get("issue_date") or raw.get("issued_date") or "")[:10]
        stakeholders = []
        if contractor:
            stakeholders.append({"name": contractor, "role": "Main Contractor"})
        if applicant and applicant != contractor:
            stakeholders.append({"name": applicant, "role": "Owner"})
        return ProjectRecord(
            external_id      = f"lvg-{permit_id}",
            source_name      = self.source_name,
            source_url       = f"https://opendata.lasvegasnevada.gov/resource/{self.DATASET_ID}",
            title            = title[:500],
            description      = str(desc)[:2000] if desc else None,
            value_usd        = value,
            value_currency   = "USD",
            location_display = location[:500],
            location_country = "US",
            region           = "Americas",
            sector           = infer_sector(f"{title} {desc}"),
            stage            = stage,
            timeline_display = issue_date,
            lat              = lat,
            lng              = lng,
            stakeholders     = stakeholders,
        )


# ═══════════════════════════════════════════════════════════════
# SCRAPER 16 — City of Raleigh Building Permits
# Fast-growing Research Triangle, strong tech/commercial
# Dataset: Building Permits with Parcel ID (b9nv-68kk)
# ═══════════════════════════════════════════════════════════════

class RaleighPermitScraper(BaseScraper):
    """Raleigh building permits via Socrata."""

    source_name = "Raleigh Permits"
    DATASET_ID  = "b9nv-68kk"
    BASE_URL    = "https://data.raleighnc.gov"

    def fetch_raw(self) -> list[dict]:
        app_token = os.environ.get("SOCRATA_APP_TOKEN") or os.environ.get("NYC_APP_TOKEN", "")
        try:
            resp = requests.get(
                f"{self.BASE_URL}/resource/{self.DATASET_ID}.json",
                params={"$limit": 1000, "$$app_token": app_token},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            records = resp.json()
            if records:
                logger.info(f"[Raleigh Permits] Sample fields: {list(records[0].keys())[:15]}")
            filtered = [r for r in records if self._parse_cost(r) >= MIN_VALUE_USD]
            logger.info(f"[Raleigh Permits] Fetched {len(records)} raw, {len(filtered)} filtered")
            return filtered
        except requests.RequestException as e:
            logger.warning(f"[Raleigh Permits] Fetch failed: {e}")
            return []

    def _parse_cost(self, r):
        for f in ("estimated_value", "declared_valuation", "job_value", "cost", "valuation"):
            v = r.get(f)
            if v:
                try: return float(str(v).replace(",","").replace("$",""))
                except: pass
        return 0

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        permit_id = (raw.get("permit_number") or raw.get("permitnumber") or raw.get("permit_num") or "").strip()
        if not permit_id:
            return None
        value = self._parse_cost(raw)
        if value < MIN_VALUE_USD:
            return None
        permit_type = raw.get("permit_type") or raw.get("work_class") or ""
        desc        = raw.get("description") or raw.get("comments") or ""
        title       = f"{permit_type}: {str(desc)[:120]}" if desc else permit_type or "Building Permit"
        address     = raw.get("address") or raw.get("location_address") or ""
        zip_code    = raw.get("zip_code") or raw.get("zip") or ""
        location    = f"{address}, Raleigh, NC {zip_code}".strip(", ")
        try:
            lat = float(raw["latitude"])  if raw.get("latitude")  else None
            lng = float(raw["longitude"]) if raw.get("longitude") else None
        except (ValueError, TypeError):
            lat, lng = None, None
        status = (raw.get("status") or raw.get("permit_status") or "").lower()
        if any(s in status for s in ("finaled", "cancelled", "expired", "withdrawn", "complete", "closed")):
            return None
        stage      = "Awarded" if "issued" in status or "approved" in status else "Planning"
        contractor = (raw.get("contractor_company_name") or raw.get("contractor") or "").strip()
        applicant  = (raw.get("applicant_name") or raw.get("owner_name") or "").strip()
        issue_date = (raw.get("issued_date") or raw.get("issue_date") or "")[:10]
        stakeholders = []
        if contractor:
            stakeholders.append({"name": contractor, "role": "Main Contractor"})
        if applicant and applicant != contractor:
            stakeholders.append({"name": applicant, "role": "Owner"})
        return ProjectRecord(
            external_id      = f"ral-{permit_id}",
            source_name      = self.source_name,
            source_url       = f"https://data.raleighnc.gov/resource/{self.DATASET_ID}",
            title            = title[:500],
            description      = str(desc)[:2000] if desc else None,
            value_usd        = value,
            value_currency   = "USD",
            location_display = location[:500],
            location_country = "US",
            region           = "Americas",
            sector           = infer_sector(f"{title} {desc}"),
            stage            = stage,
            timeline_display = issue_date,
            lat              = lat,
            lng              = lng,
            stakeholders     = stakeholders,
        )