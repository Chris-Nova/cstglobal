"""
etl/scrapers/planning_and_procurement.py

Four new scrapers for private + public project data:

1. UK Planning Portal     — private development applications across England & Wales
2. NSW ePlanning          — Australian planning applications (Sydney, NSW)
3. World Bank Projects    — international development projects (free API, no key needed)
4. Contracts Finder (UK)  — UK public procurement (free API, no key needed)

All inherit BaseScraper and produce ProjectRecord objects compatible with the existing pipeline.
"""

import re
import time
import logging
from typing import Optional
from datetime import datetime, timedelta, timezone

import requests

from etl.base_scraper import BaseScraper, ProjectRecord

logger = logging.getLogger(__name__)


# ── Shared utilities (copied from ted_eu_and_sam_gov.py) ──────

SECTOR_KEYWORDS = {
    "Transport":      ["metro", "rail", "road", "highway", "bridge", "tunnel", "airport", "port", "transit"],
    "Energy":         ["solar", "wind", "power", "energy", "grid", "substation", "pipeline", "lng", "renewab"],
    "Water":          ["water", "sewage", "sewer", "desalin", "irrigation", "drainage", "dam", "flood"],
    "Commercial":     ["office", "retail", "campus", "hotel", "mixed use", "commercial", "business park"],
    "Healthcare":     ["hospital", "clinic", "medical", "health centre", "care home"],
    "Education":      ["school", "university", "college", "academy", "nursery", "education"],
    "Residential":    ["housing", "residential", "apartment", "homes", "dwelling", "flats", "affordable"],
    "Sport & Leisure": ["stadium", "arena", "leisure", "sport", "gym", "swimming", "recreation"],
    "Infrastructure": ["infrastructure", "utilities", "telecom", "broadband", "data centre", "warehouse"],
    "Mixed Use":      ["mixed use", "mixed-use", "regeneration", "development"],
}

REGION_COUNTRY_MAP = {
    "Middle East":  ["AE", "SA", "QA", "KW", "BH", "OM", "IQ", "JO", "LB", "YE"],
    "Europe":       ["DE", "FR", "GB", "IT", "ES", "NL", "PL", "SE", "NO", "CH", "AT", "BE", "DK", "FI"],
    "Asia Pacific": ["AU", "NZ", "SG", "JP", "KR", "IN", "CN", "TH", "MY", "ID", "PH"],
    "Americas":     ["US", "CA", "BR", "MX", "CO", "CL", "AR", "PE"],
    "Africa":       ["ZA", "NG", "KE", "GH", "EG", "ET", "TZ", "UG", "CI", "SN", "RW", "MZ"],
    "Central Asia": ["KZ", "UZ", "AZ", "TM", "KG", "TJ"],
}

_COUNTRY_TO_REGION = {
    c: r for r, cs in REGION_COUNTRY_MAP.items() for c in cs
}


def infer_sector(text: str) -> str:
    text_lower = (text or "").lower()
    for sector, keywords in SECTOR_KEYWORDS.items():
        if any(k in text_lower for k in keywords):
            return sector
    return "Infrastructure"


def parse_value(value) -> Optional[float]:
    if value is None:
        return None
    s = str(value).replace(",", "").strip()
    multiplier = 1
    if re.search(r'[bB]illion|[bB]$', s):
        multiplier = 1_000_000_000
        s = re.sub(r'[bB]illion|[bB]', '', s)
    elif re.search(r'[mM]illion|[mM]$', s):
        multiplier = 1_000_000
        s = re.sub(r'[mM]illion|[mM]', '', s)
    s = re.sub(r'[^\d.]', '', s)
    try:
        return float(s) * multiplier
    except (ValueError, TypeError):
        return None


# ═══════════════════════════════════════════════════════════════
# SCRAPER 1 — UK Planning Portal
# Covers planning applications across England & Wales
# Free RSS/JSON feeds from local planning authorities
# ═══════════════════════════════════════════════════════════════

class UKPlanningPortalScraper(BaseScraper):
    source_name = "UK Planning Portal"

    # Planning Data API — free, no key required
    # Returns applications with estimated construction values
    BASE_URL = "https://www.planning.data.gov.uk/entity.json"

    # Application types to capture
    DATASETS = [
        "development-plan-document",
        "planning-permission",
    ]

    def fetch_raw(self) -> list[dict]:
        results = []
        since = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d")

        try:
            resp = requests.get(
                self.BASE_URL,
                params={
                    "dataset":        "planning-permission",
                    "entry-date-day-after": since,
                    "limit":          100,
                    "offset":         0,
                    "format":         "json",
                },
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            entities = data.get("entities", [])
            results.extend(entities)
            logger.info(f"[UK Planning Portal] Fetched {len(entities)} planning applications")
        except requests.RequestException as e:
            logger.warning(f"[UK Planning Portal] Fetch failed: {e}")

        # Also query the DLUHC planning applications feed
        try:
            resp2 = requests.get(
                "https://www.planning.data.gov.uk/entity.json",
                params={
                    "dataset": "development-plan-document",
                    "entry-date-day-after": since,
                    "limit":   100,
                    "format":  "json",
                },
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp2.raise_for_status()
            data2 = resp2.json()
            results.extend(data2.get("entities", []))
        except requests.RequestException as e:
            logger.warning(f"[UK Planning Portal] Development plans fetch failed: {e}")

        return results

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        name = (raw.get("name") or raw.get("reference") or "").strip()
        if not name or len(name) < 3:
            return None

        description = raw.get("notes") or raw.get("description") or None
        title = name if len(name) > 10 else f"Planning Application {name}"

        geometry = raw.get("geometry") or ""
        lat, lng = None, None
        if "," in str(geometry):
            try:
                coords = str(geometry).replace("POINT(", "").replace(")", "").split()
                if len(coords) == 2:
                    lng, lat = float(coords[0]), float(coords[1])
            except (ValueError, IndexError):
                pass

        organisation = raw.get("organisation-entity") or raw.get("organisation") or ""
        location = raw.get("address-text") or raw.get("geometry") or "United Kingdom"

        return ProjectRecord(
            external_id         = str(raw.get("entity") or raw.get("reference") or name),
            source_name         = self.source_name,
            source_url          = f"https://www.planning.data.gov.uk/entity/{raw.get('entity', '')}",
            title               = title[:500],
            description         = str(description)[:2000] if description else None,
            location_display    = str(location)[:500] if location else "United Kingdom",
            location_country    = "GB",
            region              = "Europe",
            sector              = infer_sector(f"{title} {description or ''}"),
            stage               = "Planning",
            timeline_display    = raw.get("entry-date", ""),
            stakeholders        = [{"name": str(organisation), "role": "Owner"}] if organisation else [],
            lat                 = lat,
            lng                 = lng,
        )


# ═══════════════════════════════════════════════════════════════
# SCRAPER 2 — NSW ePlanning (Australia)
# Sydney and New South Wales development applications
# Free API — no key required
# ═══════════════════════════════════════════════════════════════

class NSWEPlanningScraper(BaseScraper):
    source_name = "NSW ePlanning"
    BASE_URL    = "https://api.apps1.nsw.gov.au/eplanning/development-applications/v1"

    def fetch_raw(self) -> list[dict]:
        results = []
        page = 1
        page_size = 100
        since = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%d/%m/%Y")

        while page <= 5:  # max 500 records per run
            try:
                resp = requests.get(
                    f"{self.BASE_URL}/",
                    params={
                        "filters[lodgement_date_from]": since,
                        "filters[development_type]":    "Commercial,Infrastructure,Residential",
                        "page_number": page,
                        "page_size":   page_size,
                    },
                    headers={
                        "Accept": "application/json",
                        "X-API-Key": "guest",
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()

                applications = data.get("Application", [])
                if not applications:
                    break

                results.extend(applications)
                logger.info(f"[NSW ePlanning] Page {page}: {len(applications)} applications")

                if len(applications) < page_size:
                    break

                page += 1
                time.sleep(0.5)

            except requests.RequestException as e:
                logger.warning(f"[NSW ePlanning] Page {page} failed: {e}")
                break

        return results

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        title = (raw.get("ApplicationType") or raw.get("DevelopmentDescription") or "").strip()
        if not title:
            return None

        description = raw.get("DevelopmentDescription") or raw.get("ApplicationType") or None

        # Estimated value
        cost_str = raw.get("CostOfDevelopment") or raw.get("EstimatedCost") or None
        value = parse_value(cost_str)

        # Location
        address = raw.get("Address") or {}
        if isinstance(address, dict):
            street  = address.get("FullAddress") or address.get("RoadName") or ""
            suburb  = address.get("SuburbName") or ""
            location_display = f"{street}, {suburb}, NSW, Australia".strip(", ")
            lat = address.get("Latitude")
            lng = address.get("Longitude")
            try:
                lat = float(lat) if lat else None
                lng = float(lng) if lng else None
            except (ValueError, TypeError):
                lat, lng = None, None
        else:
            location_display = "NSW, Australia"
            lat, lng = None, None

        # Stage
        status = (raw.get("ApplicationStatus") or "").lower()
        if "approved" in status or "granted" in status:
            stage = "Awarded"
        elif "under assessment" in status or "submitted" in status:
            stage = "Planning"
        else:
            stage = "Planning"

        applicant = raw.get("ApplicantName") or raw.get("Applicant") or ""

        return ProjectRecord(
            external_id         = str(raw.get("ApplicationNumber") or raw.get("PlanningPortalApplicationNumber") or title),
            source_name         = self.source_name,
            source_url          = f"https://www.planningportal.nsw.gov.au/pages/rp.aspx?da={raw.get('ApplicationNumber', '')}",
            title               = f"{title[:200]} — {raw.get('ApplicationType', '')}".strip(" —"),
            description         = str(description)[:2000] if description else None,
            value_usd           = value,
            value_currency      = "AUD",
            location_display    = location_display[:500],
            location_country    = "AU",
            region              = "Asia Pacific",
            sector              = infer_sector(f"{title} {description or ''}"),
            stage               = stage,
            lat                 = lat,
            lng                 = lng,
            stakeholders        = [{"name": str(applicant), "role": "Owner"}] if applicant else [],
        )


# ═══════════════════════════════════════════════════════════════
# SCRAPER 3 — World Bank Projects
# International development & infrastructure projects worldwide
# Free API — no key required, excellent global coverage
# ═══════════════════════════════════════════════════════════════

class WorldBankScraper(BaseScraper):
    source_name = "World Bank"
    BASE_URL    = "https://search.worldbank.org/api/v2/projects"

    SECTOR_MAP = {
        "TI": "Transport",
        "ET": "Energy",
        "WS": "Water",
        "UR": "Infrastructure",
        "ED": "Education",
        "HE": "Healthcare",
        "AG": "Infrastructure",
    }

    def fetch_raw(self) -> list[dict]:
        results = []
        rows = 100
        start = 0

        while start < 500:  # max 500 per run
            try:
                resp = requests.get(
                    self.BASE_URL,
                    params={
                        "format":   "json",
                        "rows":     rows,
                        "os":       start,
                        "fl":       "id,project_name,totalamt,countryname,countrycode,sector1,status,boardapprovaldate,closingdate,project_abstract,url,lendprojectcost",
                        "strdate":  "2020-01-01",
                        "enddate":  datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        "status":   "Active",
                    },
                    headers={"Accept": "application/json"},
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()

                projects = data.get("projects", {})
                if isinstance(projects, dict):
                    batch = list(projects.values())
                elif isinstance(projects, list):
                    batch = projects
                else:
                    break

                if not batch:
                    break

                results.extend(batch)
                logger.info(f"[World Bank] Offset {start}: {len(batch)} projects")

                if len(batch) < rows:
                    break

                start += rows
                time.sleep(0.5)

            except requests.RequestException as e:
                logger.warning(f"[World Bank] Fetch failed at offset {start}: {e}")
                break

        return results

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        if not isinstance(raw, dict):
            return None

        title = (raw.get("project_name") or "").strip()
        if not title or len(title) < 3:
            return None

        # Value — World Bank stores in USD thousands
        total_amt = raw.get("totalamt") or raw.get("lendprojectcost") or 0
        try:
            value = float(str(total_amt).replace(",", "")) * 1000  # convert thousands to dollars
        except (ValueError, TypeError):
            value = None

        # countrycode and countryname can be lists in World Bank API response
        cc = raw.get("countrycode") or ""
        country_code = (cc[0] if isinstance(cc, list) else cc).strip().upper()
        cn = raw.get("countryname") or ""
        country_name = (cn[0] if isinstance(cn, list) else cn)

        # Sector
        sector_code = ""
        sector1 = raw.get("sector1") or {}
        if isinstance(sector1, dict):
            sector_code = sector1.get("code", "")[:2]
        sector = self.SECTOR_MAP.get(sector_code, infer_sector(title))

        # Stage
        status = (raw.get("status") or "").lower()
        if "active" in status:
            stage = "Under Construction"
        elif "pipeline" in status:
            stage = "Planning"
        elif "closed" in status:
            stage = "Completed"
        else:
            stage = "Awarded"

        # Dates
        board_date   = raw.get("boardapprovaldate") or ""
        closing_date = raw.get("closingdate") or ""
        timeline     = ""
        if board_date and closing_date:
            try:
                y_start = board_date[:4]
                y_end   = closing_date[:4]
                timeline = f"{y_start}–{y_end}"
            except Exception:
                pass

        abstract = raw.get("project_abstract") or {}
        description = abstract.get("cdata") if isinstance(abstract, dict) else str(abstract or "")

        return ProjectRecord(
            external_id         = raw.get("id") or title,
            source_name         = self.source_name,
            source_url          = raw.get("url") or f"https://projects.worldbank.org/en/projects-operations/project-detail/{raw.get('id', '')}",
            title               = title[:500],
            description         = str(description)[:2000] if description else None,
            value_usd           = value,
            value_currency      = "USD",
            location_display    = country_name,
            location_country    = country_code if len(country_code) == 2 else None,
            region              = _COUNTRY_TO_REGION.get(country_code),
            sector              = sector,
            stage               = stage,
            timeline_display    = timeline,
            stakeholders        = [{"name": "World Bank", "role": "Financier"}],
        )


# ═══════════════════════════════════════════════════════════════
# SCRAPER 4 — Contracts Finder (UK)
# UK public procurement — central government + local authorities
# Free API — no key required, excellent for UK construction
# ═══════════════════════════════════════════════════════════════

class ContractsFinderScraper(BaseScraper):
    source_name = "Contracts Finder"
    BASE_URL    = "https://www.contractsfinder.service.gov.uk/Published/Notices/PublicSearch/Search"

    CONSTRUCTION_KEYWORDS = [
        "construction", "building", "civil engineering", "infrastructure",
        "highways", "roads", "rail", "water", "energy", "housing",
        "refurbishment", "renovation", "facilities management",
        "mechanical", "electrical", "M&E", "structural",
    ]

    def fetch_raw(self) -> list[dict]:
        # Contracts Finder API v2 requires POST with JSON body
        results = []
        since = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%dT00:00:00")

        for keyword in ["construction", "building works", "civil engineering"]:
            try:
                resp = requests.post(
                    "https://www.contractsfinder.service.gov.uk/api/rest/2/search_notices/json",
                    json={
                        "searchCriteria": {
                            "types":         ["Contract", "Pipeline"],
                            "statuses":      ["Open"],
                            "keyword":       keyword,
                            "publishedFrom": since,
                            "publishedTo":   None,
                        },
                        "size": 100,
                    },
                    headers={
                        "Accept":       "application/json",
                        "Content-Type": "application/json",
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
                notices = data.get("noticeList", [])
                # Each item is {"score": x, "item": {...}}
                batch = [n.get("item", n) for n in notices if isinstance(n, dict)]
                results.extend(batch)
                logger.info(f"[Contracts Finder] keyword='{keyword}' fetched {len(batch)} notices")
                time.sleep(1)  # Be polite
            except requests.RequestException as e:
                logger.warning(f"[Contracts Finder] keyword='{keyword}' failed: {e}")

        return results

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        if not isinstance(raw, dict):
            return None

        notice = raw.get("notice") or raw
        title  = (notice.get("title") or notice.get("name") or "").strip()
        if not title or len(title) < 5:
            return None

        description = notice.get("description") or notice.get("summary") or None

        # Value
        value = None
        value_str = notice.get("value") or notice.get("estimatedValue") or notice.get("awardedValue")
        if isinstance(value_str, dict):
            value = parse_value(value_str.get("amount"))
        elif value_str:
            value = parse_value(value_str)

        # Stage
        notice_type = (notice.get("type") or notice.get("noticeType") or "").lower()
        stage = "Awarded" if "award" in notice_type else "Tender"

        # Location
        location = notice.get("location") or notice.get("deliveryLocation") or {}
        if isinstance(location, dict):
            location_display = location.get("region") or location.get("name") or "United Kingdom"
            location_display = f"{location_display}, United Kingdom" if location_display != "United Kingdom" else "United Kingdom"
        else:
            location_display = "United Kingdom"

        # Dates
        pub_date   = (notice.get("publishedDate") or "")[:10]
        close_date = (notice.get("closingDate") or notice.get("deadlineDate") or "")[:10]
        timeline   = f"{pub_date} - {close_date}" if pub_date and close_date else pub_date

        # Org
        org = notice.get("organisationName") or notice.get("buyerName") or ""
        if isinstance(org, dict):
            org = org.get("name") or ""

        notice_id = notice.get("id") or notice.get("noticeIdentifier") or notice.get("ocid") or title[:80]

        return ProjectRecord(
            external_id         = str(notice_id),
            source_name         = self.source_name,
            source_url          = notice.get("uri") or "https://www.contractsfinder.service.gov.uk",
            title               = title[:500],
            description         = str(description)[:2000] if description else None,
            value_usd           = value,
            value_currency      = "GBP",
            location_display    = location_display,
            location_country    = "GB",
            region              = "Europe",
            sector              = infer_sector(f"{title} {description or ''}"),
            stage               = stage,
            timeline_display    = timeline,
            stakeholders        = [{"name": str(org), "role": "Owner"}] if org else [],
        )