"""
etl/scrapers/ted_eu.py  &  sam_gov.py

Two concrete scrapers as implementation examples.
Both inherit BaseScraper and implement fetch_raw() + normalize().

TED EU  → https://ted.europa.eu/api/v3/notices (public REST API)
Sam.gov → https://sam.gov/api/prod/opportunities/v2/search
"""

import re
import time
import logging
from typing import Optional

import requests

from etl.base_scraper import BaseScraper, ProjectRecord

logger = logging.getLogger(__name__)


# ── Shared utilities ──────────────────────────────────────────

SECTOR_KEYWORDS = {
    "Transport":     ["metro", "rail", "road", "highway", "bridge", "tunnel", "airport", "port"],
    "Energy":        ["solar", "wind", "power station", "grid", "substation", "pipeline", "lng"],
    "Water":         ["water treatment", "sewage", "desalination", "irrigation", "drainage", "dam"],
    "Commercial":    ["office", "retail", "campus", "hotel", "mixed use", "commercial"],
    "Healthcare":    ["hospital", "clinic", "medical centre"],
    "Education":     ["school", "university", "college", "campus"],
    "Residential":   ["housing", "residential", "apartments", "homes"],
    "Infrastructure": ["infrastructure", "utilities", "telecom", "broadband"],
}

REGION_COUNTRY_MAP = {
    "Middle East":  ["AE", "SA", "QA", "KW", "BH", "OM", "IQ", "JO", "LB", "YE"],
    "Europe":       ["DE", "FR", "GB", "IT", "ES", "NL", "PL", "SE", "NO", "CH", "AT"],
    "Asia Pacific": ["AU", "NZ", "SG", "JP", "KR", "IN", "CN", "TH", "MY", "ID", "PH"],
    "Americas":     ["US", "CA", "BR", "MX", "CO", "CL", "AR", "PE"],
    "Africa":       ["ZA", "NG", "KE", "GH", "EG", "ET", "TZ", "UG", "CI", "SN"],
    "Central Asia": ["KZ", "UZ", "AZ", "TM", "KG", "TJ"],
}

_COUNTRY_TO_REGION = {
    country: region
    for region, countries in REGION_COUNTRY_MAP.items()
    for country in countries
}


def infer_sector(text: str) -> Optional[str]:
    """Rule-based sector inference from title + description."""
    text_lower = text.lower()
    for sector, keywords in SECTOR_KEYWORDS.items():
        if any(k in text_lower for k in keywords):
            return sector
    return "Infrastructure"   # fallback


def country_to_region(country_code: str) -> Optional[str]:
    return _COUNTRY_TO_REGION.get(country_code.upper())


def parse_value(value_str: Optional[str], currency: str = "USD") -> Optional[int]:
    """Extract integer cent value from messy strings like '$1.2B', '€500M', '2,500,000'."""
    if not value_str:
        return None
    s = str(value_str).replace(",", "").strip()
    multiplier = 1
    if s.lower().endswith("b") or "billion" in s.lower():
        multiplier = 1_000_000_000
        s = re.sub(r"[bB]illion|[bB]", "", s)
    elif s.lower().endswith("m") or "million" in s.lower():
        multiplier = 1_000_000
        s = re.sub(r"[mM]illion|[mM]", "", s)
    s = re.sub(r"[^\d.]", "", s)
    try:
        return int(float(s) * multiplier * 100)   # store in cents
    except (ValueError, TypeError):
        return None


# ═══════════════════════════════════════════════════════════════
# SCRAPER 1 — TED EU (Tenders Electronic Daily)
# Covers EU + associated country public procurement
# ═══════════════════════════════════════════════════════════════

class TedEUScraper(BaseScraper):
    source_name = "TED EU"
    BASE_URL    = "https://ted.europa.eu/api/v3/notices/search"

    # CPV (Common Procurement Vocabulary) codes for construction
    CPV_CODES = [
        "45000000",   # construction work
        "45200000",   # building & civil engineering
        "45300000",   # building installation work
        "45400000",   # building completion work
        "45221000",   # bridges & tunnels
        "45234000",   # railway construction
    ]

    def fetch_raw(self) -> list[dict]:
        """
        Query TED API for recent construction notices.
        Paginates automatically.
        """
        notices = []
        page = 1
        page_size = 50

        cpv_filter = " OR ".join(f'cpv:{c}' for c in self.CPV_CODES)
        query = f"({cpv_filter}) AND publicationDateRange:>=2025-01-01"

        while True:
            try:
                resp = requests.get(
                    self.BASE_URL,
                    params={
                        "q":        query,
                        "fields":   "ND,TI,TD,PC,CY,TW,MA,VL,CU,DT,AU",
                        "pageNum":  page,
                        "pageSize": page_size,
                        "scope":    "ALL",
                    },
                    timeout=30,
                    headers={"Accept": "application/json"},
                )
                resp.raise_for_status()
                data = resp.json()

                batch = data.get("notices", [])
                notices.extend(batch)

                if len(batch) < page_size:
                    break   # last page

                page += 1
                time.sleep(0.5)  # polite rate limiting

            except requests.RequestException as e:
                logger.warning(f"[TED EU] Request failed on page {page}: {e}")
                break

        return notices

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        title = raw.get("TI", {}).get("value", "").strip()
        if not title or len(title) < 5:
            return None

        country_code = raw.get("CY", {}).get("value", "")
        value_raw    = raw.get("VL", {}).get("value", "")
        currency     = raw.get("CU", {}).get("value", "EUR")

        description_parts = [
            raw.get("TD", {}).get("value", ""),
            raw.get("PC", {}).get("value", ""),
        ]
        description = " | ".join(p for p in description_parts if p) or None

        # Stage inference from notice type code
        type_code = raw.get("TW", {}).get("value", "")
        stage_map = {"F02": "Tender", "F03": "Awarded", "F01": "Planning"}
        stage = stage_map.get(type_code, "Planning")

        return ProjectRecord(
            external_id         = raw.get("ND", {}).get("value", ""),
            source_name         = self.source_name,
            source_url          = f"https://ted.europa.eu/udl?uri=TED:NOTICE:{raw.get('ND', {}).get('value', '')}",
            title               = title,
            description         = description,
            value_usd           = parse_value(value_raw, currency),
            value_currency      = currency,
            value_raw           = str(value_raw),
            location_country    = country_code,
            location_city       = raw.get("TW", {}).get("value", ""),
            location_display    = f"{raw.get('TW', {}).get('value', '')}, {country_code}".strip(", "),
            region              = country_to_region(country_code),
            sector              = infer_sector(title + " " + (description or "")),
            stage               = stage,
            stakeholders        = [{"name": raw.get("AU", {}).get("value", ""), "role": "Owner"}]
                                   if raw.get("AU") else [],
        )


# ═══════════════════════════════════════════════════════════════
# SCRAPER 2 — SAM.GOV (US Federal Procurement)
# ═══════════════════════════════════════════════════════════════

class SamGovScraper(BaseScraper):
    source_name = "Sam.gov"
    BASE_URL    = "https://api.sam.gov/opportunities/v2/search"

    NAICS_CODES = [
        "236220",   # commercial & institutional building construction
        "237110",   # water & sewer line construction
        "237130",   # power & communication line construction
        "237310",   # highway, street, bridge construction
        "237990",   # other heavy/civil engineering construction
    ]

    def __init__(self, api_key: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        import os
        self.api_key = api_key or os.environ.get("SAM_GOV_API_KEY", "DEMO_KEY")

    def fetch_raw(self) -> list[dict]:
        all_results = []
        offset = 0
        limit  = 100

        for naics in self.NAICS_CODES:
            offset = 0
            while True:
                try:
                    resp = requests.get(
                        self.BASE_URL,
                        params={
                            "api_key":    self.api_key,
                            "naicsCode":  naics,
                            "limit":      limit,
                            "offset":     offset,
                            "postedFrom": "01/01/2025",
                            "postedTo":   "12/31/2025",
                        },
                        timeout=30,
                    )
                    resp.raise_for_status()
                    data = resp.json()

                    batch = data.get("opportunitiesData", [])
                    all_results.extend(batch)

                    if len(batch) < limit:
                        break

                    offset += limit
                    time.sleep(0.3)

                except requests.RequestException as e:
                    logger.warning(f"[Sam.gov] NAICS {naics} page failed: {e}")
                    break

        return all_results

    def normalize(self, raw: dict) -> Optional[ProjectRecord]:
        title = (raw.get("title") or "").strip()
        if not title:
            return None

        # Extract value from award amounts or estimated values
        award_amounts = raw.get("award", {})
        value_raw = (
            award_amounts.get("amount") or
            raw.get("estimatedValue") or
            None
        )

        description = raw.get("description") or raw.get("synopsis") or None

        # Stage from opportunity type
        opportunity_type = raw.get("type", "").lower()
        if "award" in opportunity_type:
            stage = "Awarded"
        elif "solicitation" in opportunity_type or "bid" in opportunity_type:
            stage = "Tender"
        else:
            stage = "Planning"

        # Location
        place = raw.get("placeOfPerformance") or {}
        city_obj  = place.get("city") or {}
        state_obj = place.get("state") or {}
        city  = city_obj.get("name", "") if isinstance(city_obj, dict) else ""
        state = state_obj.get("code", "") if isinstance(state_obj, dict) else ""
        location_display = ", ".join(p for p in [city, state, "USA"] if p)

        # Stakeholder
        org = raw.get("organizationHierarchy") or []
        owner_name = org[0].get("name", "") if org and isinstance(org[0], dict) else raw.get("department", "") or ""

        return ProjectRecord(
            external_id         = raw.get("noticeId", raw.get("solicitationNumber", "")),
            source_name         = self.source_name,
            source_url          = raw.get("uiLink"),
            tender_document_url = raw.get("resourceLinks", [None])[0],
            title               = title,
            description         = description,
            value_usd           = parse_value(str(value_raw)) if value_raw else None,
            value_currency      = "USD",
            value_raw           = str(value_raw) if value_raw else None,
            location_display    = location_display,
            location_country    = "US",
            location_city       = city,
            region              = "Americas",
            sector              = infer_sector(title + " " + (description or "")),
            stage               = stage,
            stakeholders        = [{"name": owner_name, "role": "Owner"}] if owner_name else [],
        )
