"""
etl/runner.py — CSTGlobal ETL Runner

Usage:
  python -m etl.runner                           # run all scrapers
  python -m etl.runner --source world_bank       # run one
  python -m etl.runner --source nyc_permits chicago_permits
  python -m etl.runner --dry-run
  python -m etl.runner --sequential
"""

import argparse
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from etl.scrapers.ted_eu_and_sam_gov import TedEUScraper, SamGovScraper
from etl.scrapers.planning_and_procurement import (
    UKPlanningPortalScraper,
    NSWEPlanningScraper,
    WorldBankScraper,
    ContractsFinderScraper,
)
from etl.scrapers.city_permits import (
    NYCPermitScraper,
    ChicagoPermitScraper,
    LosAngelesPermitScraper,
    HoustonPermitScraper,
    PhiladelphiaPermitScraper,
    USACEScraper,
    PhillyPermitScraper,
    DenverPermitScraper,
    SanFranciscoPermitScraper,
    BostonPermitScraper,
    SanJosePermitScraper,
    BaltimorePermitScraper,
)

SCRAPERS = {
    # Public procurement
    "ted_eu":           TedEUScraper,
    "sam_gov":          SamGovScraper,
    "contracts_finder": ContractsFinderScraper,
    "world_bank":       WorldBankScraper,
    # Planning applications (private + mixed)
    "uk_planning":      UKPlanningPortalScraper,
    "nsw_eplanning":    NSWEPlanningScraper,
    # US city building permits (private projects)
    "nyc_permits":      NYCPermitScraper,
    "chicago_permits":  ChicagoPermitScraper,
    "la_permits":       LosAngelesPermitScraper,
    "houston_permits":  HoustonPermitScraper,
    "philly_permits":   PhiladelphiaPermitScraper,
    "usace":            USACEScraper,
    "philly_arcgis":    PhillyPermitScraper,
    "denver_permits":   DenverPermitScraper,
    "sf_permits":       SanFranciscoPermitScraper,
    "boston_permits":   BostonPermitScraper,
    "sj_permits":       SanJosePermitScraper,
    "baltimore_permits":BaltimorePermitScraper,
}

SCRAPER_META = {
    "ted_eu":           {"label": "TED EU",              "region": "Europe",       "type": "Public Tender"},
    "sam_gov":          {"label": "SAM.gov",             "region": "Americas",     "type": "Public Tender"},
    "contracts_finder": {"label": "Contracts Finder",    "region": "Europe",       "type": "Public Tender"},
    "world_bank":       {"label": "World Bank",          "region": "Global",       "type": "Development Project"},
    "uk_planning":      {"label": "UK Planning Portal",  "region": "Europe",       "type": "Planning Application"},
    "nsw_eplanning":    {"label": "NSW ePlanning",       "region": "Asia Pacific", "type": "Planning Application"},
    "nyc_permits":      {"label": "NYC DOB",             "region": "Americas",     "type": "City Permit"},
    "chicago_permits":  {"label": "Chicago Permits",     "region": "Americas",     "type": "City Permit"},
    "la_permits":       {"label": "LA Building & Safety","region": "Americas",     "type": "City Permit"},
    "houston_permits":  {"label": "Houston Permits",     "region": "Americas",     "type": "City Permit"},
    "philly_permits":   {"label": "Seattle Permits",     "region": "Americas",     "type": "City Permit"},
    "usace":            {"label": "USACE",               "region": "Americas",     "type": "Federal Infrastructure"},
    "philly_arcgis":    {"label": "Philadelphia L&I",   "region": "Americas",     "type": "City Permit"},
    "denver_permits":   {"label": "Dallas Permits",     "region": "Americas",     "type": "City Permit"},
    "sf_permits":       {"label": "SF Building Permits","region": "Americas",     "type": "City Permit"},
    "boston_permits":   {"label": "Boston Permits",      "region": "Americas",     "type": "City Permit"},
    "sj_permits":       {"label": "San Jose Permits",    "region": "Americas",     "type": "City Permit"},
    "baltimore_permits":{"label": "Baltimore Permits",   "region": "Americas",     "type": "City Permit"},
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("etl.runner")


def run_scraper(name: str, ScraperClass, dry_run: bool = False) -> dict:
    logger.info(f"Starting scraper: {name}")
    start = time.time()
    try:
        scraper = ScraperClass()
        if dry_run:
            raw = scraper.fetch_raw()
            normalised = [scraper.normalize(r) for r in raw]
            valid = [n for n in normalised if n is not None]
            stats = {"fetched": len(raw), "normalised": len(valid), "dry_run": True}
        else:
            stats = scraper.run()
        elapsed = round(time.time() - start, 1)
        logger.info(f"✓ {name} completed in {elapsed}s — {stats}")
        return {"source": name, "status": "success", "stats": stats, "elapsed": elapsed}
    except Exception as e:
        elapsed = round(time.time() - start, 1)
        logger.error(f"✗ {name} failed after {elapsed}s — {e}", exc_info=True)
        return {"source": name, "status": "failed", "error": str(e), "elapsed": elapsed}


def run_all(
    sources: Optional[list[str]] = None,
    dry_run: bool = False,
    parallel: bool = True,
    max_workers: int = 4,
) -> list[dict]:
    to_run = {
        name: cls for name, cls in SCRAPERS.items()
        if not sources or name in sources
    }
    if not to_run:
        logger.warning(f"No scrapers matched sources={sources}")
        return []

    results = []
    if parallel and len(to_run) > 1:
        with ThreadPoolExecutor(max_workers=min(max_workers, len(to_run))) as pool:
            futures = {pool.submit(run_scraper, name, cls, dry_run): name for name, cls in to_run.items()}
            for future in as_completed(futures):
                results.append(future.result())
    else:
        for name, cls in to_run.items():
            results.append(run_scraper(name, cls, dry_run))

    ok  = sum(1 for r in results if r["status"] == "success")
    err = sum(1 for r in results if r["status"] == "failed")
    total_inserted = sum(r.get("stats", {}).get("inserted", 0) for r in results)
    total_updated  = sum(r.get("stats", {}).get("updated",  0) for r in results)

    logger.info(
        f"\n{'='*50}\n"
        f"ETL Run Summary\n"
        f"  Scrapers:  {ok} ok / {err} failed\n"
        f"  Inserted:  {total_inserted}\n"
        f"  Updated:   {total_updated}\n"
        f"{'='*50}"
    )
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CSTGlobal ETL Runner")
    parser.add_argument("--source", nargs="*", choices=list(SCRAPERS.keys()))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sequential", action="store_true")
    args = parser.parse_args()
    run_all(sources=args.source, dry_run=args.dry_run, parallel=not args.sequential)