"""
etl/base_scraper.py
Abstract base class for all CSTGlobal data scrapers.
"""

import hashlib
import json
import logging
import os
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


@dataclass
class ProjectRecord:
    external_id:          str
    source_name:          str
    source_url:           Optional[str] = None
    tender_document_url:  Optional[str] = None
    title:                str = ""
    description:          Optional[str] = None
    value_usd:            Optional[float] = None
    value_currency:       str = "USD"
    value_raw:            Optional[str] = None
    location_display:     Optional[str] = None
    location_country:     Optional[str] = None
    lat:                  Optional[float] = None
    lng:                  Optional[float] = None
    region:               Optional[str] = None
    sector:               Optional[str] = None
    stage:                Optional[str] = None
    timeline_display:     Optional[str] = None
    milestones:           list = field(default_factory=list)
    stakeholders:         list = field(default_factory=list)

    def content_hash(self) -> str:
        payload = json.dumps({
            "external_id": self.external_id,
            "source_name": self.source_name,
            "title":       self.title,
            "value_usd":   self.value_usd,
            "stage":       self.stage,
            "location":    self.location_display,
        }, sort_keys=True)
        return hashlib.sha256(payload.encode()).hexdigest()


class BaseScraper(ABC):
    source_name: str = "unknown"

    def __init__(self, db_url: Optional[str] = None):
        self.db_url = db_url or os.environ["DATABASE_URL"]
        self.conn   = None
        self.run_id = str(uuid.uuid4())
        self.stats  = dict(fetched=0, inserted=0, updated=0, skipped=0)

    @abstractmethod
    def fetch_raw(self) -> list[dict]: ...

    @abstractmethod
    def normalize(self, raw: dict) -> Optional[ProjectRecord]: ...

    def run(self) -> dict:
        logger.info(f"[{self.source_name}] ETL run {self.run_id} starting")
        started_at = datetime.now(timezone.utc)

        try:
            self._connect()
            self._log_run_start(started_at)

            raw_records = self.fetch_raw()
            self.stats["fetched"] = len(raw_records)
            logger.info(f"[{self.source_name}] Fetched {len(raw_records)} raw records")

            for raw in raw_records:
                try:
                    record = self.normalize(raw)
                    if record is None:
                        continue
                    action = self._upsert(record)
                    self.stats[action] += 1
                except Exception as e:
                    # CRITICAL: rollback aborted transaction so next record can proceed
                    try:
                        self.conn.rollback()
                    except Exception:
                        pass
                    logger.warning(f"[{self.source_name}] Skipping record due to error: {e}")

            self._log_run_finish(status="success")
            logger.info(f"[{self.source_name}] Run complete: {self.stats}")
            return self.stats

        except Exception as e:
            logger.error(f"[{self.source_name}] Run failed: {e}", exc_info=True)
            self._log_run_finish(status="failed", error=str(e))
            raise

        finally:
            if self.conn:
                self.conn.close()

    def _upsert(self, record: ProjectRecord) -> str:
        h = record.content_hash()

        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id, content_hash FROM projects WHERE external_id = %s AND source_name = %s",
                (record.external_id, record.source_name)
            )
            existing = cur.fetchone()

            if existing:
                if existing["content_hash"] == h:
                    cur.execute(
                        "UPDATE projects SET last_verified_at = NOW() WHERE id = %s",
                        (existing["id"],)
                    )
                    self.conn.commit()
                    return "skipped"
                else:
                    self._update_project(cur, existing["id"], record, h)
                    self.conn.commit()
                    return "updated"
            else:
                project_id = self._insert_project(cur, record, h)
                self._insert_stakeholders(cur, project_id, record.stakeholders)
                self.conn.commit()
                return "inserted"

    def _insert_project(self, cur, record: ProjectRecord, h: str) -> str:
        # Valid stage and sector values from schema enums
        VALID_STAGES  = {'Planning', 'Tender', 'Awarded', 'Under Construction', 'Completed'}
        VALID_SECTORS = {'Transport', 'Infrastructure', 'Energy', 'Commercial', 'Water',
                         'Sport & Leisure', 'Mixed Use', 'Residential', 'Healthcare', 'Education', 'Other'}
        VALID_REGIONS = {'Middle East', 'Asia Pacific', 'Europe', 'Americas', 'Africa', 'Central Asia', 'Global'}

        stage  = record.stage  if record.stage  in VALID_STAGES  else 'Planning'
        sector = record.sector if record.sector in VALID_SECTORS else 'Infrastructure'
        region = record.region if record.region in VALID_REGIONS else None

        if record.lng and record.lat:
            coords_sql = "ST_SetSRID(ST_MakePoint(%s, %s), 4326)"
            coords_params = [record.lng, record.lat]
        else:
            coords_sql = "NULL"
            coords_params = []

        sql = f"""
            INSERT INTO projects (
                external_id, source_name, source_url, tender_document_url,
                title, description,
                value_usd, value_currency,
                location_display, location_country,
                geog, region, sector, stage,
                timeline_display, milestones,
                content_hash, last_verified_at, first_seen_at
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                {coords_sql}, %s, %s, %s,
                %s, %s::jsonb,
                %s, NOW(), NOW()
            )
            RETURNING id
        """

        params = [
            record.external_id or str(uuid.uuid4()),
            record.source_name,
            record.source_url,
            record.tender_document_url,
            record.title or "Untitled",
            record.description,
            record.value_usd,
            record.value_currency or "USD",
            record.location_display,
            record.location_country,
        ] + coords_params + [
            region,
            sector,
            stage,
            record.timeline_display,
            json.dumps(record.milestones or []),
            h,
        ]

        cur.execute(sql, params)
        return cur.fetchone()["id"]

    def _update_project(self, cur, project_id: str, record: ProjectRecord, h: str):
        sql = """
            UPDATE projects SET
                title            = %s,
                description      = %s,
                value_usd        = %s,
                location_display = %s,
                location_country = %s,
                timeline_display = %s,
                milestones       = %s::jsonb,
                content_hash     = %s,
                last_verified_at = NOW(),
                updated_at       = NOW()
            WHERE id = %s
        """
        cur.execute(sql, [
            record.title, record.description, record.value_usd,
            record.location_display, record.location_country,
            record.timeline_display,
            json.dumps(record.milestones or []),
            h, project_id,
        ])

    def _insert_stakeholders(self, cur, project_id: str, stakeholders: list):
        for s in (stakeholders or []):
            if not isinstance(s, dict):
                continue
            name = s.get("name", "")
            if not name:
                continue
            try:
                cur.execute(
                    "INSERT INTO organisations (name) VALUES (%s) ON CONFLICT DO NOTHING RETURNING id",
                    (name,)
                )
                row = cur.fetchone()
                if not row:
                    cur.execute("SELECT id FROM organisations WHERE name = %s", (name,))
                    row = cur.fetchone()
                if row:
                    cur.execute("""
                        INSERT INTO project_stakeholders (project_id, organisation_id, role)
                        VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (project_id, row["id"], s.get("role", "Owner")))
            except Exception as e:
                logger.warning(f"Stakeholder insert failed: {e}")

    def _connect(self):
        self.conn = psycopg2.connect(self.db_url)
        psycopg2.extras.register_uuid()

    def _log_run_start(self, started_at: datetime):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO etl_runs (id, source_name, started_at, status)
                    VALUES (%s, %s, %s, 'running')
                """, (self.run_id, self.source_name, started_at))
            self.conn.commit()
        except Exception:
            self.conn.rollback()

    def _log_run_finish(self, status: str, error: Optional[str] = None):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE etl_runs SET
                        finished_at = NOW(),
                        status      = %s,
                        inserted    = %s,
                        updated     = %s,
                        skipped     = %s,
                        errors      = %s
                    WHERE id = %s
                """, (
                    status,
                    self.stats["inserted"],
                    self.stats["updated"],
                    self.stats["skipped"],
                    0,
                    self.run_id,
                ))
            self.conn.commit()
        except Exception:
            try:
                self.conn.rollback()
            except Exception:
                pass