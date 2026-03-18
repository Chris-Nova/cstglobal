"""
etl/scheduler.py

In-process scheduler using APScheduler.
Runs inside the ETL Docker container — no external cron needed.

Schedule (configurable via ETL_SCHEDULE env var):
  Default: every 6 hours
  Override: ETL_SCHEDULE="0 */2 * * *"  (every 2 hours)
"""

import logging
import os
import signal
import sys
import time

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from etl.runner import run_all

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/app/logs/scheduler.log", mode="a"),
    ],
)
logger = logging.getLogger("etl.scheduler")

# Default: "0 */6 * * *" = every 6 hours at minute 0
SCHEDULE = os.environ.get("ETL_SCHEDULE", "0 */6 * * *")

scheduler = BlockingScheduler(timezone="UTC")


def etl_job():
    logger.info("Scheduled ETL job triggered")
    try:
        results = run_all(parallel=True)
        ok  = sum(1 for r in results if r["status"] == "success")
        err = sum(1 for r in results if r["status"] == "failed")
        logger.info(f"ETL job complete: {ok} ok, {err} failed")
    except Exception as e:
        logger.error(f"ETL job error: {e}", exc_info=True)


def handle_shutdown(sig, frame):
    logger.info(f"Received signal {sig} — shutting down scheduler")
    scheduler.shutdown(wait=False)
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    # Parse cron expression from env
    cron_parts = SCHEDULE.strip().split()
    if len(cron_parts) != 5:
        logger.error(f"Invalid ETL_SCHEDULE cron expression: '{SCHEDULE}'")
        sys.exit(1)

    trigger = CronTrigger(
        minute=cron_parts[0],
        hour=cron_parts[1],
        day=cron_parts[2],
        month=cron_parts[3],
        day_of_week=cron_parts[4],
        timezone="UTC",
    )

    scheduler.add_job(etl_job, trigger=trigger, id="etl_full_run", max_instances=1)

    logger.info(f"ETL scheduler started. Schedule: '{SCHEDULE}' (UTC)")

    # Run immediately on startup so DB isn't empty on first deploy
    logger.info("Running initial ETL pass on startup...")
    etl_job()

    scheduler.start()
