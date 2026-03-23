#!/usr/bin/env python3
"""Hormuz monitoring daemon — runs the scraper on a schedule and serves the dashboard.

Launches:
  1. FastAPI dashboard server (background thread)
  2. Periodic Hormuz scraping loop (every SCRAPE_INTERVAL_MINUTES, default 120 min)

Usage:
  python hormuz_daemon.py              # Default: scrape every 2 hours
  python hormuz_daemon.py --interval 60   # Scrape every 60 minutes
  python hormuz_daemon.py --once       # Scrape once and exit (don't start server)

The dashboard is available at http://localhost:8000/hormuz
"""

import argparse
import logging
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone

import uvicorn
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("hormuz-daemon")


def run_scrape():
    """Run the scraper for Hormuz region only."""
    logger.info("Starting Hormuz scrape...")
    t0 = time.time()
    result = subprocess.run(
        [sys.executable, "scraper_global.py", "--regions=H"],
        text=True,
        capture_output=True,
    )
    elapsed = time.time() - t0

    # Log key results
    for line in result.stdout.splitlines():
        if any(k in line for k in ["region=H", "Region H", "STOPWATCH", "Database"]):
            logger.info("  %s", line.strip())
    for line in result.stderr.splitlines():
        if "ERROR" in line:
            logger.error("  %s", line.strip())

    if result.returncode == 0:
        logger.info("Scrape completed in %.0fs", elapsed)
    else:
        logger.error("Scrape failed (exit %d) after %.0fs", result.returncode, elapsed)

    return result.returncode == 0


def start_api_server(port=8000):
    """Start the FastAPI server in a background thread."""
    def _run():
        uvicorn.run(
            "api:app",
            host="0.0.0.0",
            port=port,
            log_level="warning",
        )
    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    logger.info("Dashboard server started at http://localhost:%d/hormuz", port)
    return thread


def main():
    parser = argparse.ArgumentParser(description="Hormuz monitoring daemon")
    parser.add_argument("--interval", type=int, default=120,
                        help="Scrape interval in minutes (default: 120)")
    parser.add_argument("--port", type=int, default=8000,
                        help="Dashboard server port (default: 8000)")
    parser.add_argument("--once", action="store_true",
                        help="Run one scrape and exit (no server, no loop)")
    args = parser.parse_args()

    if args.once:
        run_scrape()
        return

    # Start dashboard API server
    start_api_server(args.port)

    # Run initial scrape
    run_scrape()

    # Scraping loop
    logger.info("Entering scrape loop (every %d minutes). Press Ctrl+C to stop.", args.interval)
    try:
        while True:
            next_run = datetime.now(timezone.utc).strftime("%H:%M") + f" + {args.interval}min"
            logger.info("Next scrape at ~%s", next_run)
            time.sleep(args.interval * 60)
            run_scrape()
    except KeyboardInterrupt:
        logger.info("Shutting down.")


if __name__ == "__main__":
    main()
