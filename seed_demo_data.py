"""Seed realistic demo data for the Hormuz monitoring dashboard.

Generates synthetic captures that simulate the conflict timeline from event.md:
  - Pre-conflict (Feb 26-27): ~25-30 ships per observation (normal)
  - Conflict onset (Feb 28): sharp drop
  - Escalation (Mar 1-14): 2-5 ships (90%+ blockade)
  - Further deterioration (Mar 15-22): 1-3 ships (95-97% blockade)

Usage:
    python seed_demo_data.py
"""

import json
import os
import random
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# Hormuz polygon bounds for generating realistic positions
LAT_MIN, LAT_MAX = 24.21, 26.41
LON_MIN, LON_MAX = 56.25, 57.30

# Schema (reuse from update_database)
from update_database import _SCHEMA_SQL

INSERT_SQL = """
INSERT INTO captures (
    region, region_name, captured_at, filepath, is_north,
    zoom, status, file_size_kb,
    tiles_total, tiles_ok, tiles_failed,
    tankers, cargos, moving_tankers, moving_cargos,
    markers, detections
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s
)
ON CONFLICT (region, captured_at) DO NOTHING
RETURNING id
"""

INSERT_MARKER_SQL = """
INSERT INTO vessel_positions (capture_id, lat, lon, ship_type, motion)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (capture_id, lat, lon) DO NOTHING
"""


def generate_markers(n_tankers_moving, n_tankers_stationary, n_cargo_moving, n_cargo_stationary):
    """Generate random vessel markers within the Hormuz polygon."""
    markers = []
    configs = [
        (n_tankers_moving, "tanker", "moving"),
        (n_tankers_stationary, "tanker", "stationary"),
        (n_cargo_moving, "cargo", "moving"),
        (n_cargo_stationary, "cargo", "stationary"),
    ]
    for count, ship_type, motion in configs:
        for _ in range(count):
            # Inbound ships tend to be in the northern part, outbound in southern
            if motion == "moving":
                lat = random.uniform(LAT_MIN + 0.5, LAT_MAX)
            else:
                lat = random.uniform(LAT_MIN, LAT_MAX - 0.5)
            lon = random.uniform(LON_MIN, LON_MAX)
            markers.append({
                "lat": round(lat, 4),
                "lon": round(lon, 4),
                "type": ship_type,
                "motion": motion,
            })
    return markers


def get_ship_counts(dt):
    """Return (moving_tankers, stationary_tankers, moving_cargo, stationary_cargo) for a given datetime."""
    day = dt.date()
    conflict_start = datetime(2026, 2, 28).date()
    escalation_1 = datetime(2026, 3, 1).date()
    escalation_2 = datetime(2026, 3, 12).date()
    escalation_3 = datetime(2026, 3, 19).date()

    if day < conflict_start:
        # Normal operations: ~25-30 ships per observation
        mt = random.randint(5, 8)
        st = random.randint(2, 4)
        mc = random.randint(6, 9)
        sc = random.randint(3, 5)
    elif day < escalation_1:
        # Initial shock: rapid drop to ~10-15
        mt = random.randint(2, 4)
        st = random.randint(1, 3)
        mc = random.randint(2, 4)
        sc = random.randint(1, 3)
    elif day < escalation_2:
        # Quasi-blockade: 2-5 ships
        mt = random.randint(0, 1)
        st = random.randint(0, 1)
        mc = random.randint(0, 1)
        sc = random.randint(0, 2)
    elif day < escalation_3:
        # Insurance suspension: 1-3 ships
        mt = random.randint(0, 1)
        st = random.randint(0, 1)
        mc = random.randint(0, 1)
        sc = random.randint(0, 1)
    else:
        # Post South Pars strike: 0-2 ships, mostly stationary
        mt = random.randint(0, 0)
        st = random.randint(0, 1)
        mc = random.randint(0, 1)
        sc = random.randint(0, 1)

    return mt, st, mc, sc


def seed():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(_SCHEMA_SQL)
        conn.commit()

        # Generate observations every 2 hours from Feb 25 to now
        start = datetime(2026, 2, 25, 0, 0, tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        dt = start
        total_captures = 0
        total_markers = 0

        with conn.cursor() as cur:
            while dt <= now:
                mt, st, mc, sc = get_ship_counts(dt)
                markers = generate_markers(mt, st, mc, sc)
                total_ships = mt + st + mc + sc

                row = (
                    "H", "Strait of Hormuz", dt, "", False,
                    12, "success", round(random.uniform(200, 400), 1),
                    1, 1, 0,
                    st, sc, mt, mc,
                    psycopg2.extras.Json(markers),
                    psycopg2.extras.Json([]),
                )
                cur.execute(INSERT_SQL, row)
                result = cur.fetchone()

                if result:
                    capture_id = result[0]
                    total_captures += 1
                    for m in markers:
                        cur.execute(INSERT_MARKER_SQL, (
                            capture_id, m["lat"], m["lon"], m["type"], m["motion"]
                        ))
                        total_markers += 1

                # Every 2 hours, with slight jitter
                dt += timedelta(hours=2, minutes=random.randint(-10, 10))

        conn.commit()
        print(f"Seeded {total_captures} captures with {total_markers} vessel positions")
        print(f"Date range: {start.date()} to {now.date()}")

    finally:
        conn.close()


if __name__ == "__main__":
    seed()
