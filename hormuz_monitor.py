"""Hormuz Strait geopolitical analysis module.

Transforms raw MarineScraper capture data for region "H" (Strait of Hormuz)
into geopolitical analysis dimensions: flow rate, direction (inbound/outbound),
ship type breakdown, scenario mapping, and alert levels.

Design context:
    The Strait of Hormuz carries ~20mb/d of crude oil (~17% of global supply).
    Pre-conflict baseline: ~138 ships/day transiting.  As of 2026-03-22, flow
    has dropped 95-97% due to the US-Iran conflict.  This module provides the
    analytical layer that maps raw ship counts to market-relevant indicators.

Direction heuristic:
    The Hormuz TSS (Traffic Separation Scheme) routes inbound traffic (entering
    the Persian Gulf) through the northern lane (closer to Iran, higher lat)
    and outbound traffic (exiting to Arabian Sea) through the southern lane
    (closer to Oman, lower lat).  We use a latitude threshold as a proxy for
    direction since individual ship headings are not available from pixel
    detection.
"""

import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2.extras
from fastapi import APIRouter, Query

router = APIRouter(prefix="/api/hormuz", tags=["hormuz"])

# ---------------------------------------------------------------------------
# Constants — Pre-conflict baselines & thresholds
# ---------------------------------------------------------------------------

REGION_CODE = "H"

# Pre-conflict baselines (sources: Goldman Sachs, event.md)
BASELINE_SHIPS_PER_DAY = 138
BASELINE_OIL_FLOW_MBD = 20.0   # million barrels/day through Hormuz
BASELINE_LNG_MTPA = 77.0       # Qatar LNG capacity (mtpa)

# Direction classification — latitude-based heuristic
# The narrowed Hormuz polygon spans lat 26.05 to 26.55 (transit channel only).
# Northern lane (lat > threshold) = inbound (进湾, entering Gulf)
# Southern lane (lat < threshold) = outbound (出湾, exiting to Arabian Sea)
DIRECTION_LAT_THRESHOLD = 26.30

# Alert levels — flow as % of baseline
ALERT_LEVELS = [
    {
        "level": "CRITICAL",
        "label": "近乎完全封锁",
        "label_en": "Near-Total Blockade",
        "max_pct": 5,
        "color": "#f85149",
        "oil_scenario": "布伦特 $100-130+, 极端 $160-240",
        "market_impact": "全球衰退风险急剧上升；央行被迫收紧；标普 -10%+",
    },
    {
        "level": "SEVERE",
        "label": "严重中断",
        "label_en": "Severe Disruption",
        "max_pct": 20,
        "color": "#d29922",
        "oil_scenario": "布伦特 $85-100",
        "market_impact": "增长冲击开始定价；美联储推迟降息；价值>成长",
    },
    {
        "level": "ELEVATED",
        "label": "重大中断",
        "label_en": "Major Disruption",
        "max_pct": 50,
        "color": "#e3b341",
        "oil_scenario": "布伦特 $80-90",
        "market_impact": "通胀预期上行；能源板块受益；防御配置",
    },
    {
        "level": "WATCH",
        "label": "流量受限",
        "label_en": "Restricted Flow",
        "max_pct": 75,
        "color": "#58a6ff",
        "oil_scenario": "布伦特 $70-80",
        "market_impact": "风险溢价持续但可控；关注恢复速度",
    },
    {
        "level": "NORMAL",
        "label": "正常运行",
        "label_en": "Normal Operations",
        "max_pct": 100,
        "color": "#3fb950",
        "oil_scenario": "布伦特 $55-70",
        "market_impact": "利空出尽反弹；但流量恢复仍需时间",
    },
]

# Scenario matrix from event.md (probabilities as of 2026-03-22)
SCENARIOS = [
    {
        "id": "fast_deescalation",
        "name": "快速降级",
        "name_en": "Fast De-escalation",
        "probability": "低 (↓)",
        "description": "停火+海峡恢复（需伊朗同意，目前无迹象）",
        "oil_price": "$70s",
        "flow_pct_range": [75, 100],
    },
    {
        "id": "sustained_conflict",
        "name": "持续冲突 4-8周",
        "name_en": "Sustained Conflict 4-8wk",
        "probability": "中",
        "description": "美军继续打击；海峡部分恢复（护航~20%）；普京潜在调停",
        "oil_price": "$85-100",
        "flow_pct_range": [10, 30],
    },
    {
        "id": "escalation",
        "name": "长期化/升级",
        "name_en": "Prolonged / Escalation",
        "probability": "中-高 (↑)",
        "description": "冲突超60天；能源基础设施持续受损；核风险升级",
        "oil_price": "$100-130+",
        "flow_pct_range": [0, 10],
    },
    {
        "id": "nuclear",
        "name": "核升级/扩散",
        "name_en": "Nuclear Escalation",
        "probability": "极低但上升",
        "description": "核设施被攻击引发地区核扩散恐慌",
        "oil_price": "极端",
        "flow_pct_range": [0, 5],
    },
]

# Key events timeline from event.md
KEY_EVENTS = [
    {"date": "2026-02-28", "event": "美以协调对伊全面打击（Epic Fury行动）；哈梅内伊身亡", "severity": "critical"},
    {"date": "2026-03-01", "event": "伊朗准封锁霍尔木兹，海峡通行量 -90%", "severity": "critical"},
    {"date": "2026-03-04", "event": "美军潜艇击沉伊朗军舰", "severity": "high"},
    {"date": "2026-03-08", "event": "莫杰塔巴·哈梅内伊被任命为新最高领袖", "severity": "high"},
    {"date": "2026-03-12", "event": "航运保险暂停，海峡通行进一步恶化", "severity": "high"},
    {"date": "2026-03-14", "event": "冲突两周：布油~$80(+45%)；标普-3.6%", "severity": "medium"},
    {"date": "2026-03-19", "event": "以色列打击伊朗南帕尔斯气田→伊朗报复攻击GCC能源设施", "severity": "critical"},
    {"date": "2026-03-21", "event": "伊朗导弹击中迪莫纳核设施附近(~13km)", "severity": "critical"},
    {"date": "2026-03-22", "event": "伊朗外长：看不到美国停止侵略的迹象", "severity": "medium"},
]


# ---------------------------------------------------------------------------
# Analysis helpers
# ---------------------------------------------------------------------------

def _get_alert_level(flow_pct: float) -> dict:
    """Return the alert level dict matching the current flow percentage."""
    for level in ALERT_LEVELS:
        if flow_pct <= level["max_pct"]:
            return level
    return ALERT_LEVELS[-1]


def _classify_direction(lat: float) -> str:
    """Classify vessel direction based on latitude within the strait.

    Northern lane (higher lat) = inbound to Gulf (进湾)
    Southern lane (lower lat) = outbound to Arabian Sea (出湾)
    """
    return "inbound" if lat > DIRECTION_LAT_THRESHOLD else "outbound"


def _compute_flow_metrics(total_ships: int, tankers: int, cargos: int,
                          moving_tankers: int, moving_cargos: int,
                          scrape_interval_hours: float = 2.0) -> dict:
    """Compute derived flow metrics from a single observation."""
    # Estimate daily flow: observed ships × (24h / observation_interval)
    # This is approximate — a snapshot captures ships present, not throughput
    # Use a conservative multiplier (ships transit in ~6-8 hours)
    daily_estimate = total_ships * (24.0 / max(scrape_interval_hours, 1.0))
    # Cap at a reasonable max (snapshot ≠ throughput; use a dampening factor)
    # Empirically, snapshot count × 4-6 ≈ daily throughput for this zoom level
    daily_estimate = total_ships * 5  # reasonable heuristic

    flow_pct = (daily_estimate / BASELINE_SHIPS_PER_DAY * 100) if BASELINE_SHIPS_PER_DAY > 0 else 0
    flow_pct = min(flow_pct, 200)  # cap at 200% for display

    # Implied oil flow based on flow percentage
    implied_oil_mbd = BASELINE_OIL_FLOW_MBD * (flow_pct / 100.0)
    oil_supply_shock_mbd = BASELINE_OIL_FLOW_MBD - implied_oil_mbd

    total_moving = moving_tankers + moving_cargos
    total_stationary = total_ships - total_moving
    stationary_ratio = (total_stationary / total_ships * 100) if total_ships > 0 else 0

    alert = _get_alert_level(flow_pct)

    return {
        "observed_ships": total_ships,
        "daily_estimate": round(daily_estimate),
        "flow_pct_of_baseline": round(flow_pct, 1),
        "implied_oil_flow_mbd": round(implied_oil_mbd, 1),
        "oil_supply_shock_mbd": round(oil_supply_shock_mbd, 1),
        "stationary_ratio_pct": round(stationary_ratio, 1),
        "alert_level": alert["level"],
        "alert_label": alert["label"],
        "alert_label_en": alert["label_en"],
        "alert_color": alert["color"],
        "oil_scenario": alert["oil_scenario"],
        "market_impact": alert["market_impact"],
    }


def _analyze_vessels(markers: list) -> dict:
    """Analyze a list of vessel markers for direction/type breakdown."""
    inbound = {"tanker": 0, "cargo": 0, "moving": 0, "stationary": 0, "total": 0}
    outbound = {"tanker": 0, "cargo": 0, "moving": 0, "stationary": 0, "total": 0}

    for m in markers:
        lat = m.get("lat")
        if lat is None:
            continue
        direction = _classify_direction(lat)
        bucket = inbound if direction == "inbound" else outbound

        ship_type = m.get("type", "unknown")
        motion = m.get("motion", "unknown")

        bucket["total"] += 1
        if ship_type == "tanker":
            bucket["tanker"] += 1
        elif ship_type == "cargo":
            bucket["cargo"] += 1
        if motion == "moving":
            bucket["moving"] += 1
        else:
            bucket["stationary"] += 1

    return {"inbound": inbound, "outbound": outbound}


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

# We need a reference to the database pool from api.py.
# This will be set by api.py when it includes this router.
_get_conn_func = None


def set_db_connector(get_conn_func):
    """Called by api.py to inject the database connection context manager."""
    global _get_conn_func
    _get_conn_func = get_conn_func


@router.get("/status")
def hormuz_status():
    """Current Hormuz Strait status with full geopolitical analysis dimensions.

    Returns: alert level, flow metrics, direction breakdown, type breakdown,
    scenario mapping, and key context.
    """
    with _get_conn_func() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Latest capture for Hormuz
            cur.execute("""
                SELECT id, captured_at, tankers, cargos, moving_tankers, moving_cargos,
                       markers, status, zoom
                FROM captures
                WHERE region = %s
                ORDER BY captured_at DESC
                LIMIT 1
            """, (REGION_CODE,))
            latest = cur.fetchone()

            # Previous capture for delta comparison
            cur.execute("""
                SELECT tankers, cargos, moving_tankers, moving_cargos, captured_at
                FROM captures
                WHERE region = %s
                ORDER BY captured_at DESC
                LIMIT 1 OFFSET 1
            """, (REGION_CODE,))
            previous = cur.fetchone()

            # 24h average
            cur.execute("""
                SELECT AVG(tankers + cargos + moving_tankers + moving_cargos)::int AS avg_total,
                       AVG(tankers)::int AS avg_tankers,
                       COUNT(*) AS observation_count
                FROM captures
                WHERE region = %s
                      AND captured_at >= NOW() - INTERVAL '24 hours'
            """, (REGION_CODE,))
            avg_24h = cur.fetchone()

    if not latest:
        return {
            "status": "no_data",
            "message": "No Hormuz captures available. Run the scraper with --regions H",
        }

    total = (latest["tankers"] + latest["cargos"]
             + latest["moving_tankers"] + latest["moving_cargos"])
    tankers_total = latest["tankers"] + latest["moving_tankers"]
    cargos_total = latest["cargos"] + latest["moving_cargos"]

    # Flow metrics
    flow = _compute_flow_metrics(
        total, tankers_total, cargos_total,
        latest["moving_tankers"], latest["moving_cargos"],
    )

    # Direction & type analysis from markers
    markers = latest["markers"] if isinstance(latest["markers"], list) else []
    direction = _analyze_vessels(markers)

    # Delta vs previous observation
    delta = None
    if previous:
        prev_total = (previous["tankers"] + previous["cargos"]
                      + previous["moving_tankers"] + previous["moving_cargos"])
        delta = {
            "total": total - prev_total,
            "tankers": tankers_total - (previous["tankers"] + previous["moving_tankers"]),
            "previous_captured_at": previous["captured_at"].isoformat(),
        }

    # Match current flow to scenario
    matched_scenario = None
    for s in SCENARIOS:
        lo, hi = s["flow_pct_range"]
        if lo <= flow["flow_pct_of_baseline"] <= hi:
            matched_scenario = s
            break
    if not matched_scenario:
        matched_scenario = SCENARIOS[-1] if flow["flow_pct_of_baseline"] <= 5 else SCENARIOS[0]

    return {
        "captured_at": latest["captured_at"].isoformat(),
        "capture_status": latest["status"],
        "flow": flow,
        "ships": {
            "total": total,
            "tankers": tankers_total,
            "cargos": cargos_total,
            "moving_tankers": latest["moving_tankers"],
            "moving_cargos": latest["moving_cargos"],
            "stationary_tankers": latest["tankers"],
            "stationary_cargos": latest["cargos"],
        },
        "direction": direction,
        "delta": delta,
        "avg_24h": {
            "total": avg_24h["avg_total"] or 0,
            "tankers": avg_24h["avg_tankers"] or 0,
            "observation_count": avg_24h["observation_count"],
        },
        "matched_scenario": matched_scenario,
        "baselines": {
            "ships_per_day": BASELINE_SHIPS_PER_DAY,
            "oil_flow_mbd": BASELINE_OIL_FLOW_MBD,
        },
    }


@router.get("/flow")
def hormuz_flow_history(
    hours: int = Query(168, ge=1, le=720),
    granularity: str = Query("hourly", pattern="^(hourly|daily)$"),
):
    """Time series of Hormuz flow metrics for charting.

    Returns bucketed flow data with all analysis dimensions computed per bucket.
    """
    trunc = "hour" if granularity == "hourly" else "day"
    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    with _get_conn_func() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT date_trunc(%s, captured_at) AS bucket,
                       AVG(tankers + moving_tankers)::numeric AS avg_tankers,
                       AVG(cargos + moving_cargos)::numeric AS avg_cargos,
                       AVG(moving_tankers + moving_cargos)::numeric AS avg_moving,
                       AVG(tankers + cargos)::numeric AS avg_stationary,
                       AVG(tankers + cargos + moving_tankers + moving_cargos)::numeric AS avg_total,
                       MAX(tankers + cargos + moving_tankers + moving_cargos) AS max_total,
                       MIN(tankers + cargos + moving_tankers + moving_cargos) AS min_total,
                       COUNT(*) AS observations
                FROM captures
                WHERE region = %s AND captured_at >= %s
                GROUP BY bucket
                ORDER BY bucket
            """, (trunc, REGION_CODE, since))
            rows = cur.fetchall()

    series = []
    for row in rows:
        avg_total = float(row["avg_total"] or 0)
        avg_tankers = float(row["avg_tankers"] or 0)
        daily_est = round(avg_total * 5)
        flow_pct = round(daily_est / BASELINE_SHIPS_PER_DAY * 100, 1) if BASELINE_SHIPS_PER_DAY > 0 else 0
        implied_oil = round(BASELINE_OIL_FLOW_MBD * flow_pct / 100, 1)

        series.append({
            "timestamp": row["bucket"].isoformat(),
            "avg_total": round(avg_total, 1),
            "avg_tankers": round(avg_tankers, 1),
            "avg_cargos": round(float(row["avg_cargos"] or 0), 1),
            "avg_moving": round(float(row["avg_moving"] or 0), 1),
            "avg_stationary": round(float(row["avg_stationary"] or 0), 1),
            "max_total": row["max_total"] or 0,
            "min_total": row["min_total"] or 0,
            "daily_estimate": daily_est,
            "flow_pct": min(flow_pct, 200),
            "implied_oil_mbd": implied_oil,
            "observations": row["observations"],
        })

    return {
        "region": REGION_CODE,
        "granularity": granularity,
        "hours": hours,
        "series": series,
        "baselines": {
            "ships_per_day": BASELINE_SHIPS_PER_DAY,
            "oil_flow_mbd": BASELINE_OIL_FLOW_MBD,
        },
        "alert_thresholds": [
            {"level": a["level"], "pct": a["max_pct"], "color": a["color"], "label": a["label"]}
            for a in ALERT_LEVELS
        ],
    }


@router.get("/direction")
def hormuz_direction_history(
    hours: int = Query(168, ge=1, le=720),
):
    """Time series of inbound vs outbound vessel counts based on position heuristic."""
    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    with _get_conn_func() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT captured_at, markers
                FROM captures
                WHERE region = %s AND captured_at >= %s
                ORDER BY captured_at
            """, (REGION_CODE, since))
            rows = cur.fetchall()

    series = []
    for row in rows:
        markers = row["markers"] if isinstance(row["markers"], list) else []
        analysis = _analyze_vessels(markers)
        series.append({
            "timestamp": row["captured_at"].isoformat(),
            "inbound": analysis["inbound"],
            "outbound": analysis["outbound"],
        })

    return {
        "series": series,
        "methodology": (
            f"Direction classified by latitude threshold ({DIRECTION_LAT_THRESHOLD}°N). "
            "Northern lane = inbound (进湾, entering Gulf), "
            "Southern lane = outbound (出湾, exiting to Arabian Sea). "
            "This is a heuristic based on the Hormuz TSS routing convention."
        ),
    }


@router.get("/context")
def hormuz_context():
    """Static geopolitical context: scenarios, events, baselines, alert level definitions."""
    return {
        "scenarios": SCENARIOS,
        "key_events": KEY_EVENTS,
        "alert_levels": ALERT_LEVELS,
        "baselines": {
            "ships_per_day": BASELINE_SHIPS_PER_DAY,
            "oil_flow_mbd": BASELINE_OIL_FLOW_MBD,
            "lng_mtpa": BASELINE_LNG_MTPA,
        },
        "methodology": {
            "direction": (
                f"Latitude threshold at {DIRECTION_LAT_THRESHOLD}°N. "
                "Based on Hormuz TSS: northern lane = inbound, southern lane = outbound."
            ),
            "daily_estimate": (
                "Snapshot count × 5 (heuristic multiplier). "
                "Ships transit Hormuz in ~6-8 hours; at zoom-12 the monitoring area "
                "captures a cross-section of traffic."
            ),
            "oil_flow": (
                "Implied oil flow = baseline (20mb/d) × flow percentage. "
                "Tanker-weighted estimates would be more precise but require vessel size data."
            ),
        },
    }
