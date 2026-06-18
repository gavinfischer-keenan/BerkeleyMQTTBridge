"""
BerkeleyMQTTBridge — bridges the home MQTT bus into BerkeleyHouse (4K TV kiosk).

BerkeleyHouse runs on Express + Socket.IO and has a generic ingest endpoint:
  POST http://localhost:5050/api/ingest/:service_name
  { ...any JSON payload... }

The registry auto-discovers new service names and broadcasts them via:
  Socket.IO event: ingest:{service_name}

This bridge subscribes to selected MQTT topics and transforms + forwards them
so the 4K TV display shows your own sensor data instead of (or alongside)
the public USGS/PurpleAir feeds.

Forwarded channels:
  MQTT home/alerts/earthquake          → /api/ingest/eqengine-alert
  MQTT home/sensors/seismic/#          → /api/ingest/seismograph
  MQTT home/alerts/fire-weather/+      → /api/ingest/fire-weather-alert
  MQTT home/sensors/air/#              → /api/ingest/airgradient
  MQTT home/events/bird-audio          → /api/ingest/birdnet
  MQTT home/sensors/environmental-station → /api/ingest/weather-local
  MQTT home/status/#                   → /api/ingest/agent-status

All payloads are forwarded as-is; topic metadata is added under _bridge.*.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import time
from typing import Any

import paho.mqtt.client as mqtt
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Configuration ──────────────────────────────────────────────────────────────

MQTT_BROKER      = os.getenv("MQTT_BROKER",       "localhost")
MQTT_PORT        = int(os.getenv("MQTT_PORT",      "1883"))
MQTT_CLIENT_ID   = os.getenv("MQTT_CLIENT_ID",     "berkeley-mqtt-bridge")

HOUSE_INGEST_URL = os.getenv("HOUSE_INGEST_URL",   "http://localhost:5050/api/ingest")
INGEST_TIMEOUT   = float(os.getenv("INGEST_TIMEOUT_SEC", "3.0"))
LOG_LEVEL        = os.getenv("LOG_LEVEL",           "INFO")
HOUSE_LAT        = float(os.getenv("HOUSE_LAT", "0.0"))
HOUSE_LNG        = float(os.getenv("HOUSE_LNG", "0.0"))

# ── Topic → ingest service mapping ────────────────────────────────────────────
#
# Each entry:  (mqtt_pattern, ingest_name, transform_fn)
#   mqtt_pattern  — MQTT wildcard pattern to subscribe to
#   ingest_name   — name sent to /api/ingest/{name}  (appears in BerkeleyHouse UI)
#   transform_fn  — optional function(topic, payload) → dict; None = forward as-is
#
# BerkeleyHouse registry display names are auto-set from the ingest name, but
# you can POST with a _meta.displayName key to override (we use _bridge.service).

def _xform_earthquake(topic: str, payload: dict) -> dict:
    """EQ alert → USGS-compatible quake object BerkeleyHouse can map."""
    # BerkeleyHouse Hazard layer expects:  { id, mag, place, time, lat, lng, depth, _local }
    return {
        "id":     f"local-{payload.get('alert_id', 'eq')}",
        "mag":    payload.get("estimated_magnitude", 0.0),
        "place":  f"Local sensor — {payload.get('station', 'BK.BKMO')}",
        "time":   int(time.time() * 1000),
        "lat":    HOUSE_LAT,
        "lng":    HOUSE_LNG,
        "depth":  0.0,
        "_local": True,      # flag so the frontend can style it differently
        "_bridge": {
            "source_topic": topic,
            "severity":     payload.get("severity"),
            "sta_lta":      payload.get("sta_lta_ratio"),
            "s_wave_sec":   payload.get("seconds_until_s_wave"),
            "tts":          payload.get("tts_text", "Earthquake Imminent"),
        },
    }


def _xform_seismograph(topic: str, payload: dict) -> dict:
    """RSAM reading → seismograph telemetry for the TV hazard layer."""
    return {
        "rsam":        payload.get("rsam_value"),
        "station":     payload.get("station_id", payload.get("station")),
        "channel":     payload.get("channel"),
        "network":     payload.get("network", "BK"),
        "timestamp":   payload.get("timestamp"),
        "sta_lta":     payload.get("sta_lta_ratio"),
        "engine_state":payload.get("engine_state"),
        "_bridge":     {"source_topic": topic},
    }


def _xform_airgradient(topic: str, payload: dict) -> dict:
    """AirGradient ONE → air quality payload for BerkeleyHouse."""
    # BerkeleyHouse /api/airquality expects PurpleAir-style data but the ingest
    # generic path just stores raw. We shape it to be useful for the Hazard layer.
    sensor_id = topic.split("/")[-1]   # home/sensors/air/{sensor_id}
    return {
        "sensor_id":  sensor_id,
        "pm25":       payload.get("pm025", payload.get("pm25")),
        "co2":        payload.get("rco2",  payload.get("co2")),
        "tvoc":       payload.get("tvoc"),
        "nox":        payload.get("nox"),
        "temp_c":     payload.get("atmp",  payload.get("temperature_c")),
        "humidity":   payload.get("rhum",  payload.get("humidity")),
        "timestamp":  payload.get("timestamp"),
        "_bridge":    {"source_topic": topic, "source": "airgradient-one"},
    }


def _xform_birdnet(topic: str, payload: dict) -> dict:
    """BirdNET detection event → audio ingest for BerkeleyHouse audio layer."""
    # BerkeleyHouse audio-store expects the same shape as the Python audio-receiver POSTs:
    # { node_id, analyzer, detections: [{species, common_name, confidence, ...}] }
    return {
        "node_id":    payload.get("node_id", "mqtt-bridge"),
        "analyzer":   "birdnet",
        "detections": payload.get("detections", [payload] if "species" in payload else []),
        "node_meta":  payload.get("node_meta", {}),
        "_bridge":    {"source_topic": topic},
    }


def _xform_weather(topic: str, payload: dict) -> dict:
    """Environmental station reading → weather telemetry for BerkeleyHouse."""
    return {
        "temperature_c":  payload.get("temperature_c", payload.get("temp_c")),
        "humidity":       payload.get("humidity"),
        "wind_speed_mph": payload.get("wind_speed_mph"),
        "wind_dir_deg":   payload.get("wind_direction_deg"),
        "pressure_hpa":   payload.get("pressure_hpa"),
        "rain_rate_mmhr": payload.get("rain_rate_mm_hr"),
        "timestamp":      payload.get("timestamp"),
        "station_id":     payload.get("station_id", "mosswood-envstation"),
        "_bridge":        {"source_topic": topic, "source": "envstation"},
    }


def _xform_fire_weather(topic: str, payload: dict) -> dict:
    """Fire weather critical alert → hazard layer alert for BerkeleyHouse."""
    return {
        "alert_type":  "fire_weather",
        "severity":    payload.get("severity", "critical"),
        "title":       payload.get("title", "CRITICAL FIRE WEATHER"),
        "message":     payload.get("message", ""),
        "sensor_id":   payload.get("sensor_id"),
        "timestamp":   payload.get("timestamp"),
        "_bridge":     {"source_topic": topic},
    }


def _xform_agent_status(topic: str, payload: dict) -> dict:
    """Agent heartbeat → service health for the BerkeleyHouse service registry view."""
    # topic = home/status/{agent_name}
    parts = topic.split("/")
    agent = parts[-1] if len(parts) >= 3 else "unknown"
    return {
        "agent":   agent,
        "status":  payload.get("status", "unknown"),
        "version": payload.get("version"),
        "uptime":  payload.get("uptime_sec"),
        "_bridge": {"source_topic": topic},
    }


# ── Routing table ──────────────────────────────────────────────────────────────
# (mqtt_pattern, ingest_service_name, transform_fn)
ROUTES: list[tuple[str, str, Any]] = [
    ("home/alerts/earthquake",           "eqengine-alert",      _xform_earthquake),
    ("home/sensors/seismic/#",           "seismograph",         _xform_seismograph),
    ("home/alerts/fire-weather/+",       "fire-weather-alert",  _xform_fire_weather),
    ("home/sensors/air/#",               "airgradient",         _xform_airgradient),
    ("home/events/bird-audio",           "birdnet",             _xform_birdnet),
    ("home/sensors/environmental-station","weather-local",      _xform_weather),
    ("home/status/#",                    "agent-status",        _xform_agent_status),
]

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
)
log = logging.getLogger("mqtt-bridge")

# ── HTTP session with retry ────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.4,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["POST"],
    )
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session

_session = _make_session()
_stats: dict[str, int] = {}   # service_name → forwarded count


def _post(service_name: str, payload: dict) -> None:
    url = f"{HOUSE_INGEST_URL}/{service_name}"
    try:
        r = _session.post(url, json=payload, timeout=INGEST_TIMEOUT)
        r.raise_for_status()
        _stats[service_name] = _stats.get(service_name, 0) + 1
        log.debug("→ %s  (%d total)", service_name, _stats[service_name])
    except requests.exceptions.ConnectionError:
        log.warning("BerkeleyHouse unreachable — is it running? (%s)", url)
    except requests.exceptions.Timeout:
        log.warning("POST timeout for %s", service_name)
    except requests.exceptions.HTTPError as e:
        log.warning("HTTP %s for %s: %s", e.response.status_code, service_name, e)
    except Exception as e:
        log.error("Unexpected POST error for %s: %s", service_name, e)


# ── MQTT topic matching ────────────────────────────────────────────────────────

def _matches(pattern: str, topic: str) -> bool:
    """Simple MQTT wildcard matching: # matches anything, + matches one level."""
    p_parts = pattern.split("/")
    t_parts = topic.split("/")
    if p_parts[-1] == "#":
        return t_parts[:len(p_parts)-1] == p_parts[:-1]
    if len(p_parts) != len(t_parts):
        return False
    return all(pp == tp or pp == "+" for pp, tp in zip(p_parts, t_parts))


# ── MQTT callbacks ─────────────────────────────────────────────────────────────

def _on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        log.info("Connected to MQTT broker %s:%s", MQTT_BROKER, MQTT_PORT)
        for pattern, _name, _fn in ROUTES:
            client.subscribe(pattern, qos=1)
            log.debug("Subscribed: %s", pattern)
    else:
        log.error("MQTT connect failed, rc=%s", rc)


def _on_disconnect(client, userdata, disconnect_flags, rc, properties=None):
    log.warning("MQTT disconnected (rc=%s) — paho will reconnect", rc)


def _on_message(client, userdata, msg: mqtt.MQTTMessage):
    topic = msg.topic
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        log.warning("Bad payload on %s — skipping", topic)
        return

    for pattern, service_name, transform_fn in ROUTES:
        if _matches(pattern, topic):
            try:
                forwarded = transform_fn(topic, payload) if transform_fn else payload
            except Exception as e:
                log.error("Transform error for %s → %s: %s", topic, service_name, e)
                forwarded = {**payload, "_bridge": {"source_topic": topic, "transform_error": str(e)}}
            _post(service_name, forwarded)
            return   # first matching route wins

    log.debug("No route for topic: %s", topic)


# ── Stats heartbeat ────────────────────────────────────────────────────────────

def _stats_loop(interval: int = 60) -> None:
    while True:
        time.sleep(interval)
        log.info("Forwarded totals: %s", _stats)


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    log.info(
        "BerkeleyMQTTBridge starting — broker=%s:%s → house=%s",
        MQTT_BROKER, MQTT_PORT, HOUSE_INGEST_URL,
    )
    log.info("Routes: %s", [r[1] for r in ROUTES])

    client = mqtt.Client(
        client_id=MQTT_CLIENT_ID,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    client.on_connect    = _on_connect
    client.on_disconnect = _on_disconnect
    client.on_message    = _on_message

    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)

    # Stats thread
    threading.Thread(target=_stats_loop, daemon=True, name="stats").start()

    stop_event = threading.Event()

    def _shutdown(sig, frame):
        log.info("Shutdown signal — disconnecting")
        stop_event.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    client.loop_start()
    stop_event.wait()
    client.loop_stop()
    client.disconnect()
    log.info("Final stats: %s", _stats)
    sys.exit(0)


if __name__ == "__main__":
    main()
