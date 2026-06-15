# BerkeleyMQTTBridge

Bridges the home MQTT bus into the **BerkeleyHouse** 4K TV kiosk, replacing public-API feeds with your own sensor data where available.

## What it does

BerkeleyHouse has a generic ingest endpoint — `POST /api/ingest/:service_name` — that auto-registers new services and broadcasts their data over Socket.IO to the live TV display. This bridge subscribes to the home MQTT bus and forwards selected topics to that endpoint.

## Forwarded channels

| MQTT topic | Ingest service | TV display |
|------------|----------------|------------|
| `home/alerts/earthquake` | `eqengine-alert` | Hazard layer — local EQ alert marker |
| `home/sensors/seismic/#` | `seismograph` | Hazard layer — live RSAM + STA/LTA |
| `home/alerts/fire-weather/+` | `fire-weather-alert` | Hazard layer — fire weather banner |
| `home/sensors/air/#` | `airgradient` | Air Quality layer — replaces PurpleAir |
| `home/events/bird-audio` | `birdnet` | Any consumer via Socket.IO |
| `home/sensors/environmental-station` | `weather-local` | Weather layer — local station |
| `home/status/#` | `agent-status` | Service registry health view |

## Architecture

```
home MQTT bus (Mosquitto)
  │
  ├─ home/alerts/earthquake ──┐
  ├─ home/sensors/seismic/# ──┤  transform()
  ├─ home/alerts/fire-weather/+─┤  + POST
  ├─ home/sensors/air/# ──────┤        ↓
  ├─ home/events/bird-audio ──┤  POST /api/ingest/:name
  ├─ home/sensors/env-station ┤        ↓
  └─ home/status/# ──────────┘  Socket.IO broadcast
                                 ingest:{name}
                                        ↓
                                 BerkeleyHouse 4K TV
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MQTT_BROKER` | `localhost` | MQTT broker host |
| `MQTT_PORT` | `1883` | MQTT broker port |
| `HOUSE_INGEST_URL` | `http://localhost:5050/api/ingest` | BerkeleyHouse ingest base URL |
| `INGEST_TIMEOUT_SEC` | `3.0` | HTTP POST timeout |
| `LOG_LEVEL` | `INFO` | Log verbosity |

## Running

```bash
# Direct (development)
pip install -e .
MQTT_BROKER=node01.local HOUSE_INGEST_URL=http://node01.local:5050/api/ingest berkeley-mqtt-bridge

# Docker
docker build -t berkeley-mqtt-bridge .
docker run --env-file .env berkeley-mqtt-bridge
```

## Payload transforms

Each channel has a dedicated transform function that shapes MQTT payloads into what BerkeleyHouse expects:

- **EQ alert** → USGS-compatible quake object `{ id, mag, place, lat, lng, _local: true }` — the TV can apply distinct styling for local vs. USGS events
- **RSAM** → `{ rsam, station, channel, sta_lta, engine_state }` — Hazard waveform layer
- **AirGradient** → `{ pm25, co2, tvoc, temp_c, humidity }` — replaces PurpleAir on Air Quality view
- **BirdNET** → `{ node_id, analyzer, detections[] }` — matches the audio-receiver POST format exactly
- **Weather** → `{ temperature_c, humidity, wind_speed_mph, … }` — local station supplement
- **Agent status** → `{ agent, status, version, uptime }` — service health view

All payloads include a `_bridge.source_topic` field for traceability.
