"""
Fetch live flight data from OpenSky and publish it to Kafka.
"""

import json
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "air-traffic-raw")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))

# India bounding box: lamin, lomin, lamax, lomax
BBOX = (6.0, 68.0, 37.0, 98.0)
OPENSKY_URL = (
    "https://opensky-network.org/api/states/all"
    f"?lamin={BBOX[0]}&lomin={BBOX[1]}&lamax={BBOX[2]}&lomax={BBOX[3]}"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def fetch_flights():
    try:
        response = requests.get(OPENSKY_URL, timeout=15)
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        print(f"[api] {exc}")
        return []

    flights = []
    for state in data.get("states") or []:
        if state[5] is None or state[6] is None:
            continue

        flights.append(
            {
                "source": "opensky",
                "flight_id": (state[1] or state[0] or "UNKNOWN").strip(),
                "icao24": state[0],
                "origin_country": state[2],
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "latitude": round(state[6], 4),
                "longitude": round(state[5], 4),
                "altitude_ft": round((state[7] or 0) * 3.28084),
                "speed_kmh": round((state[9] or 0) * 3.6),
                "heading_deg": round(state[10] or 0),
                "status": "en_route",
            }
        )
    return flights


def main():
    print(f"OpenSky producer started. Broker={KAFKA_BROKER}, topic={TOPIC}")
    while True:
        flights = fetch_flights()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] fetched {len(flights)} flights")

        for flight in flights:
            producer.send(TOPIC, value=flight)

        producer.flush()
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
