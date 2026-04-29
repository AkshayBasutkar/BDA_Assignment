"""
Generate synthetic flight data and publish it to Kafka.
"""

import json
import math
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "air-traffic-raw")
NUM_AIRCRAFT = int(os.getenv("NUM_AIRCRAFT", "20"))
PUBLISH_INTERVAL = float(os.getenv("PUBLISH_INTERVAL", "1.0"))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

AIRPORTS = {
    "DEL": (28.5561, 77.1000),
    "BOM": (19.0896, 72.8656),
    "BLR": (13.1986, 77.7066),
    "MAA": (12.9941, 80.1709),
    "HYD": (17.2403, 78.4294),
    "CCU": (22.6520, 88.4463),
    "AMD": (23.0772, 72.6347),
    "PNQ": (18.5822, 73.9197),
    "COK": (10.1520, 76.4019),
    "GAU": (26.1061, 91.5859),
}

AIRLINES = ["AI", "6E", "SG", "UK", "IX", "G8", "QP", "I5"]


def great_circle_point(lat1, lon1, lat2, lon2, fraction):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    distance = 2 * math.asin(
        math.sqrt(
            math.sin((lat2 - lat1) / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
        )
    )
    if distance < 1e-6:
        return math.degrees(lat1), math.degrees(lon1)

    a_value = math.sin((1 - fraction) * distance) / math.sin(distance)
    b_value = math.sin(fraction * distance) / math.sin(distance)
    x_value = (
        a_value * math.cos(lat1) * math.cos(lon1)
        + b_value * math.cos(lat2) * math.cos(lon2)
    )
    y_value = (
        a_value * math.cos(lat1) * math.sin(lon1)
        + b_value * math.cos(lat2) * math.sin(lon2)
    )
    z_value = a_value * math.sin(lat1) + b_value * math.sin(lat2)
    return (
        math.degrees(math.atan2(z_value, math.sqrt(x_value**2 + y_value**2))),
        math.degrees(math.atan2(y_value, x_value)),
    )


def altitude_profile(fraction):
    if fraction < 0.15:
        return int(fraction / 0.15 * 36000)
    if fraction > 0.85:
        return int((1 - fraction) / 0.15 * 36000)
    return random.randint(34000, 40000)


def status_from_fraction(fraction):
    if fraction < 0.15:
        return "climbing"
    if fraction > 0.85:
        return "descending"
    return "en_route"


class SimulatedFlight:
    def __init__(self):
        self.reset()

    def reset(self):
        origin, destination = random.sample(list(AIRPORTS.keys()), 2)
        self.origin = origin
        self.destination = destination
        self.airline = random.choice(AIRLINES)
        self.flight_id = f"{self.airline}{random.randint(100, 999)}"
        self.fraction = 0.0
        self.speed = random.randint(700, 920)
        self.step = random.uniform(0.004, 0.008)

    def update(self):
        self.fraction = min(1.0, self.fraction + self.step)
        if self.fraction >= 1.0:
            self.reset()
            return None

        origin_lat, origin_lon = AIRPORTS[self.origin]
        dest_lat, dest_lon = AIRPORTS[self.destination]
        latitude, longitude = great_circle_point(
            origin_lat,
            origin_lon,
            dest_lat,
            dest_lon,
            self.fraction,
        )

        return {
            "source": "simulator",
            "flight_id": self.flight_id,
            "icao24": f"sim{self.flight_id.lower()}",
            "origin_country": "India",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "latitude": round(latitude, 4),
            "longitude": round(longitude, 4),
            "altitude_ft": altitude_profile(self.fraction),
            "speed_kmh": self.speed + random.randint(-20, 20),
            "heading_deg": random.randint(0, 359),
            "status": status_from_fraction(self.fraction),
            "origin": self.origin,
            "destination": self.destination,
        }


def main():
    fleet = [SimulatedFlight() for _ in range(NUM_AIRCRAFT)]
    print(f"Simulator started. Broker={KAFKA_BROKER}, topic={TOPIC}")

    tick = 0
    while True:
        tick += 1
        updates = 0
        for aircraft in fleet:
            message = aircraft.update()
            if message:
                producer.send(TOPIC, value=message)
                updates += 1

        producer.flush()
        print(f"[tick {tick:04d}] published {updates} updates")
        time.sleep(PUBLISH_INTERVAL)


if __name__ == "__main__":
    main()
