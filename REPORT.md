# Detailed Project Report: Air Traffic Streaming Pipeline

## 1. Executive Summary
The **Air Traffic Streaming Pipeline** is an end-to-end data engineering solution designed for the real-time ingestion, processing, and analysis of aviation telemetry. Utilizing **Apache Kafka** as a distributed message broker and **Apache Spark Structured Streaming** for low-latency analytics, the system transforms raw flight data into actionable insights regarding airline efficiency, airport congestion, and live flight tracking.

---

## 2. System Architecture & Data Flow

### 2.1. High-Level Architecture
The system follows a classic Lambda-lite architecture for streaming:
1.  **Ingestion Layer**: Python-based producers fetch or simulate data.
2.  **Transport Layer**: Apache Kafka provides a fault-tolerant, persistent buffer.
3.  **Processing Layer**: Spark Structured Streaming performs micro-batch processing.
4.  **Serving Layer**: Results are persisted to CSV for batch analysis or PostgreSQL for live dashboarding.

### 2.2. Data Lifecycle
1.  **Production**: Producers package telemetry into JSON objects.
2.  **Transmission**: Data is sent to the `air-traffic-raw` Kafka topic.
3.  **Consumption**: Spark reads the stream, enforces a schema, and calculates rolling window aggregates.
4.  **Sink**: Processed data is written out every 10 seconds (configurable).

---

## 3. Detailed Component Analysis

### 3.1. Data Producers

#### 3.1.1. OpenSky Producer (`opensky_producer.py`)
*   **Source**: OpenSky Network REST API (`/states/all`).
*   **Geofencing**: Limited to a bounding box over India: `(6.0, 68.0)` to `(37.0, 98.0)`.
*   **Polling**: Default interval of 10 seconds to comply with API rate limits.
*   **Transformation**: Converts metric SI units (meters/sec) to aviation standards (Feet, KM/H).

#### 3.1.2. Simulator Producer (`simulator_producer.py`)
*   **Purpose**: Ensures pipeline availability without external API dependency.
*   **Mathematical Models**:
    *   **Trajectory**: Uses **Great Circle distance** calculations to interpolate movement between 10 major Indian airports (DEL, BOM, BLR, etc.).
    *   **Altitude Profile**: Simulates realistic flight phases (Climbing if < 15% distance, Descending if > 85%, else Cruise).
    *   **Telemetry**: Random jitter is added to speed and heading to simulate real-world variability.
*   **Concurrency**: Simulates multiple aircraft (default: 20) simultaneously.

### 3.2. Processing Engine (`spark_consumer.py`)

#### 3.2.1. Schema Enforcement
Spark enforces a strict schema on the incoming Kafka JSON strings to ensure data quality:
```python
StructType([
    StructField("source", StringType()),
    StructField("flight_id", StringType()),
    StructField("latitude", DoubleType()),
    StructField("altitude_ft", IntegerType()),
    # ... and 9 other fields
])
```

#### 3.2.2. Windowed Aggregations
The engine maintains state across windows using **Event-Time Processing**:
*   **Live Counts**: 1-minute windows tracking flight status distributions. Uses `Complete` output mode to update the full status set.
*   **Airline Performance**: 2-minute windows calculating `avg(speed)` and `max(altitude)` per airline code (derived from `flight_id` prefix).
*   **Airport Traffic**: 1-minute windows filtering for non-null origin airports to count departures.

#### 3.2.3. Checkpointing
Fault tolerance is achieved through directory-based checkpointing in `./checkpoints/`. This allows the consumer to resume exactly where it left off after a failure or restart.

---

## 4. Technical Configuration

### 4.1. Environment Variables
| Variable | Description | Default |
| :--- | :--- | :--- |
| `KAFKA_BROKER` | Address of the Kafka broker | `localhost:9092` |
| `KAFKA_TOPIC` | Topic name for raw data | `air-traffic-raw` |
| `SINK` | Destination: `csv`, `postgres`, or `both` | `csv` |
| `TRIGGER_INTERVAL` | Processing frequency | `10 seconds` |
| `OUTPUT_DIR` | Local path for CSV output | `./output` |
| `PG_HOST`/`PG_DB` | PostgreSQL connection details | `localhost`/`air_traffic` |

### 4.2. Database Schema (`schema.sql`)
The PostgreSQL sink populates four primary tables:
1.  `flight_events`: The "Bronze" layer containing raw historical telemetry.
2.  `live_counts`: Aggregated flight status counts.
3.  `airline_perf`: Metrics by airline carrier.
4.  `airport_traffic`: Departure counts by airport code.
5.  **`latest_positions` (View)**: Utilizes `DISTINCT ON (flight_id)` to provide the most recent location for every active aircraft, optimized for live map rendering.

---

## 5. Operations & Troubleshooting

### 5.1. Setup Procedure
1.  **Infrastructure**: Use `setup_kafka.sh` to initialize Zookeeper and Kafka. Ensure `KAFKA_HOME` is set correctly.
2.  **Dependencies**: `pip install -r requirements.txt` (requires `pyspark`, `kafka-python`, `psycopg2-binary`).
3.  **Spark Submit**: Must include the Kafka connector package:
    ```bash
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_consumer.py
    ```

### 5.2. Common Issues
*   **Kafka Connectivity**: Ensure the broker is fully initialized before starting producers. `setup_kafka.sh` includes a 5-second sleep, but slow systems may need more.
*   **PostgreSQL Authentication**: If using `postgres` sink, ensure the `PG_PASSWORD` env var is set or the user has passwordless access.
*   **Spark Performance**: `spark.sql.shuffle.partitions` is set to `4` for local development; increase this for production clusters.

---

## 6. Future Enhancements
*   **Watermarking**: Explicitly define `withWatermark` in Spark to handle late-arriving data more gracefully.
*   **Security**: Implement SSL/TLS for Kafka communication and SASL authentication.
*   **Visualization**: Build a Grafana dashboard or a Leaflet.js frontend to consume the `latest_positions` view.
