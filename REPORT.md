# Project Report: Air Traffic Streaming Pipeline

## 1. Executive Summary
The **Air Traffic Streaming Pipeline** is a real-time data engineering project designed to ingest, process, and analyze flight telemetry data. By leveraging a modern tech stack consisting of **Apache Kafka** and **Apache Spark**, the system provides near real-time insights into air traffic patterns, airline performance, and airport activity.

## 2. System Architecture
The architecture follows a decoupled, scalable streaming design:

1.  **Data Producers**: Fetch or generate flight data and publish it to a Kafka topic.
2.  **Message Broker**: **Apache Kafka** acts as the central ingestion hub, ensuring high throughput and fault tolerance.
3.  **Stream Processor**: **Apache Spark Structured Streaming** consumes raw data from Kafka, applies transformations, and calculates aggregates.
4.  **Sinks/Storage**: The processed results are written to **CSV files** (for local analysis) and/or a **PostgreSQL database** (for persistence and BI tool integration).

## 3. Component Details

### 3.1. Data Sources (Producers)
The project supports two types of data producers:
*   **OpenSky Producer (`opensky_producer.py`)**: Connects to the OpenSky Network API to fetch live telemetry for aircraft over India (defined by a bounding box).
*   **Simulator Producer (`simulator_producer.py`)**: Generates synthetic flight paths between major Indian airports (e.g., DEL, BOM, BLR). This is ideal for testing and demonstration when live data is unavailable or inconsistent.

### 3.2. Message Brokering (Kafka)
*   **Topic**: `air-traffic-raw`
*   **Partitions**: 3 (as configured in the setup script)
*   **Role**: Decouples the data ingestion from the processing layer, allowing for independent scaling and resilience.

### 3.3. Stream Processing (Spark)
The `spark_consumer.py` script utilizes Spark Structured Streaming to perform several key operations:
*   **Data Parsing**: Converts JSON payloads from Kafka into a structured DataFrame.
*   **Time-Windowed Aggregation**: Uses event-time processing and watermarking to group data into time windows (e.g., 1-minute or 2-minute intervals).
*   **Aggregations**:
    *   **Live Counts**: Tracks the number of flights per status (climbing, en_route, descending).
    *   **Airline Performance**: Calculates average speed and maximum altitude for each airline.
    *   **Airport Traffic**: Monitors departures from specific airports.

## 4. Data Schema
The system processes the following fields for each flight event:
*   `flight_id`, `icao24`: Unique identifiers for the aircraft.
*   `latitude`, `longitude`, `altitude_ft`, `speed_kmh`, `heading_deg`: Spatial and movement telemetry.
*   `status`: The current phase of the flight (climbing, en_route, etc.).
*   `origin`, `destination`: (Simulator only) Source and target airports.
*   `timestamp`: Event time in ISO format.

## 5. Storage and Visualization
The pipeline supports multiple output modes:
*   **CSV Sinks**: Data is partitioned into directories like `output/flight_events/` and `output/airline_perf/` for easy inspection.
*   **PostgreSQL**: A structured database (`schema.sql`) allows for long-term storage. A specialized view `latest_positions` is provided to facilitate real-time mapping in tools like Power BI or Tableau.

## 6. How to Run the Pipeline
1.  **Environment Setup**: Install dependencies via `pip install -r requirements.txt`.
2.  **Kafka Startup**: Execute `setup_kafka.sh` (or `.ps1`) to start Zookeeper, Kafka, and create the topic.
3.  **Start Producer**: Run `python simulator_producer.py` or `python opensky_producer.py`.
4.  **Start Consumer**: Run the Spark job using `spark-submit`.

## 7. Conclusion and Future Work
This project demonstrates a robust foundation for real-time aerospace analytics. Future iterations could include:
*   **Anomaly Detection**: Identifying unusual flight patterns using ML.
*   **Geo-fencing**: Triggering alerts when aircraft enter restricted zones.
*   **Web Dashboard**: A React-based frontend for live map visualization.
