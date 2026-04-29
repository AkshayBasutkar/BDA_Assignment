# Air Traffic Streaming Pipeline Guide

This project is a real-time data pipeline for processing air traffic data. It uses Apache Kafka for message brokering and Apache Spark for stream processing.

## Prerequisites
Before running the pipeline, ensure you have the following installed:
1. **Python 3.8+**
2. **Java (JRE/JDK 11 or 8)** - Required by Apache Spark.
3. **Apache Kafka** - Installed and extracted to your local system (e.g., `C:\kafka`).

Install the required Python dependencies:
```powershell
pip install -r requirements.txt
```

## Step 1: Start the Infrastructure (Kafka)

You need to have Zookeeper and the Kafka Broker running. A PowerShell script is provided to automate this process.

Open your first terminal and run:
```powershell
.\setup_kafka.ps1
```
> **Note:** If your Kafka is not installed at `C:\kafka`, set the `$env:KAFKA_HOME` environment variable before running the script.

Wait for the script to print `Kafka is ready. Topic 'air-traffic-raw' is available.`

## Step 2: Start the Data Producer

The producer acts as the source of our streaming data. You have two options. Open a **new, second terminal** and run one of the following:

**Option A: Simulator (Recommended for testing)**
Generates synthetic flight paths between Indian airports.
```powershell
python simulator_producer.py
```

**Option B: Live OpenSky Data**
Fetches real, live flight telemetry using the OpenSky Network API.
```powershell
python opensky_producer.py
```

Leave this terminal open and running. You will see logs indicating that data is being published to Kafka.

## Step 3: Start the Spark Consumer

The consumer reads the raw data from Kafka, processes it, and computes real-time aggregations (e.g., live counts, airline performance).

Open a **new, third terminal** and run:
```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_consumer.py
```

*Note: The first time you run this, Spark will download the required Kafka dependencies.*

## Viewing the Results

By default, the `spark_consumer.py` script is configured to write its output to CSV files. 

Check the automatically created `output/` directory. Inside, you will see subdirectories for the different streaming aggregates:
- `output/flight_events/` (Raw data)
- `output/live_counts/` (Counts of flight statuses)
- `output/airline_perf/` (Average speed and max altitude by airline)
- `output/airport_traffic/` (Departures by airport)

## Teardown
To stop the pipeline, simply press `Ctrl + C` in the terminals running the Producer and Consumer. You can then close the terminals.
