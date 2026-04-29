-- ─────────────────────────────────────────────
-- schema.sql  —  Air Traffic Analytics DB
-- Optional: use PostgreSQL instead of CSV files
-- so Power BI can connect via DirectQuery
-- ─────────────────────────────────────────────

-- Raw flight events (full history)
CREATE TABLE IF NOT EXISTS flight_events (
    id              SERIAL PRIMARY KEY,
    source          VARCHAR(20),          -- 'opensky' or 'simulator'
    flight_id       VARCHAR(20),
    icao24          VARCHAR(10),
    origin_country  VARCHAR(50),
    event_time      TIMESTAMPTZ,
    latitude        NUMERIC(8, 4),
    longitude       NUMERIC(8, 4),
    altitude_ft     INTEGER,
    speed_kmh       INTEGER,
    heading_deg     INTEGER,
    status          VARCHAR(20),
    origin          VARCHAR(5),
    destination     VARCHAR(5),
    ingested_at     TIMESTAMPTZ DEFAULT NOW()
);

-- Aggregated: flights per status per minute
CREATE TABLE IF NOT EXISTS live_counts (
    window_start    TIMESTAMPTZ,
    window_end      TIMESTAMPTZ,
    status          VARCHAR(20),
    flight_count    INTEGER,
    PRIMARY KEY (window_start, status)
);

-- Aggregated: airline performance per 2-min window
CREATE TABLE IF NOT EXISTS airline_perf (
    window_start    TIMESTAMPTZ,
    window_end      TIMESTAMPTZ,
    airline         VARCHAR(5),
    updates         INTEGER,
    avg_speed_kmh   NUMERIC(6, 1),
    max_altitude_ft INTEGER,
    PRIMARY KEY (window_start, airline)
);

-- Aggregated: busiest origin airports per minute
CREATE TABLE IF NOT EXISTS airport_traffic (
    window_start    TIMESTAMPTZ,
    window_end      TIMESTAMPTZ,
    origin          VARCHAR(5),
    departures      INTEGER,
    PRIMARY KEY (window_start, origin)
);

-- Handy view for Power BI live map
CREATE OR REPLACE VIEW latest_positions AS
SELECT DISTINCT ON (flight_id)
    flight_id, source, latitude, longitude,
    altitude_ft, speed_kmh, status,
    origin, destination, event_time
FROM flight_events
ORDER BY flight_id, event_time DESC;
