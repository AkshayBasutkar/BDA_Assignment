$ErrorActionPreference = "Stop"

# Kafka Setup Script for Windows PowerShell
# Expects KAFKA_HOME to point to the Kafka installation directory.

$kafkaHome = if ($env:KAFKA_HOME) { $env:KAFKA_HOME } else { "C:\kafka" }
$broker = "localhost:9092"
$topic = "air-traffic-raw"

$zookeeperScript = Join-Path $kafkaHome "bin\windows\zookeeper-server-start.bat"
$brokerScript = Join-Path $kafkaHome "bin\windows\kafka-server-start.bat"
$topicsScript = Join-Path $kafkaHome "bin\windows\kafka-topics.bat"
$zookeeperConfig = Join-Path $kafkaHome "config\zookeeper.properties"
$brokerConfig = Join-Path $kafkaHome "config\server.properties"

foreach ($path in @($zookeeperScript, $brokerScript, $topicsScript, $zookeeperConfig, $brokerConfig)) {
    if (-not (Test-Path $path)) {
        throw "Kafka file not found: $path`nSet KAFKA_HOME to your Kafka install folder before running this script."
    }
}

Write-Host "[1/3] Starting Zookeeper..."
Start-Process -FilePath $zookeeperScript -ArgumentList $zookeeperConfig
Start-Sleep -Seconds 5

Write-Host "[2/3] Starting Kafka broker..."
Start-Process -FilePath $brokerScript -ArgumentList $brokerConfig
Start-Sleep -Seconds 5

Write-Host "[3/3] Creating topic: $topic"
& $topicsScript --create --if-not-exists --topic $topic --bootstrap-server $broker --partitions 3 --replication-factor 1

Write-Host ""
Write-Host "Kafka is ready. Topic '$topic' is available."
Write-Host ""
Write-Host "Next steps:"
Write-Host "  python simulator_producer.py"
Write-Host "  python opensky_producer.py"
Write-Host "  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_consumer.py"
