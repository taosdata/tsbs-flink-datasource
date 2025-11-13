TSBS Flink Test Environment Setup and Usage Guide

# 1. Java Installation

## 1.1 Install OpenJDK 11

```bash
# Update package list
sudo apt update

# Install OpenJDK 11 (good compatibility with Flink)
sudo apt install openjdk-11-jdk -y

# Verify installation
java -version
```

## 1.2 Configure Environment Variables

```bash
# Find JDK installation path
JDK_PATH=$(readlink -f /usr/bin/java | sed 's:bin/java::')
echo "JDK installation path: $JDK_PATH"

# Set environment variables (add to ~/.bashrc)
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> ~/.bashrc

# Apply configuration
source ~/.bashrc

# Verify environment variables
echo $JAVA_HOME
```

# 2. Maven Installation 

## 2.1 Install Maven

```bash
# Install Maven
sudo apt install maven -y

# Verify installation
mvn -version
```

## 2.2 Configure Maven Mirror and Java Version

Edit `/etc/maven/settings.xmlfile`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 
                              http://maven.apache.org/xsd/settings-1.0.0.xsd">
  
  <!-- Configure Alibaba Cloud mirror for faster downloads -->
  <mirrors>
    <mirror>
      <id>aliyunmaven</id>
      <name>Alibaba Cloud Public Repository</name>
      <url>https://maven.aliyun.com/repository/public</url>
      <mirrorOf>*</mirrorOf>
    </mirror>
  </mirrors>

  <!-- Configure Java 11 compilation environment -->
  <profiles>
    <profile>
      <id>jdk-11</id>
      <activation>
        <activeByDefault>true</activeByDefault>
      </activation>
      <properties>
        <maven.compiler.source>11</maven.compiler.source>
        <maven.compiler.target>11</maven.compiler.target>
        <maven.compiler.compilerVersion>11</maven.compiler.compilerVersion>
      </properties>
    </profile>
  </profiles>
  
  <activeProfiles>
    <activeProfile>jdk-11</activeProfile>
  </activeProfiles>
</settings>
```

## 2.3 Test Maven Configuration

```bash
# Test if configuration works
mvn help:system
```

# 3. Flink Installation

## 3.1 Download and Install

```bash
# Download Flink from Tsinghua mirror
wget https://mirrors.tuna.tsinghua.edu.cn/apache/flink/flink-1.17.2/flink-1.17.2-bin-scala_2.12.tgz

# Verify file integrity
wget https://archive.apache.org/dist/flink/flink-1.17.2/flink-1.17.2-bin-scala_2.12.tgz.sha512
sha512sum -c flink-1.17.2-bin-scala_2.12.tgz.sha512

# Extract installation package
tar -xzf flink-1.17.2-bin-scala_2.12.tgz

# Create symbolic link (for easier management)
ln -s flink-1.17.2 flink
```

## 3.2 Configure Environment Variables

```bash
# Add to ~/.bashrc
echo "export FLINK_HOME=\$HOME/flink" >> ~/.bashrc
echo "export PATH=\$PATH:\$FLINK_HOME/bin" >> ~/.bashrc

# Apply configuration
source ~/.bashrc
```

## 3.3 Optimize Flink Configuration

Edit `$FLINK_HOME/conf/flink-conf.yaml`:

```yaml
# Memory configuration
jobmanager.memory.process.size: 1024m
taskmanager.memory.process.size: 2048m

# Task slot configuration
taskmanager.numberOfTaskSlots: 8
parallelism.default: 4

# Web UI configuration
rest.address: 0.0.0.0
rest.bind-address: 0.0.0.0

env.java.opts: "--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.misc=ALL-UNNAMED"
akka.jvm-exit-on-fatal-error: false

```

# 4. Start Flink Cluster

## 4.1 Start Cluster Service

```bash
# Start Flink standalone cluster
$FLINK_HOME/bin/start-cluster.sh

# Check process status
jps

# Expected output should include:
# - StandaloneSessionClusterEntrypoint (JobManager)
# - TaskManagerRunner (TaskManager)
```

## 4.2 Verify Cluster Status

```bash
# Access Web UI (http://localhost:8081)
# Check available task slots count

# Start SQL Client
$FLINK_HOME/bin/sql-client.sh
```

## 4.3 Test Data Stream Processing

Execute test SQL in SQL Client:
```sql
-- Create test data source
CREATE TABLE page_visits (
    visit_id STRING,
    page_url STRING,
    user_agent STRING,
    `timestamp` TIMESTAMP(3),
    WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '100',
    'fields.visit_id.kind' = 'random',
    'fields.visit_id.length' = '10',
    'fields.page_url.kind' = 'random',
    'fields.page_url.length' = '5'
);

-- Create result table
CREATE TABLE page_visits_per_minute (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    pv BIGINT
) WITH ('connector' = 'print');

-- Execute window aggregation query
SELECT * FROM page_visits;
```

# 5. Compile Custom Data Source Project

Get and Build Project

```bash
# Clone project repository
git clone git@github.com:taosdata/tsbs-flink-datasource.git
cd tsbs-flink-datasource

# Compile project
mvn clean package -f package.pom.xml

# Verify build result
ls -la target/tsbs-flink-datasource-1.0-SNAPSHOT.jar

# Test executability
$FLINK_HOME/bin/flink run target/tsbs-flink-datasource-1.0-SNAPSHOT.jar --help
```

# 6. Debug Custom Data Source via Flink SQL

Open `$FLINK_HOME/bin/sql-client.sh` and execute the following SQL statements:

## 6.1 Register Custom Connector

```sql
-- Load custom connector JAR package
ADD JAR '/root/tsbs-flink-datasource/target/tsbs-flink-datasource-1.0-SNAPSHOT.jar';

-- Verify JAR package loading status
SHOW JARS;
```

## 6.2 Create Test Tables

```sql
-- Create readings data table
CREATE TABLE readings (
    `ts`                         TIMESTAMP(3),
    `latitude`                   DOUBLE,
    `longitude`                  DOUBLE,
    `elevation`                  DOUBLE,
    `velocity`                   DOUBLE,
    `heading`                    DOUBLE,
    `grade`                      DOUBLE,
    `fuel_consumption`           DOUBLE,
    `name`                       STRING,
    `fleet`                      STRING,
    `driver`                     STRING,
    `model`                      STRING,
    `device_version`             STRING,
    `load_capacity`             DOUBLE,
    `fuel_capacity`             DOUBLE,
    `nominal_fuel_consumption`  DOUBLE,
    WATERMARK FOR `ts` AS `ts` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'tsbs',
    'data-type' = 'readings',
    'path' = 'file:///root/tsbs-flink-datasource/src/main/resources/data/default_readings.csv'
);

-- Create diagnostics data table
CREATE TABLE diagnostics (
    ts                          TIMESTAMP(3),
    fuel_state                  DOUBLE,
    current_load               DOUBLE,
    status                     BIGINT,
    name                       VARCHAR(30),
    fleet                      VARCHAR(30),
    driver                     VARCHAR(30),
    model                      VARCHAR(30),
    device_version            VARCHAR(30),
    load_capacity             DOUBLE,
    fuel_capacity             DOUBLE,
    nominal_fuel_consumption  DOUBLE,
    WATERMARK FOR ts AS ts - INTERVAL '60' MINUTE
) WITH (
    'connector' = 'tsbs',
    'data-type' = 'diagnostics',
    'path' = 'file:///root/tsbs-flink-datasource/src/main/resources/data/default_diagnostics.csv'
);
```

## 6.3 Test data query

```sql
SELECT * FROM readings;
```

# 7. Execute Automated Testing

## 7.1 Test Framework Parameter Description

The project supports the following command-line parameters:

| Parameter             | Short | Description                         | Default Value            |
| --------------------- | ----- | ----------------------------------- | ------------------------ |
| --config              | -c    | Test case configuration file path   | Built-in default config  |
| --data1               | -d1   | Readings data file path             | Built-in default data    |
| --data2               | -d2   | Diagnostics data file path          | Built-in default data    |
| --log-output          | -l    | Log file output path                | ./tsbs-flink-log.txt     |
| --json-output         | -j    | JSON result file output path        | ./tsbs-flink-result.json |
| --scenario            | -s    | Execute specific test scenario      | All scenarios            |
| --parallelism         | -p    | Flink parallelism level             | 4                        |
| --parallelism-config  | -pc   | Parallelism configuration file path | Built-in default config  |
| --shared-queue        | -q    | Use shared queue mode               | false                    |
| --help                | -h    | Show help information               | -                        |
| --version             | -v    | Show version information            | -                        |

## 7.2 Execute Test Examples

```bash
# View help information
$FLINK_HOME/bin/flink run target/tsbs-flink-datasource-1.0-SNAPSHOT.jar --help

# Execute all test scenarios
$FLINK_HOME/bin/flink run target/tsbs-flink-datasource-1.0-SNAPSHOT.jar

# Execute specific test scenario with custom parallelism
$FLINK_HOME/bin/flink run target/tsbs-flink-datasource-1.0-SNAPSHOT.jar --scenario A1 --parallelism 2

# Use custom configuration and data files with separate log and JSON output
$FLINK_HOME/bin/flink run target/tsbs-flink-datasource-1.0-SNAPSHOT.jar \
    --config /path/to/custom_config.yaml \
    --data1 /path/to/readings.csv \
    --data2 /path/to/diagnostics.csv \
    --log-output ./custom-log.txt \
    --json-output ./custom-results.json \
    --parallelism 8 \
    --parallelism-config /path/to/parallelism_config.yaml

```

## 7.3 View Test Results

Test results will be output to:

 - Console: Real-time execution logs
 - Log file: Detailed test report with execution logs (default: tsbs-flink-log.txt)
 - JSON file: Structured test results in JSON format (default: tsbs-flink-result.json)

The log file includes execution status, execution time and other detailed information for each scenario, such as:


```
| Scenario ID | Classification | Records   | Data Records | Start Time   | End Time     | Duration(ms) | Throughput(rec/s) | Status |
|-------------|----------------|-----------|--------------|--------------|--------------|--------------|-------------------|--------|
| A1          | Summary        |         1 |           50 | 14:26:01.015 | 14:26:04.130 |         3115 |             16.05 | Passed |
| A2          | Summary        |         4 |           50 | 14:26:07.137 | 14:26:08.416 |         1279 |             39.09 | Passed |
| A3          | Summary        |         8 |           50 | 14:26:11.417 | 14:26:14.402 |         2985 |             16.75 | Passed |
| A4          | Summary        |         7 |           50 | 14:26:17.403 | 14:26:18.366 |          963 |             51.92 | Passed |
| A5          | Summary        |         5 |           50 | 14:26:21.367 | 14:26:22.316 |          949 |             52.69 | Passed |
| A6          | Summary        |         1 |          100 | 14:26:25.317 | 14:26:26.512 |         1195 |             83.68 | Passed |
| A7          | Summary        |         1 |           50 | 14:26:29.514 | 14:26:30.418 |          904 |             55.31 | Passed |
| A8          | Summary        |         1 |           50 | 14:26:33.419 | 14:26:34.560 |         1141 |             43.82 | Passed |
| A9          | Summary        |         0 |           50 | 14:26:37.561 | 14:26:38.596 |         1035 |             48.31 | Passed |
| F1          | Fleet          |         8 |           50 | 14:26:41.597 | 14:26:42.751 |         1154 |             43.33 | Passed |
| F2          | Fleet          |         8 |           50 | 14:26:45.752 | 14:26:48.487 |         2735 |             18.28 | Passed |
| F3          | Fleet          |         3 |           50 | 14:26:51.488 | 14:26:52.358 |          870 |             57.47 | Passed |
| F4          | Fleet          |         3 |          100 | 14:26:55.359 | 14:26:56.435 |         1076 |             92.94 | Passed |
| F5          | Fleet          |         3 |           50 | 14:26:59.436 | 14:27:00.281 |          845 |             59.17 | Passed |
| F6          | Fleet          |        94 |           50 | 14:27:03.282 | 14:27:04.136 |          854 |             58.55 | Passed |
| F7          | Fleet          |       109 |           50 | 14:27:07.137 | 14:27:08.062 |          925 |             54.05 | Passed |
| F8          | Fleet          |        94 |           50 | 14:27:11.063 | 14:27:12.005 |          942 |             53.08 | Passed |
| T1          | Vehicle        |         4 |           50 | 14:27:15.006 | 14:27:15.893 |          887 |             56.37 | Passed |
| T2          | Vehicle        |         3 |           50 | 14:27:18.893 | 14:27:19.799 |          906 |             55.19 | Passed |
| T3          | Vehicle        |         9 |           50 | 14:27:24.618 | 14:27:25.474 |          856 |             58.41 | Passed |
| T4          | Vehicle        |        11 |           50 | 14:27:28.475 | 14:27:29.368 |          893 |             55.99 | Passed |
| T5          | Vehicle        |         3 |           50 | 14:27:32.369 | 14:27:33.198 |          829 |             60.31 | Passed |
| T6          | Vehicle        |         4 |           50 | 14:27:36.199 | 14:27:37.054 |          855 |             58.48 | Passed |
| T7          | Vehicle        |         6 |          100 | 14:27:40.055 | 14:27:41.111 |         1056 |             94.70 | Passed |
| T8          | Vehicle        |         4 |           50 | 14:27:44.112 | 14:27:44.941 |          829 |             60.31 | Passed |
| T9          | Vehicle        |         1 |           50 | 14:27:47.942 | 14:27:48.785 |          843 |             59.31 | Passed |
```

The JSON file contains structured test results with the following format (based on actual test execution):

```json
{
  "summary" : {
    "totalCases" : 2,
    "passedCases" : 2,
    "failedCases" : 0,
    "successRate" : "100.0",
    "totalStartTime" : "2025-11-12 14:26:01.004",
    "totalEndTime" : "2025-11-12 14:27:51.785",
    "totalDuration" : 110781,
    "averageDuration" : "1189.27",
    "totalDataRecords" : 1450,
    "overallThroughput" : "13.09",
    "slowestCase" : {
      "scenarioId" : "A1",
      "duration" : 3115
    }
  },
  "results" : [ {
    "scenarioId" : "A1",
    "classification" : "Summary",
    "records" : 1,
    "recordsInput" : 50,
    "throughput" : "16.05",
    "startTime" : "2025-11-12 14:26:01.015",
    "endTime" : "2025-11-12 14:26:04.130",
    "duration" : 3115,
    "status" : "Passed"
  }, {
    "scenarioId" : "A2",
    "classification" : "Summary",
    "records" : 4,
    "recordsInput" : 50,
    "throughput" : "39.09",
    "startTime" : "2025-11-12 14:26:07.137",
    "endTime" : "2025-11-12 14:26:08.416",
    "duration" : 1279,
    "status" : "Passed"
  } ]
}

```