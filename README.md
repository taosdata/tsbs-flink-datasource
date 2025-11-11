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

| Parameter      | Short | Description                       | Default Value            |
| -------------- | ----- | --------------------------------- | ------------------------ |
| --config       | -c    | Test case configuration file path | Built-in default config  |
| --data1        | -d1   | Readings data file path           | Built-in default data    |
| --data2        | -d2   | Diagnostics data file path        | Built-in default data    |
| --log-output   | -l    | Log file output path              | ./tsbs-flink-log.txt     |
| --json-output  | -j    | JSON result file output path      | ./tsbs-flink-result.json |
| --scenario     | -s    | Execute specific test scenario    | All scenarios            |
| --parallelism  | -p    | Flink parallelism level           | 4                        |
| --shared-queue | -q    | Use shared queue mode             | false                    |
| --help         | -h    | Show help information             | -                        |
| --version      | -v    | Show version information          | -                        |

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
    --parallelism 8

```

## 7.3 View Test Results

Test results will be output to:

 - Console: Real-time execution logs
 - Log file: Detailed test report with execution logs (default: tsbs-flink-log.txt)
 - JSON file: Structured test results in JSON format (default: tsbs-flink-result.json)

The log file includes execution status, execution time and other detailed information for each scenario, such as:


```
| Scenario ID | Classification | Records | Start Time   | End Time     | Duration(ms) | Status |
| ----------- | -------------- | ------- | ------------ | ------------ | ------------ | ------ |
| A1          | Summary        | 1       | 14:31:19.384 | 14:31:22.812 | 3428         | Passed |
| A2          | Summary        | 4       | 14:31:22.813 | 14:31:23.602 | 789          | Passed |
| A3          | Summary        | 5       | 14:31:23.602 | 14:31:24.187 | 585          | Passed |
| A4          | Summary        | 7       | 14:31:24.187 | 14:31:24.493 | 306          | Passed |
| A5          | Summary        | 5       | 14:31:24.494 | 14:31:25.027 | 533          | Passed |
| A6          | Summary        | 1       | 14:31:25.027 | 14:31:25.720 | 693          | Passed |
| A7          | Summary        | 1       | 14:31:25.721 | 14:31:26.014 | 293          | Passed |
| A8          | Summary        | 1       | 14:31:26.014 | 14:31:26.544 | 530          | Passed |
| A9          | Summary        | 0       | 14:31:26.545 | 14:31:26.925 | 380          | Passed |
| F1          | Fleet          | 6       | 14:31:26.926 | 14:31:27.568 | 642          | Passed |
| F2          | Fleet          | 8       | 14:31:27.569 | 14:31:28.027 | 458          | Passed |
| F3          | Fleet          | 3       | 14:31:28.028 | 14:31:28.374 | 346          | Passed |
| F4          | Fleet          | 3       | 14:31:28.374 | 14:31:28.800 | 426          | Passed |
| F5          | Fleet          | 3       | 14:31:28.801 | 14:31:29.086 | 285          | Passed |
| F6          | Fleet          | 94      | 14:31:29.086 | 14:31:29.391 | 305          | Passed |
| F7          | Fleet          | 81      | 14:31:29.391 | 14:31:29.669 | 278          | Passed |
| F8          | Fleet          | 94      | 14:31:29.669 | 14:31:29.991 | 322          | Passed |
| T1          | Vehicle        | 4       | 14:31:29.992 | 14:31:30.251 | 259          | Passed |
| T2          | Vehicle        | 2       | 14:31:30.251 | 14:31:30.522 | 271          | Passed |
| T3          | Vehicle        | 2       | 14:31:30.523 | 14:31:30.820 | 297          | Passed |
| T4          | Vehicle        | 5       | 14:31:30.820 | 14:31:31.146 | 326          | Passed |
| T5          | Vehicle        | 3       | 14:31:31.146 | 14:31:31.410 | 264          | Passed |
| T6          | Vehicle        | 4       | 14:31:31.410 | 14:31:31.682 | 272          | Passed |
| T7          | Vehicle        | 5       | 14:31:31.682 | 14:31:32.078 | 396          | Passed |
| T8          | Vehicle        | 1       | 14:31:32.079 | 14:31:32.424 | 345          | Passed |
| T9          | Vehicle        | 1       | 14:31:32.424 | 14:31:32.683 | 259          | Passed |

The JSON file contains structured test results with the following format (based on actual test execution):

```json
{
  "summary": {
    "totalCases": 2,
    "passedCases": 2,
    "failedCases": 0,
    "successRate": "100.0",
    "totalStartTime": "2025-11-11 15:59:19.984",
    "totalEndTime": "2025-11-11 15:59:45.556",
    "totalDuration": 25572,
    "averageDuration": "2098.00",
    "slowestCase": {
      "scenarioId": "A1",
      "duration": 2968
    }
  },
  "results": [
    {
      "scenarioId": "A1",
      "classification": "Summary",
      "records": 1,
      "startTime": "2025-11-11 15:59:19.996",
      "endTime": "2025-11-11 15:59:22.964",
      "duration": 2968,
      "status": "Passed"
    },
    {
      "scenarioId": "A2",
      "classification": "Summary",
      "records": 4,
      "startTime": "2025-11-11 15:59:32.969",
      "endTime": "2025-11-11 15:59:34.197",
      "duration": 1228,
      "status": "Passed"
    }
  ]
}
```
