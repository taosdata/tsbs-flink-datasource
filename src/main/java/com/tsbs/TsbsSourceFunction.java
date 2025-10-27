package com.tsbs;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import java.io.StringReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

public class TsbsSourceFunction extends RichSourceFunction<RowData> {

    private final String directoryPath;
    private final Integer recordsPerSecond;
    private volatile boolean isRunning = true;

    private transient long intervalStartTimeNs;
    private transient int recordsEmittedThisSecond;
    private final long targetIntervalNs = 1_000_000_000L; // Nanoseconds in 1 second

    public TsbsSourceFunction(String directoryPath, Integer recordsPerSecond) {
        this.directoryPath = directoryPath;
        this.recordsPerSecond = recordsPerSecond;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        // Initialize rate control variables
        this.intervalStartTimeNs = System.nanoTime();
        this.recordsEmittedThisSecond = 0;
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        final org.apache.flink.core.fs.FileSystem fs = org.apache.flink.core.fs.FileSystem
                .get(new org.apache.flink.core.fs.Path(directoryPath).toUri());
        final org.apache.flink.core.fs.Path[] paths = Arrays
                .stream(fs.listStatus(new org.apache.flink.core.fs.Path(directoryPath)))
                .map(fileStatus -> fileStatus.getPath())
                .toArray(org.apache.flink.core.fs.Path[]::new);

        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

        for (org.apache.flink.core.fs.Path filePath : paths) {
            if (!isRunning)
                break;
            try (final java.io.InputStream in = fs.open(filePath);
                    final java.util.Scanner scanner = new java.util.Scanner(in, "UTF-8")) {
                while (scanner.hasNextLine() && isRunning) {
                    final String line = scanner.nextLine().trim();
                    if (line.isEmpty())
                        continue;

                    try (StringReader reader = new StringReader(line);
                            CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT)) {
                        for (CSVRecord record : parser) {
                            if (!isRunning)
                                break;

                            // Precise rate control
                            controlEmissionRate();

                            // Parse and emit data
                            RowData rowData = parseRecordToRowData(record, formatter);
                            ctx.collect(rowData);

                            recordsEmittedThisSecond++;
                        }
                    } catch (Exception e) {
                        System.err.println("Failed to parse line: " + line);
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * Precise control of data emission rate
     */
    private void controlEmissionRate() throws InterruptedException {
        if (recordsPerSecond == null || recordsPerSecond <= 0) {
            return; // No rate limiting
        }

        long currentTimeNs = System.nanoTime();
        long elapsedTimeNs = currentTimeNs - intervalStartTimeNs;

        // If we've emitted the target number of records but haven't reached 1 second
        // yet, need to wait
        if (recordsEmittedThisSecond >= recordsPerSecond) {
            long timeToWaitNs = targetIntervalNs - elapsedTimeNs;
            if (timeToWaitNs > 0) {
                preciseSleep(timeToWaitNs);
            }
            // Reset counters
            recordsEmittedThisSecond = 0;
            intervalStartTimeNs = System.nanoTime();
            return;
        }

        // Calculate when the next record should be emitted
        long expectedTimeForNextRecord = (long) ((recordsEmittedThisSecond + 1)
                * (targetIntervalNs / (double) recordsPerSecond));
        if (elapsedTimeNs < expectedTimeForNextRecord) {
            long sleepTimeNs = expectedTimeForNextRecord - elapsedTimeNs;
            preciseSleep(sleepTimeNs);
        }
    }

    /**
     * High-precision sleep implementation
     */
    private void preciseSleep(long sleepTimeNs) throws InterruptedException {
        if (sleepTimeNs <= 0)
            return;

        long sleepMs = sleepTimeNs / 1_000_000;
        int sleepNs = (int) (sleepTimeNs % 1_000_000);

        if (sleepMs > 0) {
            Thread.sleep(sleepMs, sleepNs);
        } else if (sleepNs > 0) {
            // For sub-millisecond sleep, use busy-wait for higher precision
            long startTime = System.nanoTime();
            while ((System.nanoTime() - startTime) < sleepTimeNs) {
                // Busy-wait - suitable for short sleep durations
            }
        }
    }

    private RowData parseRecordToRowData(CSVRecord record, DateTimeFormatter formatter) {
        // Parse timestamp (index 0)
        final String timestampStr = record.get(0).trim().replace("\"", ""); // 去除可能的前后空格
        final LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
        final TimestampData timestampData = TimestampData.fromLocalDateTime(localDateTime);

        // Parse numeric fields (indices 1-7, 13-15)
        final double latitude = Double.parseDouble(record.get(1).trim());
        final double longitude = Double.parseDouble(record.get(2).trim());
        final double elevation = Double.parseDouble(record.get(3).trim());
        final double velocity = Double.parseDouble(record.get(4).trim());
        final double heading = Double.parseDouble(record.get(5).trim());
        final double grade = Double.parseDouble(record.get(6).trim());
        final double fuelConsumption = Double.parseDouble(record.get(7).trim());
        final double loadCapacity = Double.parseDouble(record.get(13).trim());
        final double fuelCapacity = Double.parseDouble(record.get(14).trim());
        final double nominalFuelConsumption = Double.parseDouble(record.get(15).trim());

        // Parse string fields (indices 8-12)
        final String name = record.get(8).trim().replace("\"", "");
        final String fleet = record.get(9).trim().replace("\"", "");
        final String driver = record.get(10).trim().replace("\"", "");
        final String model = record.get(11).trim().replace("\"", "");
        final String deviceVersion = record.get(12).trim().replace("\"", "");

        // Create GenericRowData with 16 fields
        final org.apache.flink.table.data.GenericRowData rowData = new org.apache.flink.table.data.GenericRowData(16);

        // Set fields (order exactly matches table creation statement)
        rowData.setField(0, timestampData); // ts
        rowData.setField(1, latitude); // latitude
        rowData.setField(2, longitude); // longitude
        rowData.setField(3, elevation); // elevation
        rowData.setField(4, velocity); // velocity
        rowData.setField(5, heading); // heading
        rowData.setField(6, grade); // grade
        rowData.setField(7, fuelConsumption); // fuel_consumption
        rowData.setField(8, StringData.fromString(name)); // name
        rowData.setField(9, StringData.fromString(fleet)); // fleet
        rowData.setField(10, StringData.fromString(driver)); // driver
        rowData.setField(11, StringData.fromString(model)); // model
        rowData.setField(12, StringData.fromString(deviceVersion)); // device_version
        rowData.setField(13, loadCapacity); // load_capacity
        rowData.setField(14, fuelCapacity); // fuel_capacity
        rowData.setField(15, nominalFuelConsumption); // nominal_fuel_consumption
        return rowData;
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}