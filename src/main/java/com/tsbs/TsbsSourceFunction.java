package com.tsbs;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import java.io.StringReader;
import java.util.Arrays;

public class TsbsSourceFunction extends RichSourceFunction<RowData> {

    private final String directoryPath;
    private final Integer recordsPerSecond;
    private final String dataType;
    private volatile boolean isRunning = true;

    private transient long intervalStartTimeNs;
    private transient int recordsEmittedThisSecond;
    private final long targetIntervalNs = 1_000_000_000L; // Nanoseconds in 1 second

    public TsbsSourceFunction(String directoryPath, Integer recordsPerSecond, String dataType) {
        this.directoryPath = directoryPath;
        this.recordsPerSecond = recordsPerSecond;
        this.dataType = dataType;
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
                            RowData rowData = parseRecordToRowData(record);
                            if (rowData != null) {
                                ctx.collect(rowData);
                                recordsEmittedThisSecond++;
                            }
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

    private RowData parseRecordToRowData(CSVRecord record) {
        if ("readings".equalsIgnoreCase(dataType)) {
            return parseReadingsRecord(record);
        } else if ("diagnostics".equalsIgnoreCase(dataType)) {
            return parseDiagnosticsRecord(record);
        } else {
            System.err.println("Unknown data type: " + dataType);
            return null;
        }
    }

    private RowData parseReadingsRecord(CSVRecord record) {
        // Create GenericRowData with 16 fields
        final org.apache.flink.table.data.GenericRowData rowData = new org.apache.flink.table.data.GenericRowData(16);

        try {
            // Parse timestamp (index 0) as milliseconds (numeric value, no quotes)
            final String timestampStr = safeGet(record, 0).trim();
            final TimestampData timestampData = parseTimestampFromMillis(timestampStr);
            rowData.setField(0, timestampData);

            // Parse numeric fields
            rowData.setField(1, parseDouble(safeGet(record, 1))); // latitude
            rowData.setField(2, parseDouble(safeGet(record, 2))); // longitude
            rowData.setField(3, parseDouble(safeGet(record, 3))); // elevation
            rowData.setField(4, parseDouble(safeGet(record, 4))); // velocity
            rowData.setField(5, parseDouble(safeGet(record, 5))); // heading
            rowData.setField(6, parseDouble(safeGet(record, 6))); // grade
            rowData.setField(7, parseDouble(safeGet(record, 7))); // fuel_consumption

            // Parse string fields
            rowData.setField(8, parseString(safeGet(record, 8))); // name
            rowData.setField(9, parseString(safeGet(record, 9))); // fleet
            rowData.setField(10, parseString(safeGet(record, 10))); // driver
            rowData.setField(11, parseString(safeGet(record, 11))); // model
            rowData.setField(12, parseString(safeGet(record, 12))); // device_version

            // Parse remaining numeric fields
            rowData.setField(13, parseDouble(safeGet(record, 13))); // load_capacity
            rowData.setField(14, parseDouble(safeGet(record, 14))); // fuel_capacity
            rowData.setField(15, parseDouble(safeGet(record, 15))); // nominal_fuel_consumption

        } catch (Exception e) {
            System.err.println("Failed to parse readings record: " + record.toString());
            e.printStackTrace();
            return null;
        }

        return rowData;
    }

    private RowData parseDiagnosticsRecord(CSVRecord record) {
        // Create GenericRowData with 12 fields
        final org.apache.flink.table.data.GenericRowData rowData = new org.apache.flink.table.data.GenericRowData(12);

        try {
            // Parse timestamp (index 0) as milliseconds (numeric value, no quotes)
            final String timestampStr = safeGet(record, 0).trim();
            final TimestampData timestampData = parseTimestampFromMillis(timestampStr);
            rowData.setField(0, timestampData);

            // Parse numeric fields
            rowData.setField(1, parseDouble(safeGet(record, 1))); // fuel_state
            rowData.setField(2, parseDouble(safeGet(record, 2))); // current_load
            rowData.setField(3, parseLong(safeGet(record, 3))); // status

            // Parse string fields
            rowData.setField(4, parseString(safeGet(record, 4))); // name
            rowData.setField(5, parseString(safeGet(record, 5))); // fleet
            rowData.setField(6, parseString(safeGet(record, 6))); // driver
            rowData.setField(7, parseString(safeGet(record, 7))); // model
            rowData.setField(8, parseString(safeGet(record, 8))); // device_version

            // Parse remaining numeric fields
            rowData.setField(9, parseDouble(safeGet(record, 9))); // load_capacity
            rowData.setField(10, parseDouble(safeGet(record, 10))); // fuel_capacity
            rowData.setField(11, parseDouble(safeGet(record, 11))); // nominal_fuel_consumption

        } catch (Exception e) {
            System.err.println("Failed to parse diagnostics record: " + record.toString());
            e.printStackTrace();
            return null;
        }

        return rowData;
    }

    private String safeGet(CSVRecord record, int index) {
        if (record == null || index >= record.size()) {
            return null;
        }
        String value = record.get(index);
        return (value == null || value.trim().isEmpty() ||
                "null".equalsIgnoreCase(value) || "NULL".equalsIgnoreCase(value)) ? "" : value;
    }

    private Long parseLong(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            System.err.println("Failed to parse long from: '" + value + "'");
            return 0L;
        }
    }

    private Double parseDouble(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            System.err.println("Failed to parse double from: '" + value + "'");
            return 0.0;
        }
    }

    private StringData parseString(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        // Preserve the original string format
        return StringData.fromString(value.trim());
    }

    /**
     * Parse timestamp from milliseconds (numeric value)
     */
    private TimestampData parseTimestampFromMillis(String timestampStr) {
        if (timestampStr == null || timestampStr.trim().isEmpty()) {
            return null;
        }
        try {
            // Parse as milliseconds (numeric value, no quotes)
            long millis = Long.parseLong(timestampStr.trim());
            return TimestampData.fromEpochMillis(millis);
        } catch (Exception e) {
            System.err.println("Failed to parse timestamp from milliseconds: '" + timestampStr + "'");
            return TimestampData.fromEpochMillis(System.currentTimeMillis());
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}