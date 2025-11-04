package com.tsbs;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import java.util.HashMap;

/**
 * TSBS Data Source Function with Shared Queue Architecture
 * Single producer reads file, multiple consumers process data in parallel
 */
public class TsbsSourceFunction extends RichParallelSourceFunction<RowData> {
    private static final Map<String, SharedQueueManager> queueManagers = new HashMap<>();
    private static final Object INIT_LOCK = new Object();

    private final String filePath;
    private final String dataType;
    private volatile boolean isRunning = true;

    private transient int subtaskIndex;
    private transient int numSubtasks;
    private transient SharedQueueManager queueManager;

    public TsbsSourceFunction(String filePath, String dataType) {
        this.filePath = filePath;
        this.dataType = dataType;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        this.subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        this.numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

        // Initialize shared queue manager
        initializeQueueManager();

        LogPrinter.debug("Consumer initialized - Data type: " + dataType +
                " - Subtask: " + subtaskIndex + "/" + numSubtasks);
    }

    /**
     * Initialize shared queue manager
     */
    private void initializeQueueManager() {
        synchronized (INIT_LOCK) {
            if (!queueManagers.containsKey(dataType)) {
                queueManagers.put(dataType, new SharedQueueManager(filePath, dataType));
                LogPrinter.debug("Created shared queue manager: " + dataType);
            }
            this.queueManager = queueManagers.get(dataType);

            // Auto-start file reader thread if not running
            if (!queueManager.isReaderRunning()) {
                queueManager.startFileReader();
                LogPrinter.debug("Auto-started file reader thread: " + dataType);
            }
        }
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        LogPrinter.debug("Consumer started processing - Data type: " + dataType +
                " - Subtask: " + subtaskIndex);

        while (isRunning && queueManager.shouldContinueProcessing()) {
            try {
                // Get data from queue (with timeout)
                RowData data = queueManager.poll(100, TimeUnit.MILLISECONDS);

                if (data != null) {
                    // Emit data
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(data);
                    }

                    // Progress reporting
                    if (queueManager.getConsumedCount() % 1000 == 0) {
                        LogPrinter.debug(dataType + " - Consumer " + subtaskIndex +
                                " processed: " + queueManager.getConsumedCount() + " records");
                    }
                } else {
                    // Brief sleep when queue is empty
                    Thread.sleep(10);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        LogPrinter.debug("Consumer completed - Data type: " + dataType +
                " - Subtask: " + subtaskIndex);
    }

    @Override
    public void cancel() {
        isRunning = false;
        LogPrinter.debug("Consumer cancelled - Data type: " + dataType +
                " - Subtask: " + subtaskIndex);
    }

    /**
     * Manually clear queue (static method, can be called externally)
     */
    public static void clearQueue(String dataType) {
        synchronized (INIT_LOCK) {
            if (queueManagers.containsKey(dataType)) {
                queueManagers.get(dataType).clearQueue();
                LogPrinter.debug("Manually cleared queue: " + dataType);
            }
        }
    }

    /**
     * Restart file reading (static method, can be called externally)
     */
    public static void restartReading(String dataType) {
        synchronized (INIT_LOCK) {
            if (queueManagers.containsKey(dataType)) {
                queueManagers.get(dataType).restart();
                LogPrinter.debug("Restarted file reading: " + dataType);
            }
        }
    }

    /**
     * Shutdown all queue managers (static method)
     */
    public static void shutdownAll() {
        synchronized (INIT_LOCK) {
            for (SharedQueueManager manager : queueManagers.values()) {
                manager.shutdown();
            }
            queueManagers.clear();
            LogPrinter.debug("All queue managers shut down");
        }
    }
}

/**
 * Shared Queue Manager
 */
class SharedQueueManager {
    private final String filePath;
    private final String dataType;
    private final BlockingQueue<RowData> dataQueue;
    private volatile Thread fileReaderThread;
    private final AtomicBoolean isReaderRunning = new AtomicBoolean(false);
    private final AtomicLong producedCount = new AtomicLong(0);
    private final AtomicLong consumedCount = new AtomicLong(0);
    private final AtomicBoolean shouldRestart = new AtomicBoolean(false);

    // Queue configuration
    private static final int QUEUE_CAPACITY = 1000000;
    private static final int BATCH_SIZE = 1000;

    public SharedQueueManager(String filePath, String dataType) {
        this.filePath = filePath;
        this.dataType = dataType;
        this.dataQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    }

    /**
     * Start file reader thread
     */
    public void startFileReader() {
        if (isReaderRunning.get()) {
            LogPrinter.debug("File reader thread already running: " + dataType);
            return;
        }

        isReaderRunning.set(true);
        shouldRestart.set(false);

        fileReaderThread = new Thread(() -> {
            LogPrinter.debug("File reader thread started: " + dataType + " - File: " + filePath);

            try {
                do {
                    producedCount.set(0);
                    readFile();

                    // Check if restart is needed
                    if (shouldRestart.get()) {
                        LogPrinter.debug("Restart request detected, re-reading file: " + dataType);
                        shouldRestart.set(false);
                        clearQueue(); // Clear queue and start over
                    } else {
                        break; // Normal completion, exit loop
                    }
                } while (isReaderRunning.get());

                LogPrinter.debug("File reading completed: " + dataType +
                        " - Total records: " + producedCount.get());

            } catch (Exception e) {
                LogPrinter.error("File reading exception: " + dataType + " - " + e.getMessage());
                e.printStackTrace();
            } finally {
                isReaderRunning.set(false);
                LogPrinter.debug("File reader thread ended: " + dataType);
            }
        }, "FileReader-" + dataType);

        fileReaderThread.setDaemon(true);
        fileReaderThread.start();
    }

    /**
     * Read single file
     */
    private void readFile() throws Exception {
        File file = new File(filePath.replace("file://", ""));

        if (!file.exists() || !file.isFile()) {
            LogPrinter.error("File does not exist or is not a file: " + filePath);
            return;
        }

        LogPrinter.debug("Reading file: " + file.getName() + " - Data type: " + dataType);

        try (FileReader fileReader = new FileReader(file);
                BufferedReader bufferedReader = new BufferedReader(fileReader)) {

            String line;
            long lineNumber = 0;
            long recordsAdded = 0;

            while ((line = bufferedReader.readLine()) != null && isReaderRunning.get()) {
                lineNumber++;

                // Skip empty lines and header row
                if (line.trim().isEmpty() || lineNumber == 1)
                    continue;

                // Parse CSV line
                RowData rowData = parseCsvLineToRowData(line);
                if (rowData != null) {
                    // Non-blocking queue insertion
                    if (dataQueue.offer(rowData, 100, TimeUnit.MILLISECONDS)) {
                        recordsAdded++;
                        producedCount.incrementAndGet();
                    } else {
                        // Wait if queue is full
                        Thread.sleep(10);
                    }
                }

                // Batch progress reporting
                if (recordsAdded % BATCH_SIZE == 0) {
                    LogPrinter.debug(dataType + " - File " + file.getName() +
                            " progress: " + recordsAdded + " records");
                }
            }

            LogPrinter.debug(dataType + " - File " + file.getName() +
                    " completed: " + recordsAdded + " records");

        } catch (Exception e) {
            LogPrinter.error("Failed to read file: " + file.getName() + " - " + e.getMessage());
            throw e;
        }
    }

    /**
     * Get data from queue
     */
    public RowData poll(long timeout, TimeUnit unit) throws InterruptedException {
        RowData data = dataQueue.poll(timeout, unit);
        if (data != null) {
            consumedCount.incrementAndGet();
        }
        return data;
    }

    /**
     * Clear queue
     */
    public void clearQueue() {
        dataQueue.clear();
        consumedCount.set(0);
        LogPrinter.debug("Queue cleared: " + dataType);
    }

    /**
     * Restart file reading
     */
    public void restart() {
        shouldRestart.set(true);
        if (!isReaderRunning.get()) {
            startFileReader();
        }
    }

    /**
     * Shutdown queue manager
     */
    public void shutdown() {
        isReaderRunning.set(false);
        if (fileReaderThread != null && fileReaderThread.isAlive()) {
            fileReaderThread.interrupt();
        }
        clearQueue();
        LogPrinter.debug("Queue manager shut down: " + dataType);
    }

    /**
     * Check if processing should continue
     */
    public boolean shouldContinueProcessing() {
        return isReaderRunning.get() || !dataQueue.isEmpty();
    }

    /**
     * Check if reader thread is running
     */
    public boolean isReaderRunning() {
        return isReaderRunning.get();
    }

    /**
     * Get consumed count
     */
    public long getConsumedCount() {
        return consumedCount.get();
    }

    /**
     * Parse CSV line to RowData
     */
    private RowData parseCsvLineToRowData(String line) {
        try (StringReader reader = new StringReader(line);
                CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT)) {

            for (CSVRecord record : parser) {
                return parseRecordToRowData(record, dataType);
            }
        } catch (Exception e) {
            LogPrinter.error("Failed to parse CSV line: " + line);
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Parse record to RowData
     */
    private RowData parseRecordToRowData(CSVRecord record, String dataType) {
        if ("readings".equalsIgnoreCase(dataType)) {
            return parseReadingsRecord(record);
        } else if ("diagnostics".equalsIgnoreCase(dataType)) {
            return parseDiagnosticsRecord(record);
        } else {
            LogPrinter.error("Unknown data type: " + dataType);
            return null;
        }
    }

    private RowData parseReadingsRecord(CSVRecord record) {
        final org.apache.flink.table.data.GenericRowData rowData = new org.apache.flink.table.data.GenericRowData(16);

        try {
            // Parse timestamp (index 0) as milliseconds
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
            LogPrinter.error("Failed to parse readings record: " + record.toString());
            e.printStackTrace();
            return null;
        }

        return rowData;
    }

    private RowData parseDiagnosticsRecord(CSVRecord record) {
        final org.apache.flink.table.data.GenericRowData rowData = new org.apache.flink.table.data.GenericRowData(12);

        try {
            // Parse timestamp (index 0) as milliseconds
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
            LogPrinter.error("Failed to parse diagnostics record: " + record.toString());
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
            LogPrinter.error("Failed to parse long from: '" + value + "'");
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
            LogPrinter.error("Failed to parse double from: '" + value + "'");
            return 0.0;
        }
    }

    private StringData parseString(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return StringData.fromString(value.trim());
    }

    private TimestampData parseTimestampFromMillis(String timestampStr) {
        if (timestampStr == null || timestampStr.trim().isEmpty()) {
            return null;
        }
        try {
            long millis = Long.parseLong(timestampStr.trim());
            return TimestampData.fromEpochMillis(millis);
        } catch (Exception e) {
            LogPrinter.error("Failed to parse timestamp from milliseconds: '" + timestampStr + "'");
            return TimestampData.fromEpochMillis(System.currentTimeMillis());
        }
    }
}