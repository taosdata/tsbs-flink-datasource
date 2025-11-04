package com.tsbs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * TSBS Flink Performance Test Suite
 */
public class TsbsTest {

    /**
     * Command Line Options Configuration Class
     */
    public static class CommandLineOptions {
        @Parameter(names = { "-d1", "--data1" }, description = "Test data file path for readings table")
        public String dataFilePath1 = null;

        @Parameter(names = { "-d2", "--data2" }, description = "Test data file path for diagnostics table")
        public String dataFilePath2 = null;

        @Parameter(names = { "-c", "--config" }, description = "Test YAML configuration file path")
        public String configFilePath = null;

        @Parameter(names = { "-s", "--scenario" }, description = "Execute specific scenario ID only")
        public String scenarioId = null;

        @Parameter(names = { "-o", "--output" }, description = "Output results file path")
        public String outputFilePath = "./tsbs-flink-results.txt";

        @Parameter(names = { "-p", "--parallelism" }, description = "Flink parallelism level (default: 4)")
        public Integer parallelism = 4;

        @Parameter(names = { "-h", "--help" }, description = "Show help information", help = true)
        public boolean help = false;

        @Parameter(names = { "-v", "--version" }, description = "Show version information")
        public boolean version = false;
    }

    /**
     * Test Case Configuration Class - YAML format
     */
    public static class TestCaseConfig {
        @JsonProperty("testCases")
        public List<TestCase> testCases;

        public static class TestCase {
            @JsonProperty("scenarioId")
            public String scenarioId;

            @JsonProperty("classfication")
            public String classification;

            @JsonProperty("description")
            public String description;

            @JsonProperty("sql")
            public String sql;
        }
    }

    /**
     * Test Result Class - Contains classification information
     */
    public static class TestResult {
        public String scenarioId;
        public String classification;
        public String description;
        public long startTime;
        public long endTime;
        public long duration;
        public boolean success;
        public String errorMessage;
        public int recordsProcessed;

        public TestResult(String scenarioId, String classification, String description) {
            this.scenarioId = scenarioId;
            this.classification = classification;
            this.description = description;
            this.success = true;
        }
    }

    /**
     * Test Suite Summary Class
     */
    public static class TestSuiteSummary {
        public long totalStartTime;
        public long totalEndTime;
        public long totalDuration;
        public int totalCases;
        public int passedCases;
        public int failedCases;
        public Map<String, Integer> classificationStats = new HashMap<>();
        public Map<String, Long> classificationDurations = new HashMap<>();
        public Integer parallelism;

        public TestSuiteSummary(long startTime) {
            this.totalStartTime = startTime;
        }
    }

    /**
     * Load YAML configuration file
     */
    public static TestCaseConfig loadTestConfig(String configPath) throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        File configFile = new File(configPath);
        InputStream inputStream;

        if (configFile.exists()) {
            inputStream = new FileInputStream(configFile);
            LogPrinter.log("External test configuration file loaded: " + configFile.getAbsolutePath());
        } else {
            inputStream = TsbsTest.class.getClassLoader().getResourceAsStream(configPath);
            if (inputStream == null) {
                throw new IllegalArgumentException("Configuration file not found: " + configPath +
                        "\nCurrent working directory: " + System.getProperty("user.dir"));
            }
            LogPrinter.log("Embedded test configuration file loaded: " + configPath);
        }

        try (InputStream is = inputStream) {
            return mapper.readValue(is, TestCaseConfig.class);
        }
    }

    /**
     * Extract embedded resource to a temporary file
     */
    private static Path extractResourceToTempFile(String resourcePath, String fileName) throws IOException {
        InputStream resourceStream = TsbsTest.class.getClassLoader().getResourceAsStream(resourcePath);
        if (resourceStream == null) {
            throw new IOException("Embedded resource not found: " + resourcePath);
        }

        Path tempDir = Paths.get(System.getProperty("java.io.tmpdir"), "tsbs-test");
        Files.createDirectories(tempDir);
        Path tempFile = tempDir.resolve(fileName);

        try (InputStream is = resourceStream;
                OutputStream os = Files.newOutputStream(tempFile)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
        }

        tempFile.toFile().deleteOnExit();
        return tempFile;
    }

    /**
     * Get effective output file path (resolve relative paths to current working
     * directory)
     */
    private static String getEffectiveOutputFilePath(String outputFilePath) {
        File outputFile = new File(outputFilePath);
        if (outputFile.isAbsolute()) {
            return outputFilePath;
        } else {
            String currentWorkingDir = System.getProperty("user.dir");
            Path absoluteOutputPath = Paths.get(currentWorkingDir, outputFilePath);
            return absoluteOutputPath.toAbsolutePath().toString();
        }
    }

    /**
     * Execute a single test case
     */
    public static TestResult executeTestCase(StreamTableEnvironment tableEnv,
            TestCaseConfig.TestCase testCase) {

        // Manually clear queues (ensure clean state for each test)
        LogPrinter.debug("Initializing shared queues...");

        TsbsSourceFunction.clearQueue("readings");
        TsbsSourceFunction.clearQueue("diagnostics");
        LogPrinter.debug("Queues cleared");

        TsbsSourceFunction.restartReading("readings");
        TsbsSourceFunction.restartReading("diagnostics");
        LogPrinter.debug("File reading restarted");

        TestResult result = new TestResult(testCase.scenarioId, testCase.classification, testCase.description);
        result.startTime = System.currentTimeMillis();

        testCase.sql = testCase.sql.replace("\n", " ").replaceAll("\\s+", " ");
        LogPrinter.log("   - Starting test case: " + testCase.scenarioId);
        LogPrinter.log("   - Classification: " + testCase.classification);
        if (!testCase.description.isEmpty()) {
            LogPrinter.log("   - Description: " + testCase.description);
        }
        LogPrinter.log("   - SQL: " + testCase.sql);

        try {

            TableResult tableResult = tableEnv.executeSql(testCase.sql);

            // tableResult.await();

            // Collect results and count
            try (CloseableIterator<org.apache.flink.types.Row> iterator = tableResult.collect()) {
                int count = 0;
                LogPrinter.log("Query results:");
                while (iterator.hasNext()) {
                    org.apache.flink.types.Row row = iterator.next();
                    // Limit output rows to avoid log bloat
                    if (count < 5) {
                        LogPrinter.log("   " + row.toString());
                    } else if (count == 5) {
                        LogPrinter.log("   ... (more results omitted)");
                    }
                    count++;
                }
                result.recordsProcessed = count;
            }

            result.endTime = System.currentTimeMillis();
            result.duration = result.endTime - result.startTime;
            result.success = true;

            LogPrinter.log("   - Test passed - Records processed: " + result.recordsProcessed +
                    " | Duration: " + result.duration + "ms");

        } catch (Exception e) {
            result.endTime = System.currentTimeMillis();
            result.duration = result.endTime - result.startTime;
            result.success = false;
            result.errorMessage = e.getMessage();

            LogPrinter.log("   - Test failed - Duration: " + result.duration + "ms");
            LogPrinter.log("   - Error message: " + e.getMessage());
            LogPrinter.debug("Detailed error stack: " + e.getMessage());
            e.printStackTrace();
        }

        LogPrinter.log("   - Waiting  10000 ms for resource release...");
        try {
            Thread.sleep(10000);
            LogPrinter.log("   - Resource release wait completed");
        } catch (InterruptedException e) {
            LogPrinter.log("   - Resource release wait interrupted");
            Thread.currentThread().interrupt();
        }

        LogPrinter.log("---");
        return result;
    }

    /**
     * Execute the complete test suite
     */
    public static TestSuiteSummary executeTestSuite(StreamTableEnvironment tableEnv,
            TestCaseConfig config,
            String specificScenarioId,
            Integer parallelism) {
        TestSuiteSummary summary = new TestSuiteSummary(System.currentTimeMillis());
        summary.parallelism = parallelism;
        List<TestResult> results = new ArrayList<>();

        List<TestCaseConfig.TestCase> testCasesToExecute = config.testCases;

        if (specificScenarioId != null && !specificScenarioId.trim().isEmpty()) {
            testCasesToExecute = config.testCases.stream()
                    .filter(testCase -> specificScenarioId.equals(testCase.scenarioId))
                    .collect(Collectors.toList());

            if (testCasesToExecute.isEmpty()) {
                LogPrinter.log("No test case found with scenario ID: " + specificScenarioId);
                LogPrinter.log("Available scenario IDs: " +
                        config.testCases.stream().map(tc -> tc.scenarioId).collect(Collectors.toList()));
                summary.totalEndTime = System.currentTimeMillis();
                summary.totalDuration = summary.totalEndTime - summary.totalStartTime;
                return summary;
            }

            LogPrinter.log("Executing specific scenario: " + specificScenarioId);
            LogPrinter.log("Filtered test cases: " + testCasesToExecute.size() + " (from total " +
                    config.testCases.size() + ")");
        }

        LogPrinter.log("Starting test suite execution");
        LogPrinter.log("Number of test cases to execute: " + testCasesToExecute.size());
        LogPrinter.log("Suite start time: " + new Date(summary.totalStartTime));
        LogPrinter.log("Parallelism level: " + parallelism);

        Map<String, List<TestCaseConfig.TestCase>> casesByClassification = new HashMap<>();
        for (TestCaseConfig.TestCase testCase : testCasesToExecute) {
            casesByClassification
                    .computeIfAbsent(testCase.classification, k -> new ArrayList<>())
                    .add(testCase);
        }

        LogPrinter.log("Number of classifications: " + casesByClassification.size());
        for (String classification : casesByClassification.keySet()) {
            LogPrinter.log("   - " + classification + ": " +
                    casesByClassification.get(classification).size() + " test cases");
        }

        for (int i = 0; i < testCasesToExecute.size(); i++) {
            TestCaseConfig.TestCase testCase = testCasesToExecute.get(i);
            LogPrinter.log("Execution progress: (" + (i + 1) + "/" + testCasesToExecute.size() + ")");

            TestResult result = executeTestCase(tableEnv, testCase);
            results.add(result);

            // Update classification statistics
            summary.classificationStats.merge(result.classification, 1, Integer::sum);
            summary.classificationDurations.merge(result.classification, result.duration, Long::sum);
        }

        summary.totalEndTime = System.currentTimeMillis();
        summary.totalDuration = summary.totalEndTime - summary.totalStartTime;
        summary.totalCases = results.size();
        summary.passedCases = (int) results.stream().filter(r -> r.success).count();
        summary.failedCases = summary.totalCases - summary.passedCases;

        LogPrinter.log("==========================================");
        LogPrinter.log("Test suite execution completed");
        LogPrinter.log("Total duration: " + summary.totalDuration + "ms");
        LogPrinter.log("Parallelism: " + parallelism);
        LogPrinter.log("==========================================\n\n");

        // Generate detailed report
        generateTestReport(results, summary);

        return summary;
    }

    /**
     * Generate detailed test report
     */
    public static void generateTestReport(List<TestResult> results, TestSuiteSummary summary) {
        LogPrinter.log("Detailed test results summary report");
        LogPrinter.log("==========================================");
        SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        // Group results by classification
        Map<String, List<TestResult>> resultsByClassification = new HashMap<>();
        for (TestResult result : results) {
            resultsByClassification
                    .computeIfAbsent(result.classification, k -> new ArrayList<>())
                    .add(result);
        }

        // Overall statistics
        LogPrinter.log("Overall statistics:");
        LogPrinter.log(" * Total test cases: " + summary.totalCases);
        LogPrinter.log(" * Passed cases: " + summary.passedCases);
        LogPrinter.log(" * Failed cases: " + summary.failedCases);
        LogPrinter.log(" * Success rate: " +
                (summary.totalCases > 0 ? String.format("%.1f", (summary.passedCases * 100.0 / summary.totalCases))
                        : "0")
                + "%");
        LogPrinter.log(" * Total duration: " + summary.totalDuration + "ms (" +
                String.format("%.2f", summary.totalDuration / 1000.0) + " seconds)");
        LogPrinter.log(" * Parallelism level: " + summary.parallelism + "\n");

        // Detailed results table
        LogPrinter.log("Detailed results list:");
        LogPrinter.log(
                "| Scenario ID | Classification | Records | Start Time              | End Time                | Duration(ms) | Status    |");
        LogPrinter.log(
                "|-------------|----------------|---------|-------------------------|-------------------------|--------------|-----------|");

        for (TestResult result : results) {
            String status = result.success ? "Passed" : "Failed";

            LogPrinter.log(String.format("| %-11s | %-14s | %7d | %-19s | %-18s | %12d | %s |",
                    result.scenarioId,
                    result.classification,
                    result.recordsProcessed,
                    timeFormat.format(result.startTime),
                    timeFormat.format(result.endTime),
                    result.duration,
                    status));
        }

        // Performance analysis
        LogPrinter.log("Performance analysis:");
        if (!results.isEmpty()) {
            TestResult fastest = results.stream()
                    .min(Comparator.comparingLong(r -> r.duration))
                    .orElse(results.get(0));
            TestResult slowest = results.stream()
                    .max(Comparator.comparingLong(r -> r.duration))
                    .orElse(results.get(0));

            LogPrinter.log(" * Most time-consuming case: " + slowest.scenarioId + " (" + slowest.classification +
                    ") - " + slowest.duration + "ms");
            LogPrinter.log(" * Fastest case: " + fastest.scenarioId + " (" + fastest.classification +
                    ") - " + fastest.duration + "ms");
            LogPrinter.log(" * Average case duration: " +
                    String.format("%.2f", results.stream().mapToLong(r -> r.duration).average().orElse(0)) + "ms");
        }

        // Failed cases details
        List<TestResult> failedResults = results.stream()
                .filter(r -> !r.success)
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

        if (!failedResults.isEmpty()) {
            LogPrinter.log("Failed cases details:");
            for (TestResult failed : failedResults) {
                LogPrinter.log("• " + failed.scenarioId + " (" + failed.classification + "): " +
                        failed.errorMessage);
            }
        }

        LogPrinter.log("==========================================");
        if (LogPrinter.isOutputToFile()) {
            LogPrinter.log("Full report saved to: " + LogPrinter.getOutputFilePath() + "\n");
        }
    }

    /**
     * Main program entry point
     */
    public static void main(String[] args) {
        System.setProperty("akka.jvm-exit-on-fatal-error", "false");
        System.setProperty("akka.coordinated-shutdown.exit-jvm", "off");

        CommandLineOptions options = new CommandLineOptions();
        JCommander commander = JCommander.newBuilder()
                .addObject(options)
                .build();

        try {
            commander.parse(args);

            if (options.help) {
                commander.usage();
                return;
            }

            if (options.version) {
                System.out.println("TSBS Flink Test v1.0");
                return;
            }

            // Validate parallelism parameter
            if (options.parallelism != null && options.parallelism <= 0) {
                System.err.println("Invalid parallelism value: " + options.parallelism +
                        ". Must be a positive integer.");
                System.exit(1);
            }

            // Initialize file output if specified
            if (options.outputFilePath != null && !options.outputFilePath.trim().isEmpty()) {
                String effectiveOutputPath = getEffectiveOutputFilePath(options.outputFilePath);
                LogPrinter.openFile(effectiveOutputPath);
            }

            LogPrinter.log("Current working directory: " + System.getProperty("user.dir"));
            LogPrinter.log("Output file path: " +
                    (LogPrinter.isOutputToFile() ? LogPrinter.getOutputFilePath()
                            : "Not specified (console only)"));
            LogPrinter.log("Parallelism level: " + options.parallelism);

            // Determine data file paths
            String effectiveDataFilePath1;
            if (options.dataFilePath1 != null && new File(options.dataFilePath1).exists()) {
                effectiveDataFilePath1 = options.dataFilePath1;
                LogPrinter.log("Using external readings data file: " + effectiveDataFilePath1);
            } else {
                Path tempDataFile = extractResourceToTempFile("data/default_readings.csv", "default_readings.csv");
                effectiveDataFilePath1 = tempDataFile.toAbsolutePath().toString();
                LogPrinter.log("Using embedded default readings data file: " + effectiveDataFilePath1);
            }

            String effectiveDataFilePath2;
            if (options.dataFilePath2 != null && new File(options.dataFilePath2).exists()) {
                effectiveDataFilePath2 = options.dataFilePath2;
                LogPrinter.log("Using external diagnostics data file: " + effectiveDataFilePath2);
            } else {
                Path tempDataFile2 = extractResourceToTempFile("data/default_diagnostics.csv",
                        "default_diagnostics.csv");
                effectiveDataFilePath2 = tempDataFile2.toAbsolutePath().toString();
                LogPrinter.log("Using embedded default diagnostics data file: " + effectiveDataFilePath2);
            }

            // Determine config file path
            String effectiveConfigFilePath;
            if (options.configFilePath != null && new File(options.configFilePath).exists()) {
                effectiveConfigFilePath = options.configFilePath;
                LogPrinter.log("Using external config file: " + effectiveConfigFilePath);
            } else {
                effectiveConfigFilePath = "config/default_cases.yaml";
                LogPrinter.log("Using embedded default config file: " + effectiveConfigFilePath);
            }

            // Initialize Flink environment with custom parallelism
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
            env.setParallelism(options.parallelism);

            LogPrinter.log("Initializing Flink test environment");
            LogPrinter.log("Parallelism: " + env.getParallelism());

            // Create test table
            String createTableDDL = "CREATE TABLE readings (\n" +
                    "    `ts` TIMESTAMP(3),\n" +
                    "    latitude DOUBLE,\n" +
                    "    longitude DOUBLE,\n" +
                    "    elevation DOUBLE,\n" +
                    "    velocity DOUBLE,\n" +
                    "    heading DOUBLE,\n" +
                    "    grade DOUBLE,\n" +
                    "    fuel_consumption DOUBLE,\n" +
                    "    name STRING,\n" +
                    "    fleet STRING,\n" +
                    "    driver STRING,\n" +
                    "    model STRING,\n" +
                    "    device_version STRING,\n" +
                    "    load_capacity DOUBLE,\n" +
                    "    fuel_capacity DOUBLE,\n" +
                    "    nominal_fuel_consumption DOUBLE,\n" +
                    "    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" +
                    ") WITH (\n" +
                    "    'connector' = 'tsbs',\n" +
                    "    'data-type' = 'readings',\n" +
                    "    'path' = 'file://" + effectiveDataFilePath1 + "'\n" +
                    ")";

            tableEnv.executeSql(createTableDDL);
            LogPrinter.log("Readings table created successfully");

            // Create diagnostics table
            String createDiagnosticsTableDDL = "CREATE TABLE diagnostics (\n" +
                    "    ts TIMESTAMP(3),\n" +
                    "    fuel_state DOUBLE,\n" +
                    "    current_load DOUBLE,\n" +
                    "    status BIGINT,\n" +
                    "    name VARCHAR(30),\n" +
                    "    fleet VARCHAR(30),\n" +
                    "    driver VARCHAR(30),\n" +
                    "    model VARCHAR(30),\n" +
                    "    device_version VARCHAR(30),\n" +
                    "    load_capacity DOUBLE,\n" +
                    "    fuel_capacity DOUBLE,\n" +
                    "    nominal_fuel_consumption DOUBLE,\n" +
                    "    WATERMARK FOR ts AS ts - INTERVAL '60' MINUTE\n" +
                    ") WITH (\n" +
                    "    'connector' = 'tsbs',\n" +
                    "    'data-type' = 'diagnostics',\n" +
                    "    'path' = 'file://" + effectiveDataFilePath2 + "'\n" +
                    ")";

            tableEnv.executeSql(createDiagnosticsTableDDL);
            LogPrinter.log("Diagnostics table created successfully");

            // Load test configuration
            TestCaseConfig config = loadTestConfig(effectiveConfigFilePath);
            LogPrinter.log("Test configuration loaded successfully");
            LogPrinter.log("Total test cases loaded: " + config.testCases.size());

            // Execute test suite with parallelism parameter
            TestSuiteSummary summary = executeTestSuite(tableEnv, config, options.scenarioId,
                    options.parallelism);

            int exitCode = summary.failedCases > 0 ? 1 : 0;
            LogPrinter.log("Exit code: " + exitCode);
            LogPrinter.log("==========================================");

            System.exit(exitCode);

        } catch (ParameterException e) {
            System.err.println("Parameter error: " + e.getMessage());
            commander.usage();
            System.exit(1);
        } catch (Exception e) {
            LogPrinter.error("Program execution exception: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            // Cleanup shared queues
            TsbsSourceFunction.shutdownAll();
            // Close file output
            LogPrinter.closeFile();
        }
    }
}