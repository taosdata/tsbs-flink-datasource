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

public class TsbsTest {

    /**
     * Command Line Options Configuration Class
     */
    public static class CommandLineOptions {
        @Parameter(names = { "-d", "--data" }, description = "Test data file path")
        public String dataFilePath = null;

        @Parameter(names = { "-c", "--config" }, description = "Test YAML configuration file path")
        public String configFilePath = null;

        @Parameter(names = { "-o", "--output" }, description = "Output results file path")
        public String outputFilePath = "./tsbs-flink-results.txt";

        @Parameter(names = { "-h", "--help" }, description = "Show help information", help = true)
        public boolean help = false;

        @Parameter(names = { "-v", "--version" }, description = "Show version information")
        public boolean version = false;
    }

    /**
     * Output Manager - Handles both console and file output
     */
    private static class OutputManager {
        private Date curDate;
        private SimpleDateFormat dateFormat;
        private String outputFilePath;
        private PrintWriter fileWriter;

        public OutputManager(String baseFilePath) throws IOException {
            this.curDate = new Date();
            this.dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            this.outputFilePath = baseFilePath;

            File outputFile = new File(outputFilePath);
            if (outputFile.exists()) {
                try (FileWriter fw = new FileWriter(outputFile, false)) {
                    fw.write(""); // Clear file content
                }
            }

            this.fileWriter = new PrintWriter(new FileWriter(outputFilePath, true));
            log("📁 Output file created: " + outputFilePath);
        }

        public void log(String message) {
            String timestampedMessage = "[" + dateFormat.format(curDate) + "] " + message;
            System.out.println(timestampedMessage);
            fileWriter.println(timestampedMessage);
            fileWriter.flush();
        }

        public String getOutputFilePath() {
            return outputFilePath;
        }

        public void close() {
            if (fileWriter != null) {
                fileWriter.close();
                log("✅ Output file closed: " + outputFilePath);
            }
        }
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

        public TestSuiteSummary(long startTime) {
            this.totalStartTime = startTime;
        }
    }

    /**
     * Load YAML configuration file
     */
    public static TestCaseConfig loadTestConfig(String configPath, OutputManager outputManager) throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        File configFile = new File(configPath);
        InputStream inputStream;

        if (configFile.exists()) {
            inputStream = new FileInputStream(configFile);
            outputManager.log("✅ External test configuration file loaded: " + configFile.getAbsolutePath());
        } else {
            inputStream = TsbsTest.class.getClassLoader().getResourceAsStream(configPath);
            if (inputStream == null) {
                throw new IllegalArgumentException("Configuration file not found: " + configPath +
                        "\nCurrent working directory: " + System.getProperty("user.dir"));
            }
            outputManager.log("✅ Embedded test configuration file loaded: " + configPath);
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
            TestCaseConfig.TestCase testCase,
            OutputManager outputManager) {
        TestResult result = new TestResult(testCase.scenarioId, testCase.classification, testCase.description);
        result.startTime = System.currentTimeMillis();

        testCase.sql = testCase.sql.replace("\n", " ").replaceAll("\\s+", " ");
        outputManager.log("🚀 - Starting test case: " + testCase.scenarioId);
        outputManager.log("📂 - Classification: " + testCase.classification);
        if (!testCase.description.isEmpty()) {
            outputManager.log("📋 - Description: " + testCase.description);
        }
        outputManager.log("🔍 - SQL: " + testCase.sql);

        try {
            TableResult tableResult = tableEnv.executeSql(testCase.sql);
            tableResult.await();

            // Collect results and count
            try (CloseableIterator<org.apache.flink.types.Row> iterator = tableResult.collect()) {
                int count = 0;
                outputManager.log("📊 Query results:");
                while (iterator.hasNext()) {
                    org.apache.flink.types.Row row = iterator.next();
                    outputManager.log("   " + row.toString());
                    count++;
                    // Limit output rows to avoid log bloat
                    if (count >= 5) {
                        outputManager.log("   ... (more results omitted)");
                        break;
                    }
                }
                result.recordsProcessed = count;
            }
            // tableResult.print();

            result.endTime = System.currentTimeMillis();
            result.duration = result.endTime - result.startTime;
            result.success = true;

            outputManager.log("✅ - Test passed - Records processed: " + result.recordsProcessed +
                    " | Duration: " + result.duration + "ms");

        } catch (Exception e) {
            result.endTime = System.currentTimeMillis();
            result.duration = result.endTime - result.startTime;
            result.success = false;
            result.errorMessage = e.getMessage();

            outputManager.log("❌ - Test failed - Duration: " + result.duration + "ms");
            outputManager.log("💥 - Error message: " + e.getMessage());
            // Do not print full stack trace to file to avoid log bloat
            System.err.println("Detailed error stack:");
            e.printStackTrace();
        }

        outputManager.log("---");
        return result;
    }

    /**
     * Execute the complete test suite
     */
    public static TestSuiteSummary executeTestSuite(StreamTableEnvironment tableEnv,
            TestCaseConfig config,
            OutputManager outputManager) {
        TestSuiteSummary summary = new TestSuiteSummary(System.currentTimeMillis());
        List<TestResult> results = new ArrayList<>();

        outputManager.log("🎯 Starting test suite execution");
        outputManager.log("📈 Number of test cases: " + config.testCases.size());
        outputManager.log("⏰ Suite start time: " + new Date(summary.totalStartTime));

        // Group by classification for statistics
        Map<String, List<TestCaseConfig.TestCase>> casesByClassification = new HashMap<>();
        for (TestCaseConfig.TestCase testCase : config.testCases) {
            casesByClassification
                    .computeIfAbsent(testCase.classification, k -> new ArrayList<>())
                    .add(testCase);
        }

        outputManager.log("📂 Number of classifications: " + casesByClassification.size());
        for (String classification : casesByClassification.keySet()) {
            outputManager.log("   - " + classification + ": " +
                    casesByClassification.get(classification).size() + " test cases");
        }

        // Execute all test cases in sequence
        for (int i = 0; i < config.testCases.size(); i++) {
            TestCaseConfig.TestCase testCase = config.testCases.get(i);
            outputManager.log("📋 Execution progress: (" + (i + 1) + "/" + config.testCases.size() + ")");

            TestResult result = executeTestCase(tableEnv, testCase, outputManager);
            results.add(result);

            // Update classification statistics
            summary.classificationStats.merge(result.classification, 1, Integer::sum);
            summary.classificationDurations.merge(result.classification, result.duration, Long::sum);

            // Delay between test cases to avoid resource contention
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                outputManager.log("⚠️ Thread interrupted: " + e.getMessage());
            }
        }

        summary.totalEndTime = System.currentTimeMillis();
        summary.totalDuration = summary.totalEndTime - summary.totalStartTime;
        summary.totalCases = results.size();
        summary.passedCases = (int) results.stream().filter(r -> r.success).count();
        summary.failedCases = summary.totalCases - summary.passedCases;

        outputManager.log("==========================================");
        outputManager.log("🏁 Test suite execution completed");
        outputManager.log("⏱️  Total duration: " + summary.totalDuration + "ms");
        outputManager.log("==========================================\n\n");

        // Generate detailed report
        generateTestReport(results, summary, outputManager);

        return summary;
    }

    /**
     * Generate detailed test report
     */
    public static void generateTestReport(List<TestResult> results, TestSuiteSummary summary,
            OutputManager outputManager) {
        outputManager.log("📊 Detailed test results summary report");
        outputManager.log("==========================================");
        SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        // Group results by classification
        Map<String, List<TestResult>> resultsByClassification = new HashMap<>();
        for (TestResult result : results) {
            resultsByClassification
                    .computeIfAbsent(result.classification, k -> new ArrayList<>())
                    .add(result);
        }

        // Overall statistics
        outputManager.log("📈 Overall statistics:");
        outputManager.log(" • Total test cases: " + summary.totalCases);
        outputManager.log(" • Passed cases: " + summary.passedCases);
        outputManager.log(" • Failed cases: " + summary.failedCases);
        outputManager.log(" • Success rate: " +
                (summary.totalCases > 0 ? String.format("%.1f", (summary.passedCases * 100.0 / summary.totalCases))
                        : "0")
                + "%");
        outputManager.log(" • Total duration: " + summary.totalDuration + "ms (" +
                String.format("%.2f", summary.totalDuration / 1000.0) + " seconds)\n");

        // Detailed results table
        outputManager.log("📋 Detailed results list:");
        outputManager.log(
                "| Scenario ID | Classification | Records | Start Time              | End Time                | Duration(ms) | Status    |");
        outputManager.log(
                "|-------------|----------------|---------|-------------------------|-------------------------|--------------|-----------|");

        for (TestResult result : results) {
            String status = result.success ? "✅ Passed" : "❌ Failed";

            outputManager.log(String.format("| %-11s | %-14s | %7d | %-19s | %-18s | %12d | %s |",
                    result.scenarioId,
                    result.classification,
                    result.recordsProcessed,
                    timeFormat.format(result.startTime),
                    timeFormat.format(result.endTime),
                    result.duration,
                    status));
        }

        // Performance analysis
        outputManager.log("📈 Performance analysis:");
        if (!results.isEmpty()) {
            TestResult fastest = results.stream()
                    .min(Comparator.comparingLong(r -> r.duration))
                    .orElse(results.get(0));
            TestResult slowest = results.stream()
                    .max(Comparator.comparingLong(r -> r.duration))
                    .orElse(results.get(0));

            outputManager.log(" • Most time-consuming case: " + slowest.scenarioId + " (" + slowest.classification +
                    ") - " + slowest.duration + "ms");
            outputManager.log(" • Fastest case: " + fastest.scenarioId + " (" + fastest.classification +
                    ") - " + fastest.duration + "ms");
            outputManager.log(" • Average case duration: " +
                    String.format("%.2f", results.stream().mapToLong(r -> r.duration).average().orElse(0)) + "ms");
        }

        // Failed cases details
        List<TestResult> failedResults = results.stream()
                .filter(r -> !r.success)
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

        if (!failedResults.isEmpty()) {
            outputManager.log("⚠️ Failed cases details:");
            for (TestResult failed : failedResults) {
                outputManager.log("• " + failed.scenarioId + " (" + failed.classification + "): " +
                        failed.errorMessage);
            }
        }

        outputManager.log("==========================================");
        outputManager.log("📁 Full report saved to: " + outputManager.getOutputFilePath() + "\n");
    }

    /**
     * Main program entry point
     */
    public static void main(String[] args) {
        CommandLineOptions options = new CommandLineOptions();
        JCommander commander = JCommander.newBuilder()
                .addObject(options)
                .build();

        OutputManager outputManager = null;

        try {
            commander.parse(args);

            if (options.help) {
                commander.usage();
                return;
            }

            if (options.version) {
                System.out.println("TSBS Flink Test Framework v1.0");
                return;
            }

            // Get effective output file path (resolve to current working directory)
            String effectiveOutputPath = getEffectiveOutputFilePath(options.outputFilePath);
            outputManager = new OutputManager(effectiveOutputPath);

            outputManager.log("🔧 Program configuration information:");
            outputManager.log("📁 Current working directory: " + System.getProperty("user.dir"));
            outputManager.log("💾 Output file path: " + effectiveOutputPath);

            // Determine data file path
            String effectiveDataFilePath;
            if (options.dataFilePath != null && new File(options.dataFilePath).exists()) {
                effectiveDataFilePath = options.dataFilePath;
                outputManager.log("✅ Using external data file: " + effectiveDataFilePath);
            } else {
                // Use embedded default data file
                Path tempDataFile = extractResourceToTempFile("data/default_data.csv", "default_data.csv");
                effectiveDataFilePath = tempDataFile.toAbsolutePath().toString();
                outputManager.log("✅ Using embedded default data file (extracted to temp): " + effectiveDataFilePath);
            }

            // Determine config file path
            String effectiveConfigFilePath;
            if (options.configFilePath != null && new File(options.configFilePath).exists()) {
                effectiveConfigFilePath = options.configFilePath;
                outputManager.log("✅ Using external config file: " + effectiveConfigFilePath);
            } else {
                // Use embedded default config file
                effectiveConfigFilePath = "config/default_cases.yaml";
                outputManager.log("✅ Using embedded default config file: " + effectiveConfigFilePath);
            }

            // Initialize Flink environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
            env.setParallelism(1);

            outputManager.log("🔧 Initializing Flink test environment");
            outputManager.log("💻 Parallelism: " + env.getParallelism());

            // Create test table
            String createTableDDL = "CREATE TABLE tsbs_readings (\n" +
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
                    "    'path' = 'file://" + effectiveDataFilePath + "'\n" +
                    ")";

            tableEnv.executeSql(createTableDDL);
            outputManager.log("✅ Test table created successfully");

            // Load test configuration
            TestCaseConfig config = loadTestConfig(effectiveConfigFilePath, outputManager);
            outputManager.log("✅ Test configuration loaded successfully");
            outputManager.log("📊 Total test cases loaded: " + config.testCases.size());

            // Execute test suite
            TestSuiteSummary summary = executeTestSuite(tableEnv, config, outputManager);

            int exitCode = summary.failedCases > 0 ? 1 : 0;
            outputManager.log("Exit code: " + exitCode);
            outputManager.log("==========================================");

            System.exit(exitCode);

        } catch (ParameterException e) {
            System.err.println("Parameter error: " + e.getMessage());
            commander.usage();
            System.exit(1);
        } catch (Exception e) {
            if (outputManager != null) {
                outputManager.log("💥 Program execution exception: " + e.getMessage());
            } else {
                System.err.println("💥 Program initialization exception: " + e.getMessage());
            }
            e.printStackTrace();
            System.exit(1);
        } finally {
            if (outputManager != null) {
                outputManager.close();
            }
        }
    }
}