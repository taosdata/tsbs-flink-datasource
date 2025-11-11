package com.tsbs;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogPrinter {
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static String logFilePath = null;
    private static String jsonFilePath = null;
    private static PrintWriter logWriter = null;
    private static PrintWriter jsonWriter = null;
    private static boolean outputToFile = false;

    private LogPrinter() {
    }

    public static void openFiles(String logFile, String jsonFile) {
        if ((logFile == null || logFile.trim().isEmpty()) &&
                (jsonFile == null || jsonFile.trim().isEmpty())) {
            log("No output files specified - console output only");
            return;
        }

        outputToFile = true;
        logFilePath = logFile;
        jsonFilePath = jsonFile;

        try {
            if (logFilePath != null && !logFilePath.trim().isEmpty()) {
                File logFileObj = new File(logFilePath);
                File parentDir = logFileObj.getParentFile();
                if (parentDir != null && !parentDir.exists()) {
                    parentDir.mkdirs();
                }

                if (logFileObj.exists()) {
                    try (FileWriter fw = new FileWriter(logFileObj, false)) {
                        fw.write("");
                    }
                }
                logWriter = new PrintWriter(new FileWriter(logFilePath, true));
                log("Log file opened: " + logFilePath);
            }

            if (jsonFilePath != null && !jsonFilePath.trim().isEmpty()) {
                File jsonFileObj = new File(jsonFilePath);
                File parentDir = jsonFileObj.getParentFile();
                if (parentDir != null && !parentDir.exists()) {
                    parentDir.mkdirs();
                }

                if (jsonFileObj.exists()) {
                    try (FileWriter fw = new FileWriter(jsonFileObj, false)) {
                        fw.write("");
                    }
                }
                jsonWriter = new PrintWriter(new FileWriter(jsonFilePath, true));
                log("JSON file opened: " + jsonFilePath);
            }

        } catch (IOException e) {
            System.err.println("Failed to initialize output files: " +
                    (logFilePath != null ? logFilePath : "null") + ", " +
                    (jsonFilePath != null ? jsonFilePath : "null"));
            e.printStackTrace();
            outputToFile = false;
            if (logWriter != null) {
                logWriter.close();
                logWriter = null;
            }
            if (jsonWriter != null) {
                jsonWriter.close();
                jsonWriter = null;
            }
        }
    }

    public static void closeFiles() {
        if (outputToFile) {
            if (logWriter != null) {
                logWriter.close();
                log("Log file closed: " + logFilePath);
                logWriter = null;
            }

            if (jsonWriter != null) {
                jsonWriter.close();
                log("JSON file closed: " + jsonFilePath);
                jsonWriter = null;
            }

            outputToFile = false;
            logFilePath = null;
            jsonFilePath = null;
        }
    }

    public static void log(String message) {
        String timestampedMessage = "[" + dateFormat.format(new Date()) + "] " + message;
        System.out.println(timestampedMessage);
        if (outputToFile && logWriter != null) {
            logWriter.println(timestampedMessage);
            logWriter.flush();
        }
    }

    public static void logJson(String jsonContent) {
        if (outputToFile && jsonWriter != null) {
            jsonWriter.println(jsonContent);
            jsonWriter.flush();
        }
    }

    public static void error(String message) {
        log("❌ " + message);
    }

    public static void warn(String message) {
        log("⚠️ " + message);
    }

    public static void success(String message) {
        log("✅ " + message);
    }

    public static void info(String message) {
        log("ℹ️ " + message);
    }

    public static void debug(String message) {
        // log("🐛 " + message);
    }

    public static boolean isOutputToFile() {
        return outputToFile;
    }

    public static String getLogFilePath() {
        return logFilePath;
    }

    public static String getJsonFilePath() {
        return jsonFilePath;
    }
}