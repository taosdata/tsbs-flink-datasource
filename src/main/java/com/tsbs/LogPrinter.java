package com.tsbs;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogPrinter {
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static String outputFilePath = null;
    private static PrintWriter fileWriter = null;
    private static boolean outputToFile = false;

    private LogPrinter() {
    }

    public static void openFile(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            log("Output file not specified - console output only");
            return;
        }

        outputFilePath = filePath;
        outputToFile = true;

        try {
            File outputFile = new File(outputFilePath);
            // Create parent directories if they don't exist
            File parentDir = outputFile.getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                parentDir.mkdirs();
            }

            if (outputFile.exists()) {
                try (FileWriter fw = new FileWriter(outputFile, false)) {
                    fw.write(""); // Clear file content
                }
            }
            fileWriter = new PrintWriter(new FileWriter(outputFilePath, true));
            log("Output file opened: " + outputFilePath);
        } catch (IOException e) {
            System.err.println("Failed to initialize output file: " + outputFilePath);
            e.printStackTrace();
            outputToFile = false;
        }
    }

    public static void closeFile() {
        if (outputToFile && fileWriter != null) {
            fileWriter.close();
            log("Output file closed: " + outputFilePath);
            outputToFile = false;
            fileWriter = null;
            outputFilePath = null;
        }
    }

    public static void log(String message) {
        String timestampedMessage = "[" + dateFormat.format(new Date()) + "] " + message;
        System.out.println(timestampedMessage);
        if (outputToFile && fileWriter != null) {
            fileWriter.println(timestampedMessage);
            fileWriter.flush();
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

    public static String getOutputFilePath() {
        return outputFilePath;
    }
}