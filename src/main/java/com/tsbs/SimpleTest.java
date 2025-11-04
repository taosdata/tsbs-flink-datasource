package com.tsbs;

import static org.apache.flink.table.api.Expressions.e;
import static org.apache.flink.table.api.Expressions.$;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;

import com.ibm.icu.impl.Row;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.streaming.api.datastream.DataStream;

public class SimpleTest {
        public static void main(String[] args) throws Exception {
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
                env.setParallelism(1);

                String createReadingsTableDDL = "CREATE TABLE readings (\n" +
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
                                "    WATERMARK FOR ts AS ts - INTERVAL '60' MINUTE\n" +
                                ") WITH (\n" +
                                "    'connector' = 'tsbs',\n" +
                                "    'data-type' = 'readings',\n" +
                                "    'path' = 'file:///root/tsbs-flink-datasource/src/main/resources/data/default_readings.csv'\n"
                                +
                                ")";
                tableEnv.executeSql(createReadingsTableDDL);

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
                                "    'connector' = 'tsbs',\n" + //
                                "    'data-type' = 'diagnostics',\n" +
                                "    'path' = 'file:///root/tsbs-flink-datasource/src/main/resources/data/default_diagnostics.csv'\n"
                                +
                                ");";
                tableEnv.executeSql(createDiagnosticsTableDDL);

                // String sql = "SELECT * FROM readings LIMIT 10";
                // String sql = "SELECT AVG(fuel_consumption) FROM readings";
                String sql = "SELECT * FROM diagnostics";

                sql = sql.replace("\n", " ").replace("\r", " ").replaceAll("\\s+", " ");
                LogPrinter.log(sql);

                TableResult projectQuery = tableEnv.executeSql(sql);
                projectQuery.print();
        }
}
