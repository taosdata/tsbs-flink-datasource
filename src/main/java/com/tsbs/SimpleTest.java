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
                                "    WATERMARK FOR ts AS ts - INTERVAL '0' SECOND\n" +
                                ") WITH (\n" +
                                "    'connector' = 'tsbs',\n" +
                                "    'path' = 'file:///root/tsbs-flink-datasource/src/main/resources/data/default_data.csv'\n" +
                                ")";
                tableEnv.executeSql(createTableDDL);

                System.out.println("=== Test Query ===");
                // final String sql1 = "SELECT name, ts, fuel_consumption, velocity FROM readings WHERE velocity is not null LIMIT 10";
                // final String sql2 = "SELECT AVG(fuel_consumption) AS avg_fuel_consumption FROM readings";
                final String sql_a1 = "SELECT TUMBLE_END(ts, INTERVAL '1' HOUR) AS ts, AVG(fuel_consumption) AS avg_fuel_consumption FROM readings GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)";
                
                TableResult projectQuery = tableEnv.executeSql(sql_a1);
                projectQuery.print();
        }
}
