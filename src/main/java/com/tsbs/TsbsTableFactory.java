package com.tsbs;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

public class TsbsTableFactory implements DynamicTableSourceFactory {
    public static final String IDENTIFIER = "tsbs";

    public static final ConfigOption<String> PATH = ConfigOptions
            .key("path")
            .stringType()
            .noDefaultValue()
            .withDescription("Path to the directory containing tsbs data files.");

    public static final ConfigOption<Integer> RECORDS_PER_SECOND = ConfigOptions
            .key("records-per-second")
            .intType()
            .defaultValue(-1)
            .withDescription("Maximum number of records to read per second. Default is unlimited.");

    public static final ConfigOption<String> DATA_TYPE = ConfigOptions
            .key("data-type")
            .stringType()
            .defaultValue("readings")
            .withDescription("Type of data: 'readings' or 'diagnostics'");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PATH);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RECORDS_PER_SECOND);
        options.add(DATA_TYPE);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        final String path = helper.getOptions().get(PATH);
        final Integer recordsPerSecond = helper.getOptions().get(RECORDS_PER_SECOND);
        final String dataType = helper.getOptions().get(DATA_TYPE); // 获取数据类型
        final org.apache.flink.table.types.DataType producedDataType = context.getCatalogTable().getSchema()
                .toPhysicalRowDataType();

        return new TsbsTableSource(path, producedDataType, recordsPerSecond, dataType);
    }
}