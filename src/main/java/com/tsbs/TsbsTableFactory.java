package com.tsbs;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

/**
 * Table factory for TSBS connector
 */
public class TsbsTableFactory implements DynamicTableSourceFactory {
    public static final String IDENTIFIER = "tsbs";

    public static final ConfigOption<String> PATH = ConfigOptions
            .key("path")
            .stringType()
            .noDefaultValue()
            .withDescription("Path to the TSBS data file.");

    public static final ConfigOption<String> DATA_TYPE = ConfigOptions
            .key("data-type")
            .stringType()
            .defaultValue("readings")
            .withDescription("Type of data: 'readings' or 'diagnostics'");

    public static final ConfigOption<Boolean> DIRECT_READING = ConfigOptions
            .key("direct-reading")
            .booleanType()
            .defaultValue(true)
            .withDescription("Whether to use direct reading mode where each consumer reads file directly");

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
        options.add(DATA_TYPE);
        options.add(DIRECT_READING);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        final String path = helper.getOptions().get(PATH);
        final String dataType = helper.getOptions().get(DATA_TYPE);
        final Boolean directReading = helper.getOptions().get(DIRECT_READING);
        final org.apache.flink.table.types.DataType producedDataType = context.getCatalogTable().getSchema()
                .toPhysicalRowDataType();

        return new TsbsTableSource(path, producedDataType, dataType, directReading);
    }
}