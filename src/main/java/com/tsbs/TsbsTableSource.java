package com.tsbs;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;

/**
 * TSBS Table Source implementation
 */
public class TsbsTableSource implements ScanTableSource {

    private String path = "";
    private org.apache.flink.table.types.DataType producedDataType;
    private String dataType;
    private boolean directReading;

    public TsbsTableSource(String path, org.apache.flink.table.types.DataType producedDataType, String dataType,
            boolean directReading) {
        this.path = path;
        this.producedDataType = producedDataType;
        this.dataType = dataType;
        this.directReading = directReading;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final TsbsSourceFunction sourceFunction = new TsbsSourceFunction(path, dataType, directReading);
        return SourceFunctionProvider.of(sourceFunction, true);
    }

    @Override
    public DynamicTableSource copy() {
        return new TsbsTableSource(path, producedDataType, dataType, directReading);
    }

    @Override
    public String asSummaryString() {
        return "TsbsTableSource (type: " + dataType + ", mode: " + (directReading ? "direct-reading" : "shared-queue")
                + ")";
    }
}