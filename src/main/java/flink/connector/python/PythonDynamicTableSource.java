package flink.connector.python;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;

public class PythonDynamicTableSource implements ScanTableSource {

    private final String module;
    private final String function;
    private final String pythonExec;
    private final String pythonPaths;

    public PythonDynamicTableSource(String module, String function, String pythonExec, String pythonPaths) {
        this.module = module;
        this.function = function;
        this.pythonExec = pythonExec;
        this.pythonPaths = pythonPaths;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        return SourceFunctionProvider.of(new PythonSourceFunction(
                module,
                function,
                pythonExec,
                pythonPaths
        ), true);
    }

    @Override
    public DynamicTableSource copy() {
        return new PythonDynamicTableSource(module, function, pythonExec, pythonPaths);
    }

    @Override
    public String asSummaryString() {
        return "Python Table Source";
    }
}
