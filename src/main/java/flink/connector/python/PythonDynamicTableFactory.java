package flink.connector.python;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

public class PythonDynamicTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "python";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig options = helper.getOptions();
        return new PythonDynamicTableSource(
                options.get(PythonDynamicTableOptions.MODULE),
                options.get(PythonDynamicTableOptions.FUNC),
                options.get(PythonDynamicTableOptions.PYTHON_EXEC),
                options.get(PythonDynamicTableOptions.PYTHON_PATHS)
        );
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        HashSet<ConfigOption<?>> result = new HashSet<>();
        result.add(PythonDynamicTableOptions.MODULE);
        result.add(PythonDynamicTableOptions.FUNC);
        return result;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        HashSet<ConfigOption<?>> result = new HashSet<>();
        result.add(PythonDynamicTableOptions.PYTHON_EXEC);
        result.add(PythonDynamicTableOptions.PYTHON_PATHS);
        return result;
    }
}
