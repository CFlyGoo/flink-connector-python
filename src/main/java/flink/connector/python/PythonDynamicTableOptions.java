package flink.connector.python;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class PythonDynamicTableOptions {

    public static ConfigOption<String> FUNC = ConfigOptions.key("func").stringType().noDefaultValue();
    public static ConfigOption<String> MODULE = ConfigOptions.key("module").stringType().noDefaultValue();
    public static ConfigOption<String> PYTHON_EXEC = ConfigOptions.key("python-exec").stringType().defaultValue("python");
    public static ConfigOption<String> PYTHON_PATHS = ConfigOptions.key("python-paths").stringType().defaultValue("");
}
