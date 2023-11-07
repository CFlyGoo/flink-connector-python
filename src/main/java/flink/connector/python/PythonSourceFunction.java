package flink.connector.python;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class PythonSourceFunction extends RichSourceFunction<RowData> {
    private static final long serialVersionUID = -2386776342169073643L;
    private static final String script;

    private static final String PARAM_MODULE = "module";
    private static final String PARAM_FUNC = "func";
    private static final String PARAM_CTX = "ctx";
    private static final String PARAM_PARAMS = "params";

    static {
        URL url = PythonSourceFunction.class.getClassLoader().getResource("bridge.py");
        if (url == null) {
            throw new RuntimeException("bridge.py not found");
        }
        try (InputStream is = url.openStream()) {
            script = new String(is.readAllBytes());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private final String module;
    private final String function;
    private final String pythonExec;
    private final String pythonPaths;
    private PythonInterpreter currentInterpreter;

    private Map<String, Object> params = new HashMap<>();

    public PythonSourceFunction(String module, String function, String pythonExec, String pythonPaths) {
        this.module = module;
        this.function = function;
        this.pythonExec = pythonExec;
        this.pythonPaths = pythonPaths;
    }

    @Override
    public void run(SourceContext<RowData> ctx) {
        PythonInterpreterConfig config = PythonInterpreterConfig.newBuilder()
                .setPythonExec(pythonExec)
                .addPythonPaths(pythonPaths)
                .setExcType(PythonInterpreterConfig.ExecType.MULTI_THREAD)
                .build();
        try (PythonInterpreter interpreter = new PythonInterpreter(config)) {
            currentInterpreter = interpreter;
            interpreter.exec(script);
            HashMap<String, Object> kwargs = new HashMap<>();
            kwargs.put(PARAM_MODULE, module);
            kwargs.put(PARAM_FUNC, function);
            kwargs.put(PARAM_CTX, ctx);
            kwargs.put(PARAM_PARAMS, params);
            interpreter.invoke("collect", kwargs);
        }
    }

    @Override
    public void cancel() {
        if (currentInterpreter != null) {
            currentInterpreter.close();
        }
    }

}
