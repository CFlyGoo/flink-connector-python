package flink.connector.python;

import org.apache.flink.table.data.RowData;
import org.junit.Test;


public class PythonSourceFunctionTest {

    @Test
    public void testTestRun() {
        System.out.println(System.getProperty("user.dir"));
        PythonSourceFunction function = new PythonSourceFunction(
//                "fn_test.mock", "mock_pandas",
                "akshare", "stock_info_sh_name_code",
                "/Users/hpf/Dev/fin-data/flink-connector-python/venv/bin/python",
                "/Users/hpf/Dev/fin-data/flink-connector-python/venv/lib/python3.9/site-packages:/Users/hpf/Dev/fin-data/flink-connector-python/src/test"
        );

        try {
            MockSourceContext<RowData> ctx = new MockSourceContext<>();
            function.run(ctx);
            for (RowData element : ctx.getElements()) {
                System.out.println(element);
            }
        } finally {
            function.cancel();
        }
    }
}