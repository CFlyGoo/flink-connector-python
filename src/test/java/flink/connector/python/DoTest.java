package flink.connector.python;

import org.apache.flink.connector.testframe.environment.MiniClusterTestEnvironment;
import org.apache.flink.connector.testframe.environment.TestEnvironmentSettings;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

public class DoTest {
    public static final String ddl = "CREATE TABLE stock_info_sh_name_code ("
            + "`证券代码` STRING, "
            + "`证券简称` STRING, "
            + "`公司全称` STRING, "
            + "`上市日期` STRING "
            + ") WITH ("
            + " 'connector' = 'python',"
            + " 'module' = 'akshare',"
            + " 'func' = 'stock_info_sh_name_code',"
            + " 'python-exec' = '/Users/hpf/Dev/fin-data/flink-connector-python/venv/bin/python',"
            + " 'python-paths' = '/Users/hpf/Dev/fin-data/flink-connector-python/venv/lib/python3.9/site-packages'"
            + ")";
    public static final String dml = "select * from stock_info_sh_name_code where 证券代码 = '600006'";

    @Test
    public void doTest() throws Exception {
        MiniClusterTestEnvironment tEnv = new MiniClusterTestEnvironment();
        tEnv.startUp();
        StreamExecutionEnvironment env = tEnv.createExecutionEnvironment(TestEnvironmentSettings.builder().build());
        StreamTableEnvironment tbEnv = StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().build()
        );
        tbEnv.executeSql(ddl);
        Table table = tbEnv.sqlQuery(dml);
        tbEnv.toDataStream(table).print();
        env.execute();
    }
}
