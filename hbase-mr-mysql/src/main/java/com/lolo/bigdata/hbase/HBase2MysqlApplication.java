package com.lolo.bigdata.hbase;

import com.lolo.bigdata.hbase.tool.HBase2MysqlTool;
import org.apache.hadoop.util.ToolRunner;

public class HBase2MysqlApplication {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HBase2MysqlTool(), args);
    }
}
