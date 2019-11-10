package com.lolo.bigdata.hbase;

import com.lolo.bigdata.hbase.tool.HBaseMapperReduceTool;
import org.apache.hadoop.util.ToolRunner;

public class Table2TableApplication {
    public static void main(String[] args) throws Exception {

        // ToolRunner可以运行MR
        ToolRunner.run(new HBaseMapperReduceTool(), args);
    }
}
