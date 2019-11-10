package com.lolo.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 测试 HBase API
 */
public class TestHBaseAPI_2 {
    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);


        // 删除表
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("lolo:student");
        if (admin.tableExists(tableName)) {
            // 禁用表
            //admin.disableTable(tableName);
            // 删除表
            //admin.deleteTable(tableName);
            System.out.println("删除表...");
        }


        // 删除数据
        Table table = connection.getTable(tableName);
        // delete
        String rowKey = "1001";
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //table.delete(delete);
        System.out.println("删除数据...");


        // 扫描数据
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println("rowKey = " + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("family = " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("column = " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("value = " + CellUtil.cloneValue(cell));
            }
        }
    }
}
