package com.lolo.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class TestHBaseAPI_5 {
    public static void main(String[] args) throws Exception {

        // 1. 获取连接
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        // 2. 获取数据库操作对象
        Admin admin = connection.getAdmin();

        // 3. 获取表操作对象
        TableName tableName = TableName.valueOf("emp");
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete table success!");
        }
        HTableDescriptor td = new HTableDescriptor(tableName);
        HColumnDescriptor cd = new HColumnDescriptor("info");
        td.addFamily(cd);

        // 4. 创建表的同时添加预分区键
        byte[][] splitKeys = new byte[2][];
        splitKeys[0] = Bytes.toBytes("0|");
        splitKeys[1] = Bytes.toBytes("1|");
        admin.createTable(td, splitKeys);

        System.out.println("create table success!");
        connection.close();
    }
}
