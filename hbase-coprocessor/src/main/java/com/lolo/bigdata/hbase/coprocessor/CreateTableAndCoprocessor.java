package com.lolo.bigdata.hbase.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * 创建表的时候增加协处理器
 */
public class CreateTableAndCoprocessor {
    public static void main(String[] args) throws Exception {

        // 1. 获取HBase连接
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://hadoop102:9000/hbase");
        conf.set("hbase.cluster.distributed", "true");
        conf.set("hbase.master.port", "16000");
        conf.set("hbase.zookeeper.quorum", "hadoop102:2181,hadoop103:2181,hadoop104:2181");
        conf.set("hbase.zookeeper.property.dataDir", "/opt/module/zookeeper-3.4.14/zkData");
        Connection connection = ConnectionFactory.createConnection(conf);

        // 2. 获取数据库操作对象
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("student");

        // 3. 若表存在，删除表
        if (admin.tableExists(tableName)) {
            // 禁用表
            admin.disableTable(tableName);
            // 删除表
            admin.deleteTable(tableName);
            System.out.println("删除表...");
        }

        // 4. 创建表的时候添加协处理器
        HTableDescriptor td = new HTableDescriptor(tableName);
        HColumnDescriptor cd = new HColumnDescriptor("info");

        // 添加协处理器，指定协处理器类名
        td.addCoprocessor("com.lolo.bigdata.hbase.coprocessor.InsertLoloStudentCoprocessor");
        td.addFamily(cd);

        admin.createTable(td);
        System.out.println("create table with InsertLoloStudentCoprocessor success!");
    }
}
