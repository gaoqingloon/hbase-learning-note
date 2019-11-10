package com.lolo.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 测试 HBase API
 */
public class TestHBaseAPI_1 {
    public static void main(String[] args) throws Exception {

        // 通过java代码访问mysql数据库: JDBC
        // 1) 加载数据库驱动
        // 2) 获取数据库连接 （url,user,password）
        // 3) 获取数据库操作对象
        // 4) sql
        // 5) 执行数据库操作
        // 6) 获取查询结果ResultSet

        // 通过java代码访问hbase数据库

        // 0) 创建配置对象
        Configuration conf = HBaseConfiguration.create();

        // 1) 获取hbase连接对象
        // classLoader : Thread.currentThread().getContextClassLoader()
        // conf.setClassLoader(HBaseConfiguration.class.getClassLoader());
        // 谁把HBaseConfiguration类加载到内存，就去哪找配置文件
        // classpath: hbase-default.xml, hbase-site.xml
        // target/ : classes/ lib/
        Connection connection = ConnectionFactory.createConnection(conf);
        //System.out.println(connection);

        // 2) 获取操作对象 : Admin
        //Admin admin = new HBaseAdmin(connection);
        Admin admin = connection.getAdmin();

        // 3) 操作数据库:
        // 3.1 判断命名空间
        try {
            NamespaceDescriptor namespace = admin.getNamespaceDescriptor("lolo");
            // @throws org.apache.hadoop.hbase.NamespaceNotFoundException
        } catch (NamespaceNotFoundException e) {
            // 创建表空间
            NamespaceDescriptor nd = NamespaceDescriptor.create("lolo").build();
            admin.createNamespace(nd);
        }

        // 3.2 判断hbase中是否存在某张表
        TableName tableName = TableName.valueOf("lolo:student");
        boolean flg = admin.tableExists(tableName);
        System.out.println(flg);

        if (flg) {
            // 查询数据
            // DDL(create drop alter), DML(update,insert,delete), DQL(select)

            // 获取表对象
            Table table = connection.getTable(tableName);

            Get get = new Get(Bytes.toBytes("1001"));
            Result result = table.get(get);
            boolean empty = result.isEmpty();

            if (empty) {
                // 新增数据
                Put put = new Put(Bytes.toBytes("1001"));
                String family = "info";
                String column = "name";
                String value = "zhangsan";
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
                table.put(put);
                System.out.println("增加数据...");

            } else {
                for (Cell cell : result.rawCells()) {
                    System.out.println("rowKey = " + Bytes.toString(CellUtil.cloneRow(cell)));
                    System.out.println("family = " + Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.println("column = " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.println("value = " + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }


        } else {
            // 创建表

            // 创建表描述对象
            HTableDescriptor td = new HTableDescriptor(tableName);

            // 增加列族
            HColumnDescriptor cd = new HColumnDescriptor("info");
            td.addFamily(cd);

            admin.createTable(td);
            System.out.println("表格创建成功...");
        }

        // 4) 获取操作结果

        // 5) 关闭数据库连接
    }
}
