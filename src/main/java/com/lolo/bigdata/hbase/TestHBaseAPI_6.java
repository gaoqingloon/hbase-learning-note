package com.lolo.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 通过分区键，向表中插入数据
 */
public class TestHBaseAPI_6 {
    public static void main(String[] args) throws Exception {

        // 1. 获取连接
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        // 2. 获取表操作对象
        TableName tableName = TableName.valueOf("emp");
        Table table = connection.getTable(tableName);

        // 3. 向表中插入数据
        String rowKey = "zhangsan";
        rowKey = getRegionNum(rowKey, 3);

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("20"));
        table.put(put);
        System.out.println("insert data with region number success!");

        table.close();
        connection.close();
    }

    /**
     * 生成分区号
     * @param rowKey: 行键值
     * @param regionCount: 分区个数
     * @return : 分区号_行键值
     */
    public static String getRegionNum(String rowKey, int regionCount) {

        int regionNum;
        int hash = rowKey.hashCode();  // rowKey的散列值

        // 判断分区个数（格子数）是否为2^n
        if (regionCount > 0 && (regionCount & (regionCount - 1)) == 0) {
            // 2^n
            regionNum = hash & (regionCount - 1);
        } else {
            regionNum = hash % regionCount;
        }

        return regionNum + "_" + rowKey;
    }
}
