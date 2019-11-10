package com.lolo.bigdata.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase操作工具类
 */
public class HBaseUtil {

    // ThreadLocal
    private static ThreadLocal<Connection> connHolder = new ThreadLocal<>();
    private static Connection conn = null;

    private HBaseUtil() { }

    /**
     * 获取HBase连接对象
     * @throws Exception
     */
    public static void makeHBaseConnection() throws Exception {

        conn = connHolder.get();
        if (conn == null) {
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
    }

    /**
     * 插入数据
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @param value
     * @throws Exception
     */
    public static void insertData(String tableName, String rowKey, String family, String column, String value) throws Exception {

        conn = connHolder.get();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    /**
     * 生成分区键
     * @param regionCount
     * @return
     */
    public static byte[][] genRegionKeys(int regionCount) {

        byte[][] splitKeys = new byte[regionCount-1][];
        for (int i = 0; i < regionCount-1; i++) {
            splitKeys[i] = Bytes.toBytes(i + "|");
        }
        return splitKeys;
    }

    public static void main(String[] args) {
        byte[][] splitKeys = genRegionKeys(6);
        // 0|, 1|, 2|, 3|, 4|
        for (byte[] splitKey : splitKeys) {
            System.out.println(Bytes.toString(splitKey));
        }
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

    /**
     * 关闭连接
     * @throws Exception
     */
    public static void close() throws Exception {

        conn = connHolder.get();
        if (conn != null) {
            conn.close();
            connHolder.remove();
        }
    }
}
