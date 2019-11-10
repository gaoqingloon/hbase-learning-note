package com.lolo.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

public class TestHBaseAPI_4 {
    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("student");

        Table table = connection.getTable(tableName);

        Scan scan = new Scan();
        // 1. 按列族查询
        //scan.addFamily(Bytes.toBytes("info"));

        // 2. 添加过滤器filter查询
        BinaryComparator bc = new BinaryComparator(Bytes.toBytes("2001"));
        RowFilter rowFilterBc = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, bc);

        RegexStringComparator rsc = new RegexStringComparator("^\\d{3}$");
        RowFilter rowFilterRsc = new RowFilter(CompareFilter.CompareOp.EQUAL, rsc);

        // FilterList.Operator.MUST_PASS_ALL : and
        // FilterList.Operator.MUST_PASS_ONE : or
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);  // or
        list.addFilter(rowFilterBc);
        list.addFilter(rowFilterRsc);

        // 扫描时增加过滤器
        // 所谓的过滤，其实每条数据都会筛选过滤，性能比较低
        scan.setFilter(list);

        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println("value = " + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("rowKey = " + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("family = " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("column = " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println();
            }
        }

        table.close();
        connection.close();
    }
}
