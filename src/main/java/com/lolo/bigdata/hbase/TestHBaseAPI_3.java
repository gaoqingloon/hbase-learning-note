package com.lolo.bigdata.hbase;

import com.lolo.bigdata.util.HBaseUtil;

public class TestHBaseAPI_3 {
    public static void main(String[] args) throws Exception {

        // 创建连接对象
        HBaseUtil.makeHBaseConnection();

        // 插入数据
        HBaseUtil.insertData("lolo:student", "1002", "info", "name", "lisi");

        // 关闭连接
        HBaseUtil.close();
    }
}
