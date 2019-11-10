package com.lolo.bigdata.hbase.coprocessor;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;

/**
 * 协处理器 （HBase自己的功能，不需要连接）
 * 1) 继承类，继承 BaseRegionObserver
 * 2) 重写方法： postPut
 * 3) 实现逻辑:
 *
 *      增加student的数据
 *      同时增加lolo:student中数据
 *
 * 4) 将项目（依赖）打包后上传到hbase中，让hbase可以识别协处理器
 *
 * 5) 删除原始表，增加新表时，同时设定协处理器
 *    td.addCoprocessor("com.lolo.bigdata.hbase.coprocessor.InsertLoloStudentCoprocessor");
 */
public class InsertLoloStudentCoprocessor extends BaseRegionObserver {

    /**
     * 插入数据之后执行该方法
     * @param e
     * @param put
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

        // 获取表
        Table table = e.getEnvironment().getTable(TableName.valueOf("lolo:student"));

        // 增加数据(注意防止put之后再调用该方法形成死循环)
        table.put(put);

        // 关闭表
        table.close();
    }
}
