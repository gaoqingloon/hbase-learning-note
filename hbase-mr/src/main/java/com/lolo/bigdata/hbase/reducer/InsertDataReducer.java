package com.lolo.bigdata.hbase.reducer;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class InsertDataReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {

    // 运行reduce增加数据
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {

        for (Put put : values) {
            context.write(NullWritable.get(), put);
        }
    }
}