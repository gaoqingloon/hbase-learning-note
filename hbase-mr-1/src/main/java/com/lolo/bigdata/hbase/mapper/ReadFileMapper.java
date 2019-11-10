package com.lolo.bigdata.hbase.mapper;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReadFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {

        String[] values = line.toString().split(",");
        String rowKey = values[0];
        String value = values[1];

        byte[] row = Bytes.toBytes(rowKey);
        Put put = new Put(row);
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(value));

        context.write(new ImmutableBytesWritable(row), put);
    }
}
