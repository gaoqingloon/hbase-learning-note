package com.lolo.bigdata.hbase.reducer;

import com.lolo.bigdata.hbase.bean.CacheData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HBase2MysqlReducer extends Reducer<Text, CacheData, Text, CacheData> {
    @Override
    protected void reduce(Text key, Iterable<CacheData> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        for (CacheData data : values) {
            sum += data.getCount();
        }

        CacheData sumData = new CacheData();
        sumData.setName(key.toString());
        sumData.setCount(sum);

        context.write(key, sumData);
    }
}
