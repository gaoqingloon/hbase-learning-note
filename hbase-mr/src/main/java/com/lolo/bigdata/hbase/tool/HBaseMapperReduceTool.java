package com.lolo.bigdata.hbase.tool;

import com.lolo.bigdata.hbase.mapper.ScanDataMapper;
import com.lolo.bigdata.hbase.reducer.InsertDataReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

public class HBaseMapperReduceTool implements Tool {
    @Override
    public int run(String[] strings) throws Exception {

        // 作业
        Job job = Job.getInstance();
        job.setJarByClass(HBaseMapperReduceTool.class);

        // com.lolo.bigdata.hbase.mapper
        TableMapReduceUtil.initTableMapperJob(
                "lolo:student",
                new Scan(),
                ScanDataMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job
        );

        // com.lolo.bigdata.hbase.reducer
        TableMapReduceUtil.initTableReducerJob(
                "lolo:user",
                InsertDataReducer.class,
                job
        );

        // 执行作业
        boolean flg = job.waitForCompletion(true);
        return flg ? JobStatus.State.SUCCEEDED.getValue() : JobStatus.State.FAILED.getValue();
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
