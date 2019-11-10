package com.lolo.bigdata.hbase.tool;

import com.lolo.bigdata.hbase.bean.CacheData;
import com.lolo.bigdata.hbase.format.MysqlOutputFormat;
import com.lolo.bigdata.hbase.mapper.ScanHBaseMapper;
import com.lolo.bigdata.hbase.reducer.HBase2MysqlReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

public class HBase2MysqlTool implements Tool {
    @Override
    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(HBase2MysqlTool.class);

        // mapper
        TableMapReduceUtil.initTableMapperJob(
                "student",
                new Scan(),
                ScanHBaseMapper.class,
                Text.class,
                CacheData.class,
                job
        );

        // reducer
        job.setReducerClass(HBase2MysqlReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CacheData.class);

        job.setOutputFormatClass(MysqlOutputFormat.class);

        return job.waitForCompletion(true) ?
                JobStatus.State.SUCCEEDED.getValue() :
                JobStatus.State.FAILED.getValue();
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
