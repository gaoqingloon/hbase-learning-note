package com.lolo.bigdata.hbase.tool;

import com.lolo.bigdata.hbase.mapper.ReadFileMapper;
import com.lolo.bigdata.hbase.reducer.InsertDataReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

public class File2TableTool implements Tool {
    @Override
    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(File2TableTool.class);

        // hbase(util) ==> hbase(util)
        // hdfs ==> hbase(util)

        // com.lolo.bigdata.hbase.format
        Path path = new Path("hdfs://hadoop102:9000/student.csv");
        FileInputFormat.addInputPath(job, path);

        // com.lolo.bigdata.hbase.mapper
        job.setMapperClass(ReadFileMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        // com.lolo.bigdata.hbase.reducer
        TableMapReduceUtil.initTableReducerJob(
                "lolo:student",
                InsertDataReducer.class,
                job
        );

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
