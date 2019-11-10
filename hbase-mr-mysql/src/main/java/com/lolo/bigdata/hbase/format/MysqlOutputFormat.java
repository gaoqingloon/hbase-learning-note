package com.lolo.bigdata.hbase.format;

import com.lolo.bigdata.hbase.bean.CacheData;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlOutputFormat extends OutputFormat<Text, CacheData> {

    class MysqlRecordWriter extends RecordWriter<Text, CacheData> {

        private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
        private static final String MYSQL_URL = "jdbc:mysql://hadoop104:3306/test?useSSL=false";
        private static final String MYSQL_USERNAME = "root";
        private static final String MYSQL_PASSWORD = "123456";

        private Connection connection;

        public MysqlRecordWriter() {
            try {
                Class.forName(MYSQL_DRIVER_CLASS);
                connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void write(Text key, CacheData cacheData) throws IOException, InterruptedException {

            String sql = "INSERT INTO hbase_test (name, sumcnt) VALUES (?, ?)";
            PreparedStatement preparedStatement = null;
            try {
                preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setObject(1, key.toString());
                preparedStatement.setObject(2, cacheData.getCount());

                int count = preparedStatement.executeUpdate();
                System.out.println("影响了" + count + "条数据");
                System.err.println("---------插入成功！--------------------------");

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (preparedStatement != null) {
                    try {
                        preparedStatement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    @Override
    public RecordWriter<Text, CacheData> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MysqlRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    private FileOutputCommitter committer = null;
    //@Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {

        if (committer == null) {
            Path path = getOutPutPath(context);
            committer = new FileOutputCommitter(path, context);
        }
        return committer;
    }

    public static Path getOutPutPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }
}
