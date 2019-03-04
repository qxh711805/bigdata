package com.rimi.mapreduce.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;

/**
 * DBJob
 *
 * @author admin
 * @date 2018-09-14
 */
public class DBJob {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 设置MySQL数据的连接参数
        String driverName = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/hadoop?useUnicode=true&useSSL=false&characterEncoding=UTF-8";
        String username = "root";
        String password = "123456";
        DBConfiguration.configureDB(conf, driverName, url, username, password);

        Job job = Job.getInstance(conf);

        job.setJobName("DBJob");
        job.setJarByClass(DBJob.class);

        // 设置输入
        job.setInputFormatClass(DBInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        DBInputFormat.setInput(job, Word.class, "select word from word", "select count(1) from word");


        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 设置输出
        job.setOutputFormatClass(DBOutputFormat.class);

        DBOutputFormat.setOutput(job, "result", "word", "num");

        job.waitForCompletion(true);

    }
}
