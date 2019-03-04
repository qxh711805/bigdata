package com.rimi.mapreduce.chainMR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author admin
 * @date 2018-09-14
 */
public class ChainJob {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance();

        job.setJobName("ChainJob");
        job.setJarByClass(ChainJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\admin\\Desktop\\chain\\chain.txt"));

        // 设置map的链式结构
        ChainMapper.addMapper(job, ChainMapper1.class, LongWritable.class, Text.class, Text.class, IntWritable.class, conf);
        ChainMapper.addMapper(job, ChainMapper2.class, Text.class, IntWritable.class, Text.class, IntWritable.class, conf);

        // 配置 reduce 链式结构
        ChainReducer.setReducer(job, com.rimi.mapreduce.chainMR.ChainReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class, conf);
        ChainReducer.addMapper(job, ChainMapper3.class, Text.class, IntWritable.class, Text.class, IntWritable.class, conf);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\admin\\Desktop\\chain\\out"));


        //提交作业
        job.waitForCompletion(true);
    }
}
