package com.rimi.mapreduce.sorttemp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

/**
 * TempJob
 *
 * @author admin
 * @date 2018-09-13
 */
public class TempJob {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJobName("TempJob");
        job.setJarByClass(TempJob.class);

        // 设置mapper和reducer
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);

        // 设置reducer个数
        job.setNumReduceTasks(3);

        // 设置分区类
//        job.setPartitionerClass(TempPartitioner.class);
        job.setPartitionerClass(TotalOrderPartitioner.class);




        // 设置输入格式和输出格式
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置输出key / value 类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置文件输入路径和文件输出路径
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\admin\\Desktop\\temp\\temp.seq"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\admin\\Desktop\\temp\\out"));


        // 设置采样器
        InputSampler.Sampler sampler = new InputSampler.RandomSampler(0.1,10000,10);

        // 创建分区文件
        InputSampler.writePartitionFile(job,sampler);

        // 提交作业
        job.waitForCompletion(true);

    }
}
