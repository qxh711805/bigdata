package com.rimi.mapreduce.sorttempsecendery;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author admin
 * @date 2018-09-13
 */
public class Temp2Job {
    public static void main(String[] args) throws Exception {

        String path = "C:\\Users\\admin\\Desktop\\temp\\out2";
        FileSystem fs = FileSystem.newInstance(new Configuration());
        if (fs.exists(new Path(path))) {
            fs.delete(new Path(path), true);
        }

        Job job = Job.getInstance();
        job.setJobName("Temp2Job");
        job.setJarByClass(Temp2Job.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(path));
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\admin\\Desktop\\temp\\temp.seq"));

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置mapper输出类型
        job.setMapOutputKeyClass(CombinerKey.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 设置分区
        job.setPartitionerClass(Temp2Partitioner.class);

        // 设置reduce个数
        job.setNumReduceTasks(3);

        job.setMapperClass(Temp2Mapper.class);
        job.setReducerClass(Temp2Reducer.class);

        // 设置分组类
        job.setGroupingComparatorClass(Temp2Group.class);

        // 设置排序
        job.setSortComparatorClass(Temp2Sort.class);

        job.setCombinerClass(Temp2Reducer.class);

        job.waitForCompletion(true);
    }
}
