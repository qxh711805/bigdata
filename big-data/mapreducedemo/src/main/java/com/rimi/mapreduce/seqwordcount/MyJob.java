package com.rimi.mapreduce.seqwordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * MyJob
 *
 * @author admin
 * @date 2018-09-10
 */
public class MyJob {

    public static void main(String[] args) throws Exception {


        // 创建配置类
        Configuration conf = new Configuration();
        // 设置hdfs位置
//        conf.set("fs.defaultFS","hdfs://hd01");

        Path path = new Path("C:\\Users\\admin\\Desktop\\wc\\out");
//        Path path = new Path(args[1]);

        FileSystem fs = FileSystem.newInstance(conf);
        if (fs.exists(path)) {
            fs.delete(path,true);
        }

        // 获得作业类
        Job job = Job.getInstance(conf);

        // 设置当前作业类和名称
        job.setJobName("MyJob");
        job.setJarByClass(MyJob.class);

        // 配置 mapper类和reducer类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 定义文件输入的格式和位置
//        job.setInputFormatClass(TextInputFormat.class);
//        FileInputFormat.setInputPaths(job,"C:\\Users\\admin\\Desktop\\wc");
//        FileInputFormat.setInputPaths(job,new Path(args[0]));

        // 多文件类型读取
        MultipleInputs.addInputPath(job,new Path("C:\\Users\\admin\\Desktop\\wc\\wc.txt"), TextInputFormat.class);

        MultipleInputs.addInputPath(job,new Path("C:\\Users\\admin\\Desktop\\wc\\1.seq"), SequenceFileInputFormat.class);



        // 设置输出的Key类型和Value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 定义文件输出的格式和位置
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, path);

        // 设置reduce个数
        job.setNumReduceTasks(3);

        // 自定义分区
        job.setPartitionerClass(MyPartitioner.class);

        // 自定义组合
        job.setCombinerClass(MyReducer.class);


        // 提交作业
        job.waitForCompletion(true);

    }
}
