package com.rimi.mapreduce.sorttemp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * TempMapper
 *
 * @author admin
 * @date 2018-09-13
 */
public class TempMapper extends Mapper<IntWritable,IntWritable, IntWritable,IntWritable> {
    @Override
    protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }

    //    @Override
//    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        String lines = value.toString();
//        String[] s = lines.split(" ");
//        int year = Integer.valueOf(s[0]);
//        int temp = Integer.valueOf(s[1]);
//        context.write(new IntWritable(year),new IntWritable(temp));
//    }
}
