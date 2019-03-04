package com.rimi.mapreduce.sorttempsecendery;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Temp2Mapper
 *
 * @author admin
 * @date 2018-09-13
 */
public class Temp2Mapper extends Mapper<IntWritable,IntWritable,CombinerKey, NullWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("Temp2Mapper");
    }

    @Override
        protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            int year = key.get();
            int temp = value.get();
            CombinerKey combinerKey = new CombinerKey();
            combinerKey.setYear(year);
            combinerKey.setTemp(temp);
            context.write(combinerKey,NullWritable.get());
            // 定义计数器
            context.getCounter("Temp2","mapper").increment(1);
    }
}
