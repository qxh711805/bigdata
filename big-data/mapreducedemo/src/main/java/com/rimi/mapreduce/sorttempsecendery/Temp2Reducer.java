package com.rimi.mapreduce.sorttempsecendery;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer
 *
 * @author admin
 * @date 2018-09-13
 */
public class Temp2Reducer extends Reducer<CombinerKey, NullWritable, IntWritable,IntWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("Temp2Reducer");
    }

    @Override
    protected void reduce(CombinerKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        int year = key.getYear();
        int temp = key.getTemp();
        context.write(new IntWritable(year),new IntWritable(temp));

        context.getCounter("temp2","reduce").increment(1);
    }
}
