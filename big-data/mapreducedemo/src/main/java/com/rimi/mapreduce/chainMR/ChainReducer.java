package com.rimi.mapreduce.chainMR;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author admin
 * @date 2018-09-14
 */
public class ChainReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            int i = value.get();
            sum += i;
        }
        context.write(key, new IntWritable(sum));
    }
}
