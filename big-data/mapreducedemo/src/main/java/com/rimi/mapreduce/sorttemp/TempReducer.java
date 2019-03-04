package com.rimi.mapreduce.sorttemp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * TempReducer
 *
 * @author admin
 * @date 2018-09-13
 */
public class TempReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int max = 0;
        for (IntWritable value : values) {
            int temp = value.get();
            // 求最大值
            max = max > temp ? max : temp;
        }
        context.write(key,new IntWritable(max));
    }
}
