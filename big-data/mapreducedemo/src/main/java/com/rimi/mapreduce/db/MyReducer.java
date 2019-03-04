package com.rimi.mapreduce.db;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * MyReducer
 *
 * @author admin
 * @date 2018-09-10
 */
public class MyReducer extends Reducer<Text, IntWritable, Result, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            int i = value.get();
            sum += i;
        }
        Result result = new Result();
        result.setWord(key.toString());
        result.setNum(sum);
        context.write(result, NullWritable.get());
    }
}
