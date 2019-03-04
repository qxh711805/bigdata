package com.rimi.mapreduce.seqwordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * MyReducer
 *
 * @author admin
 * @date 2018-09-10
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 定义变量sum，用于存放总数
        int sum = 0;
        // 把 key 相同的value 进行累加
        for (IntWritable value : values) {
            int i = value.get();
            sum += i;
        }
        // 输出统计后的结果
        context.write(key, new IntWritable(sum));
    }
}
