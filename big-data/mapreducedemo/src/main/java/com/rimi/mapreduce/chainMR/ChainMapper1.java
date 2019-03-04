package com.rimi.mapreduce.chainMR;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper1
 *
 * @author admin
 * @date 2018-09-14
 */
public class ChainMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取每一行的词语
        String[] lines = value.toString().split(" ");
        for (String word : lines) {
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
