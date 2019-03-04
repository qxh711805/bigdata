package com.rimi.mapreduce.seqwordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MyMapper
 *
 * @author admin
 * @date 2018-09-10
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 把 Text 类型装换成 String
        String line = value.toString();
        // 通过String中的 split 方法，取出每行中的每个单词
        String[] words = line.split(" ");
        // 把每个单词进行映射，并输出
        for (String word : words) {
            // 输出 K,V  -> Text,IntWritable
            Text keyOut = new Text(word);
            IntWritable valueOut = new IntWritable(1);
            // 输出
            context.write(keyOut,valueOut);
        }
    }
}
