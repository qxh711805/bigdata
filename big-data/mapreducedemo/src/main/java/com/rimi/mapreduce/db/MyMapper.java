package com.rimi.mapreduce.db;

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
public class MyMapper extends Mapper<LongWritable, Word, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Word value, Context context) throws IOException, InterruptedException {
        String word = value.getWord();
        String[] s = word.split(" ");
        for (String s1 : s) {
            context.write(new Text(s1),new IntWritable(1));
        }
    }
}
