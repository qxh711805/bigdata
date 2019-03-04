package com.rimi.mapreduce.chainMR;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author admin
 * @date 2018-09-14
 */
public class ChainMapper3 extends Mapper<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        if (value.get() >= 5){
            context.write(key, value);
        }
    }
}
