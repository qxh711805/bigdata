package com.rimi.mapreduce.seqwordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区
 *
 * @author admin
 * @date 2018-09-12
 */
public class MyPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        int i = (text.toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
        return i;
    }
}
