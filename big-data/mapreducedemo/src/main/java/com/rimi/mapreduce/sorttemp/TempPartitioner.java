package com.rimi.mapreduce.sorttemp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区
 *
 * @author admin
 * @date 2018-09-13
 */
public class TempPartitioner extends Partitioner<IntWritable,IntWritable> {
    @Override
    public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
        int year = key.get();
        if ((year - 1979) <= 33){
            return 0;
        } else if ((year - 1979) <= 66){
            return 1;
        } else {
            return 2;
        }
    }
}
