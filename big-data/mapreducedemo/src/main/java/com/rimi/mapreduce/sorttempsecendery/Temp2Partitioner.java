package com.rimi.mapreduce.sorttempsecendery;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区
 *
 * @author admin
 * @date 2018-09-13
 */
public class Temp2Partitioner extends Partitioner<CombinerKey, NullWritable> {
    public Temp2Partitioner() {
        System.out.println("Temp2Partitioner");
    }

    @Override
    public int getPartition(CombinerKey combinerKey, NullWritable nullWritable, int numPartitions) {
        int year = combinerKey.getYear();
        return year % numPartitions;
    }
}
