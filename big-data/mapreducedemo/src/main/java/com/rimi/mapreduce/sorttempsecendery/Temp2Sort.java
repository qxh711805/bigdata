package com.rimi.mapreduce.sorttempsecendery;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 排序
 *
 * @author admin
 * @date 2018-09-14
 */
public class Temp2Sort extends WritableComparator {

    public Temp2Sort() {
        super(CombinerKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CombinerKey c0 = (CombinerKey) a;
        CombinerKey c1 = (CombinerKey) b;
        return c0.compareTo(c1);
    }
}
