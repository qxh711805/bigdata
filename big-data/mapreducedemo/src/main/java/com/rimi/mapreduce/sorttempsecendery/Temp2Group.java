package com.rimi.mapreduce.sorttempsecendery;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组
 *
 * @author admin
 * @date 2018-09-13
 */
public class Temp2Group extends WritableComparator {
    public Temp2Group() {
        super(CombinerKey.class,true);
        System.out.println("Temp2Group");
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CombinerKey c0 = (CombinerKey) a;
        CombinerKey c1 = (CombinerKey) b;
        return c0.getYear() - c1.getYear();
    }
}
