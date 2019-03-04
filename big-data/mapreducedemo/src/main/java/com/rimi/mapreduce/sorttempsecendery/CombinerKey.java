package com.rimi.mapreduce.sorttempsecendery;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 组合key
 *
 * @author admin
 * @date 2018-09-13
 */
public class CombinerKey implements WritableComparable<CombinerKey> {

    private int year;
    private int temp;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }

    @Override
    public int compareTo(CombinerKey o) {
        int y0 = o.year;
        int t0 = o.temp;
//        if (year > y0) {
//            return 1;
//        } else if (year < y0) {
//            return -1;
//        } else {
//            return -(temp - t0);
//        }

        return (year - y0) == 0 ? -(temp - t0) : year - y0;
    }

    // 序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(temp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.temp = in.readInt();
    }
}
