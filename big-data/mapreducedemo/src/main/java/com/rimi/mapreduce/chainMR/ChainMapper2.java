package com.rimi.mapreduce.chainMR;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 过滤脏词
 * @author admin
 * @date 2018-09-14
 */
public class ChainMapper2 extends Mapper<Text, IntWritable,Text,IntWritable> {

    private final static Map<String,Integer> DIRTY = new HashMap<>();

    static {
        DIRTY.put("falungong",1);
        DIRTY.put("seqing",1);
        DIRTY.put("qingjie",1);
        DIRTY.put("qiangxie",1);
    }

    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        String word = key.toString();
        // 判断该词语是否在脏字类中
        if (!DIRTY.containsKey(word)) {
            context.write(key, value);
        }
    }
}
