package com.rimi.mapreduce.sorttemp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.util.Random;

/**
 * 温度
 *
 * @author admin
 * @date 2018-09-13
 */
public class Temp {

    /**
      1998 20
     */
    public static void main(String[] args) throws IOException {

         String path = "C:\\Users\\admin\\Desktop\\temp\\temp.seq";

        // 创建序列文件书写器
        SequenceFile.Writer writer = SequenceFile.createWriter(
                new Configuration(),
                SequenceFile.Writer.keyClass(IntWritable.class),
                SequenceFile.Writer.valueClass(IntWritable.class),
                SequenceFile.Writer.file(new Path(path)));
//        String path = "C:\\Users\\admin\\Desktop\\temp\\temp.txt";

//        BufferedWriter writer = new BufferedWriter(new FileWriter(path));
        for (int i = 0; i < 100000; i++) {
            // 获取年份
            int year = 1979 + new Random().nextInt(100);
            // 温度   -40 60
            int temp = -40 + new Random().nextInt(20 + new Random().nextInt(80));
//            writer.write(year + " " + temp + "\n");
            writer.append(new IntWritable(year),new IntWritable(temp));
        }
        for (int i = 0; i < 100000; i++) {
            // 获取年份
            int year = 2000 + new Random().nextInt(10);
            // 温度   -40 60
            int temp = -40 + new Random().nextInt(20 + new Random().nextInt(80));
//            writer.write(year + " " + temp + "\n");
            writer.append(new IntWritable(year),new IntWritable(temp));
        }
        writer.close();

    }
}
