package com.rimi.mapreduce.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.util.Random;

/**
 * 序列文件
 *
 * @author admin
 * @date 2018-09-12
 */
public class SequenceFileDemo {

    /**
     * 保存序列文件
     */
    public static void saveSequenceFile() throws Exception {

        // 创建配置信息
        Configuration conf = new Configuration();

        // 创建 Writer.options
        // 文件输出的位置
        SequenceFile.Writer.Option fileOption = SequenceFile.Writer.file(new Path("C:\\Users\\admin\\Desktop\\seq\\1.seq"));
        // 创建key类型
        SequenceFile.Writer.Option keyClass = SequenceFile.Writer.keyClass(LongWritable.class);
        // value类型
        SequenceFile.Writer.Option valueClass = SequenceFile.Writer.valueClass(Text.class);

        // 设置压缩信息
//        SequenceFile.Writer.Option compressCodec = SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD, new GzipCodec());

        // 创建书写器
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, fileOption, keyClass, valueClass);

        String[] strings = {"hadoop","hello","您好","tom"};

        for (int i = 0; i < 100; i++) {

            // 使用append方法，把数据写入
            writer.append(new LongWritable(i),new Text(strings[new Random().nextInt(4)]));

            // 设置同步点
//            writer.sync();
        }
        writer.close();
    }

    /**
     * 读取序列文件
     *
     * @throws Exception
     */
    public static void readSequenceFile() throws Exception {

        Configuration conf = new Configuration();

        // 读取文件的位置
        SequenceFile.Reader.Option fileOption = SequenceFile.Reader.file(new Path("C:\\Users\\admin\\Desktop\\seq\\1.seq"));

        // 创建阅读器
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, fileOption);

        // 创建 key / value
        IntWritable key = new IntWritable();
        Text value = new Text();
        // 读取数据
        while (reader.next(key, value)) {
            System.out.println(key + ":" + value);
        }
        reader.close();
    }

    public static void readSequenceFile2() throws Exception {

        Configuration conf = new Configuration();

        // 读取文件的位置
        SequenceFile.Reader.Option fileOption = SequenceFile.Reader.file(new Path("C:\\Users\\admin\\Desktop\\seq\\1.seq"));

        // 创建阅读器
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, fileOption);

        // 创建 key / value
        IntWritable key = new IntWritable();
        Text value = new Text();
        // 读取数据
        while (reader.next(key)) {
            // 获取当前的value
            reader.getCurrentValue(value);
            System.out.println(key + ":" + value);
        }
        reader.close();
    }

    public static void readSequenceFile3() throws Exception {

        Configuration conf = new Configuration();

        // 读取文件的位置
        SequenceFile.Reader.Option fileOption = SequenceFile.Reader.file(new Path("C:\\Users\\admin\\Desktop\\seq\\1.seq"));

        // 创建阅读器
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, fileOption);


        // 获取读取的位置信息
        long positions = reader.getPosition();
        System.out.println(positions);
        // 设置读取的位置
//        reader.seek(100);
        reader.sync(600);
        // 创建 key / value
        IntWritable key = new IntWritable();
        Text value = new Text();
        // 读取数据
        while (reader.next(key,value)) {
            // 获取读取的位置信息
            long position = reader.getPosition();
            System.out.println(position+ "==" + key + ":" + value);
        }
        reader.close();
    }
}
