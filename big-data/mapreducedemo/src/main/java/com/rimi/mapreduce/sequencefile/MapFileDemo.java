package com.rimi.mapreduce.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;


/**
 * MapFile
 *
 * @author admin
 * @date 2018-09-12
 */
public class MapFileDemo {

    public static void saveMapFile() throws Exception {
        Configuration conf = new Configuration();

        Path dirName = new Path("C:\\Users\\admin\\Desktop\\map");

        SequenceFile.Writer.Option keyClass = MapFile.Writer.keyClass(IntWritable.class);
        SequenceFile.Writer.Option valueClass = MapFile.Writer.valueClass(Text.class);

        // 创建比较器
//        WritableComparator comparator = WritableComparator.get(IntWritable.class);
//
//        MapFile.Writer.comparator(comparator);

        MapFile.Writer writer = new MapFile.Writer(conf, dirName, keyClass, valueClass);
        for (int i = 0; i < 10000; i++) {
            writer.append(new IntWritable(i), new Text("hadoop" + i));
        }
//        for (int i = 99; i >= 0; i--) {
//            writer.append(new IntWritable(i), new Text("hadoop" + i));
//        }

        // 关闭
        writer.close();
    }

    public static void readMapFile() throws Exception {

        Configuration conf = new Configuration();
        Path dirName = new Path("C:\\Users\\admin\\Desktop\\map");

        MapFile.Reader reader = new MapFile.Reader(dirName,conf);

        IntWritable key = new IntWritable();
        Text value = new Text();

        reader.seek(new IntWritable(200));

        while (reader.next(key,value)) {
            System.out.println(key+":" + value);
        }
        reader.close();

    }
}
