package com.rimi.mapreduce.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.FileOutputStream;
import java.io.OutputStream;

/**
 * 压缩和解压
 *
 * @author admin
 * @date 2018-09-11
 */
public class CompressorDemo {

    public static void main(String[] args) throws Exception {

        // 创建压缩文件
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.newInstance(conf);

        // 创建编解码器工程
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);

        long l = System.currentTimeMillis();
        gzCodec(factory);
        System.out.println("gz:" + (System.currentTimeMillis() - l));
        long l2 = System.currentTimeMillis();
        bz2Codec(factory);
        System.out.println("bz2:" + (System.currentTimeMillis() - l2));

    }

    public static void gzCodec(CompressionCodecFactory factory) throws Exception {
        // 获得编解码器
        CompressionCodec codec = factory.getCodecByClassName("org.apache.hadoop.io.compress.GzipCodec");

        // 创建文件输出的路径
        OutputStream output = new FileOutputStream("C:\\Users\\admin\\Desktop\\out.gz2");

        CompressionOutputStream stream = codec.createOutputStream(output);
        for (int i = 0; i < 1000; i++) {
            stream.write(("map" + i).getBytes());
        }
        stream.close();
    }

    public static void bz2Codec(CompressionCodecFactory factory) throws Exception {
        // 获得编解码器
        CompressionCodec codec = factory.getCodecByClassName("org.apache.hadoop.io.compress.BZip2Codec");

        // 创建文件输出的路径
        OutputStream output = new FileOutputStream("C:\\Users\\admin\\Desktop\\out.bz");

        CompressionOutputStream stream = codec.createOutputStream(output);
        for (int i = 0; i < 1000; i++) {
            stream.write(("map" + i).getBytes());
        }
        stream.close();
    }
}
