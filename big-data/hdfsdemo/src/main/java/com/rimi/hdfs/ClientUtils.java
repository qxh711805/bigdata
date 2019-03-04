package com.rimi.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * HDFS API
 *
 * @author admin
 * @date 2018-09-05
 */
public class ClientUtils {

    private static FileSystem fs;

    static {
        try {
            fs = FileSystem.newInstance(new URI("hdfs://hd01"), new Configuration(), "root");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public static void openClient() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hd01");

        FileSystem fs = FileSystem.newInstance(conf);

        FSDataInputStream stream = fs.open(new Path("/usr/hadoop/a1.txt"));

        IOUtils.copyBytes(stream, System.out, 1024, true);
    }

    /**
     * HDFS 中创建目录
     *
     * @throws Exception
     */
    public static void createDir() throws Exception {
//        FileSystem fs = FileSystem.newInstance(new URI("hdfs://hd01"), new Configuration());
//        FileSystem fs = FileSystem.newInstance(new Configuration());
        FileSystem fs = FileSystem.newInstance(new URI("hdfs://hd01"), new Configuration(), "root");
        Path path = new Path("/zk");
        // 判断目录是否存在
        boolean exists = fs.exists(path);
        if (!exists) {
            boolean mkdirs = fs.mkdirs(path);
            System.out.println(mkdirs ? "创建目录成功" : "创建目录失败");
        } else {
            System.out.println("目录已经存在");
        }
    }

    /**
     * 创建文件，并写入数据
     *
     * @throws Exception
     */
    public static void createFile() throws Exception {
        FSDataOutputStream stream = fs.create(new Path("/usr/hadoop/a1.txt"));
        String msg = "hello hadoop";
        stream.writeBytes(msg);
        stream.close();
    }


    public static void createFile1() throws Exception {
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/usr/hadoop/a2.txt"), (short) 2);
        fsDataOutputStream.writeBytes("hdfs dfs");
    }

    public static void createFile2() throws Exception {
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/a2.txt"), true, 2048, (short) 2, 512);
        for (int i = 0; i < 100; i++) {
            fsDataOutputStream.writeBytes("hdfs dfs");
        }
    }

    /**
     * 删除
     *
     * @throws Exception
     */
    public static void delete() throws Exception {
        boolean delete = fs.delete(new Path("/usr/hadoop/a.txt"), true);
        System.out.println(delete ? "删除成功" : "删除失败");

    }


    /**
     * 遍历
     *
     * @param path 需要遍历的路径
     */
    public static void traverDir(String path, int lever) throws Exception {
        Path pa = new Path(path);
        // 判断文件/目录是否存在
        boolean exists = fs.exists(pa);
        if (exists) {
//            int lever=0;
            // 文件或目录存在,获取当前目录下的所有文件或目录
            FileStatus[] listStatus = fs.listStatus(pa);
            for (FileStatus status : listStatus) {
                // 获取文件的或目录的地址
                Path path1 = status.getPath();
                // 打印地址
//                for (int i = 0; i < lever; i++) {
//                    System.out.print("|--");
//                }
                if ("/".equals(path)) {
                    System.out.println(path + path1.getName());
                    // 判断该路径是否为目录
                    if (status.isDirectory()) {
                        traverDir(path + path1.getName(), lever + 1);
                    }
                } else {
                    System.out.println(path + "/" + path1.getName());
                    // 判断该路径是否为目录
                    if (status.isDirectory()) {
                        traverDir(path + "/" + path1.getName(), lever + 1);
                    }
                }
            }
        }

    }

    /**
     * 上传文件
     *
     * @throws Exception
     */
    public static void upload() throws Exception {
        // 把本地文件上传到hdfs中
        fs.copyFromLocalFile(new Path("C:\\Users\\admin\\Desktop\\1.png"), new Path("/"));
    }

    /**
     * 把hdfs中的文件拷贝到本地系统中
     *
     * @throws Exception
     */
    public static void download() throws Exception {
        fs.copyToLocalFile(new Path("/1.png"), new Path("C:\\Users\\admin\\Desktop\\1.png.tmp"));
    }

    /**
     * 权限
     */
    public static void permission() throws Exception {
        FsPermission fsPermission = new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL);

        fs.setPermission(new Path("/usr/hadoop/a2.txt"),fsPermission);
    }


    public static void append() throws Exception {

        FSDataInputStream open = fs.open(new Path("/usr/hadoop/a2.txt"));

        FSDataOutputStream stream = fs.append(new Path("/a2.txt"));

        IOUtils.copyBytes(open,stream,2048,true);
    }

}
