package com.rimi.hdfs;

import org.junit.Test;

public class ClientUtilsTest {

    @Test
    public void openClient() throws Exception {
        ClientUtils.openClient();
    }

    @Test
    public void createDir() throws Exception {
        ClientUtils.createDir();
    }

    @Test
    public void createFile() throws Exception {
        ClientUtils.createFile2();
    }

    @Test
    public void delete() throws Exception{
        ClientUtils.delete();
    }

    @Test
    public void traverDir() throws Exception {
        ClientUtils.traverDir("/",0);
    }

    @Test
    public void upload() throws Exception {
        ClientUtils.upload();
    }

    @Test
    public void download() throws Exception {
        ClientUtils.download();
    }

    @Test
    public void append() throws Exception {
        ClientUtils.append();
    }
}