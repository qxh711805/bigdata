package com.rimi.mapreduce.sequencefile;

import org.junit.Test;

import static org.junit.Assert.*;

public class MapFileDemoTest {

    @Test
    public void saveMapFile() throws Exception {
        MapFileDemo.saveMapFile();
    }

    @Test
    public void readMapFile() throws Exception {
        MapFileDemo.readMapFile();
    }
}