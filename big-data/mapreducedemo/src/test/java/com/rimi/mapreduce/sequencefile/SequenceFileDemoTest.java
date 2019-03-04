package com.rimi.mapreduce.sequencefile;

import org.junit.Test;

import static org.junit.Assert.*;

public class SequenceFileDemoTest {

    @Test
    public void saveSequenceFile() throws Exception {
        SequenceFileDemo.saveSequenceFile();
    }

    @Test
    public void readSequenceFile() throws Exception {
        SequenceFileDemo.readSequenceFile3();
    }
}