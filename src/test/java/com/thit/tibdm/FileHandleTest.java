package com.thit.tibdm;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author: dongzhiquan  Date: 2018/10/12 Time: 14:22
 */
public class FileHandleTest {

    @Test
    public void testGetFilename() throws Exception {
        String filename = FileHandle.getFilename(System.currentTimeMillis());
        boolean flag = false;
        if (filename.contains("_"))
            flag = true;
        assertTrue(flag);

    }

    @Test
    public void testGetPath() throws Exception {
        String path = FileHandle.getPath();
        boolean flag = false;
        if (path.contains("data"))
            flag = true;
        assertTrue(flag);
    }
}