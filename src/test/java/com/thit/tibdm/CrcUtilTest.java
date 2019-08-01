package com.thit.tibdm;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class CrcUtilTest {

    @Test
    public void crc() {
        byte[] bytes = new byte[]{0x01, 0x23, 0x68, 0x12, 0x14};
        int result = 0x12a1;
        Assert.assertEquals(CrcUtil.crc(bytes), result);
    }
}