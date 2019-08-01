package com.thit.tibdm;

import com.google.common.base.Charsets;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class HexUtilTest {

    @Test
    public void getFrame() {
        byte[] bytes=new byte[]{0x10,0x13,0x19,0x10,0x20,0x22};
        String result="10 13 19 10 20 22 ";
        String frame = HexUtil.getFrame(bytes," ");
        Assert.assertEquals(result,frame);
    }

    @Test
    public void serTest(){
        boolean result = true;
        byte[] bytes=new byte[]{0x10,0x13,0x19,0x10,0x20,0x22,0x09,0x11,0x20,0x39};
        String s = new String(bytes, Charsets.UTF_8);
        byte[] bytes1 = s.getBytes();
        for (int i = 0; i < bytes.length; i++) {
            boolean b = bytes[i] == bytes1[i];
            if (!b){
                result=false;
            }
        }
        assertTrue(result);
    }
}