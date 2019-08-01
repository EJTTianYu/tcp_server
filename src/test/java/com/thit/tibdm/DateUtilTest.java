package com.thit.tibdm;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class DateUtilTest {

    @Test
    public void getTimeStame() {
        ByteBuf in= Unpooled.buffer();
        long time=1521508210000L;
        byte[] bytes=new byte[16];
        in.writeBytes(bytes);
        in.writeByte(18);
        in.writeByte(3);
        in.writeByte(20);
        in.writeByte(9);
        in.writeByte(10);
        in.writeByte(10);
        in.writeShort(0);
        long timeStame = DateUtil.getTimeStame(in);
        Assert.assertEquals(time,timeStame);
    }
}