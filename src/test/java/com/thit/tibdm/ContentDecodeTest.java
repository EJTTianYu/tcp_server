package com.thit.tibdm;


import com.alibaba.fastjson.JSON;
import com.thit.tibdm.parse.ContentDecode;
//import com.thit.tibdm.rollup.RAMJob;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.MapContext;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ContentDecodeTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContentDecodeTest.class);

    @Test
    public void testInit() throws InterruptedException {
//        ContentDecode i = ContentDecode.I;
    }

//    @Test
//    public void testJedis() throws InterruptedException {
//        Jedis jedis = new Jedis(Config.I.getRedisIp());
//        for (int i = 0; i < 1000; i++) {
//            Thread.sleep(10);
//            jedis.hset("test_redisson1", i + "", i + "");
//        }
//    }


    @Test
    public void testCopy() {
        byte[] bytes = new byte[]{0x0, 0x10, 0x12, 0x16};
        byte[] bytes1 = new byte[bytes.length * 2 - 2];
        System.arraycopy(bytes, 0, bytes1, 0, bytes.length);
        System.arraycopy(bytes, 2, bytes1, bytes.length, bytes.length - 2);
        LOGGER.info(Arrays.toString(bytes1));

    }



    @Test
    public void testJexlContext(){
        JexlContext map=new MapContext();
        Map map1=new HashMap<>();
    }

}