package com.thit.tibdm.parse;

import com.alibaba.fastjson.JSON;
import com.thit.tibdm.Config;
import com.thit.tibdm.HexUtil;
import com.thit.tibdm.RedisUtil;
import com.thit.tibdm.rollup.RollupUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import org.redisson.api.RMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by admin on 2019/2/28.
 */
public class ContentDecodeTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ContentDecodeTest.class);

    @Test
    public void testCalFun() throws Exception {

        String fun = "sum";
        List list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);
//        String s = ContentDecode.I.calFun(fun, list);
//        System.out.println(s);
    }

    @Test
    public void testEquals() {
        List list = new ArrayList<>();
        list.add(1);
        list.add(1);
        list.add(1);

        boolean getequals = getequals(list);
        System.out.println(getequals);
    }


    public boolean getequals(List list) {
        boolean flag = false;
        Object obj = new Object();
        if (list != null && list.size() != 0) {
            obj = list.get(0);
        } else {
            return false;
        }
        for (Object o : list) {
            if (!obj.equals(o)) {
                return flag;
            }
        }
        flag = true;
        return flag;
    }


    public boolean getequals(List<String> paramList, String ch, WarnVariable warn) {
        boolean flag = false;
        String str = "";
        String parm = paramList.get(0);
        List<String> countList = RollupUtil.getCount(parm, ch, System.currentTimeMillis(), new Double(warn.getDuration()).longValue());
        flag = getequals(paramList);
        return flag;
    }

    @Test
    public void testparse() {

        String value = Config.I.getWarnFilePath();

        ByteBuf buf = Unpooled.buffer();
        buf.writeShort(1701);
        buf.writeLong(System.currentTimeMillis());
        buf.writeBytes(HexUtil.hexString2Bytes(value));
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);

        Map<String, Object> parse = ContentDecode.I.parse(bytes, 1);

        LOGGER.info(JSON.toJSONString(parse));

        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }


}