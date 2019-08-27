package com.thit.tibdm;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author wanghaoqiang
 * @date 2016/10/24
 */
public class DateUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(DateUtil.class);

    /**
     * 如果不够的话补零
     *
     * @param num 字数
     * @return String 返回
     */
    private static String getAddZero(int num) {
        return num < 10 ? "0" + num : num + "";
    }


    /**
     * @param in 数据
     * @return long
     * @throws ParseException 异常
     */
    public static long getTimeStame(ByteBuf in) {
        DateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        StringBuffer time = new StringBuffer();
        time.append("20");
        time.append(DateUtil.getAddZero(in.getUnsignedByte(16)));
        time.append(DateUtil.getAddZero(in.getUnsignedByte(17)));
        time.append(DateUtil.getAddZero(in.getUnsignedByte(18)));
        time.append(DateUtil.getAddZero(in.getUnsignedByte(19)));
        time.append(DateUtil.getAddZero(in.getUnsignedByte(20)));
        time.append(DateUtil.getAddZero(in.getUnsignedByte(21)));
        time.append(DateUtil.getAddZero(in.getUnsignedShort(22)));
        Date parse = null;
        try {
            parse = sdf.parse(time.toString());
        } catch (ParseException e) {
            LOGGER.error("发生异常{}", e);
        }
        return parse != null ? parse.getTime() : System.currentTimeMillis();
    }

}
