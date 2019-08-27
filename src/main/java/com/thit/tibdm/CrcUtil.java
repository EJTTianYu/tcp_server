package com.thit.tibdm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * CRC数组处理工具类及数组合并
 * @author wanghaoqiang
 */


public class CrcUtil {
    /**
              * 日志
     */
    public static final Logger LOGGER = LoggerFactory.getLogger(CrcUtil.class);


    /**
     * CRC-16/CCITT-FALSE x16+x12+x5+1 算法
     * <p>
     * info
     * Name:CRC-16/CCITT-FAI
     * Width:16
     * Poly:0x1021
     * Init:0xFFFF
     * RefIn:False
     * RefOut:False
     * XorOut:0x0000
     *
     * @param bytes  字节数组
     * @return int 返回
     */
    public static int crc(byte[] bytes) {
        int crc = 0xffff;
        int polynomial = 0x1021;
        for (int index = 0; index < bytes.length; index++) {
            byte b = bytes[index];
            for (int i = 0; i < 8; i++) {
                boolean bit = ((b >> (7 - i) & 1) == 1);
                boolean c15 = ((crc >> 15 & 1) == 1);
                crc <<= 1;
                if (c15 ^ bit) {
                    crc ^= polynomial;
                }
            }
        }
        crc &= 0xffff;
        //输出String字样的16进制
        return crc;
    }

}