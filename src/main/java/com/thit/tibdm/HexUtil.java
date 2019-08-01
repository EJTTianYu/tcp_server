package com.thit.tibdm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 进制计算
 *
 * @author wanghaoqiang
 * @version 1.0
 * @time 2017-08-22 15:18
 **/
public class HexUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(HexUtil.class);

    /**
     * 数组转16进制表示字符串
     * @param all
     * @param slice
     * @return
     */
    public static String getFrame(byte[] all, String slice) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < all.length; i++) {
            String str = Integer.toHexString(all[i] & 0xFF);
            if (str.length() == 1) {
                str = '0' + str;
            }
            sb.append(str).append(slice);
        }
        return sb.toString();
    }

    /**
     * 根据bytes来获取值，可以传入索引，长度,有无符号
     *
     * @param bytes  字节数组
     * @param index  索引
     * @param length 长度
     * @param isSign 有无符号
     * @return 字符串
     */
    public static String converByteToV(byte[] bytes, int index, int length, boolean isSign) {
        String result = "";
        //有符号
        if (isSign) {
            //两个字节，三个字节，四个字节
            if (length == 2) {
                result = convertTwoBytesToInt(bytes[index + 1], bytes[index]) + "";
            } else if (length == 3) {
                result = convertThreeBytesToInt(bytes[index + 2], bytes[index + 1], bytes[index]) + "";
            } else if (length == 4) {
                result = convertFourBytesToInt(bytes[index + 3], bytes[index + 2], bytes[index + 1], bytes[index]) + "";
            } else if (length == 8) {
                result = bytes2Long(bytes, index) + "";
            }
        } else {
            //无符号
            //两个字节，三个字节，四个字节
            if (length == 2) {
                result = convertTwoBytesToIntUn(bytes[index + 1], bytes[index]) + "";
            } else if (length == 3) {
                result = convertThreeBytesToIntUn(bytes[index + 2], bytes[index + 1], bytes[index]) + "";
            } else if (length == 4) {
                result = convertFourBytesToIntUn(bytes[index + 3], bytes[index + 2], bytes[index + 1], bytes[index]) + "";
            }
        }
        return result;
    }

    /**
     * 有符号
     *
     * @param b1 字节
     * @param b2 字节
     * @return int
     */
    public static int convertTwoBytesToInt(byte b1, byte b2) {
        return (b2 << 8) | (b1 & 0xFF);
    }

    /**
     * @param b1 字节
     * @param b2 字节
     * @param b3 字节
     * @return 整数
     */
    public static int convertThreeBytesToInt(byte b1, byte b2, byte b3) {
        return (b3 << 16) | (b2 & 0xFF) << 8 | (b1 & 0xFF);
    }

    /**
     * @param b1 字节
     * @param b2 字节
     * @param b3 字节
     * @param b4 字节
     * @return 整数
     */
    public static int convertFourBytesToInt(byte b1, byte b2, byte b3, byte b4) {
        return (b4 << 24) | (b3 & 0xFF) << 16 | (b2 & 0xFF) << 8 | (b1 & 0xFF);
    }


    /**
     * 无符号
     *
     * @param b1 字节
     * @param b2 字节
     * @return 整数
     */
    public static int convertTwoBytesToIntUn(byte b1, byte b2) {
        return (b2 & 0xFF) << 8 | (b1 & 0xFF);
    }

    /**
     * @param b1 字节
     * @param b2 字节
     * @param b3 字节
     * @return 整数
     */
    public static int convertThreeBytesToIntUn(byte b1, byte b2, byte b3) {
        return (b3 & 0xFF) << 16 | (b2 & 0xFF) << 8 | (b1 & 0xFF);
    }

    /**
     * @param b1 字节
     * @param b2 字节
     * @param b3 字节
     * @param b4 字节
     * @return long
     */
    public static long convertFourBytesToIntUn(byte b1, byte b2, byte b3, byte b4) {
        return (long) (b4 & 0xFF) << 24 | (b3 & 0xFF) << 16 | (b2 & 0xFF) << 8 | (b1 & 0xFF);
    }


    /**
     * @param byteNum 字节数组
     * @param index   索引
     * @return long
     */
    public static long bytes2Long(byte[] byteNum, int index) {
        long num = 0;
        for (int ix = 0; ix < 8; ++ix) {
            num <<= 8;
            num |= (byteNum[ix + index] & 0xff);
        }
        return num;
    }

    /**
     * 获取无符号的一个字节
     *
     * @param b 字节
     * @return 无符号
     */
    public static int getUnByte(byte b) {
        return b & 0xff;
    }


    /**
     * 获取字节里面的多个比特位组成的数字
     *
     * @param b     字节
     * @param start 开始位置
     * @param end   结束位置
     * @return 比特
     */
    public static String getBitsByByte(byte b, int start, int end) {
        return Integer.valueOf(byteToBit(b).substring(start, end), 2).toString();
    }

    /**
     * 把byte转为字符串的bit
     *
     * @param b 字节
     * @return 比特
     */
    public static String byteToBit(byte b) {
        return ""
                + (byte) ((b >> 7) & 0x1) + (byte) ((b >> 6) & 0x1)
                + (byte) ((b >> 5) & 0x1) + (byte) ((b >> 4) & 0x1)
                + (byte) ((b >> 3) & 0x1) + (byte) ((b >> 2) & 0x1)
                + (byte) ((b >> 1) & 0x1) + (byte) ((b) & 0x1);

    }

    /**
     * 16进制字符串转字节数组
     *
     * @param hex 字符
     * @return 字节数组
     */
    public static byte[] hexString2Bytes(String hex) {

        if ((hex == null) || (hex.equals(""))) {
            return null;
        } else if (hex.length() % 2 != 0) {
            return null;
        } else {
            hex = hex.toUpperCase();
            int len = hex.length() / 2;
            byte[] b = new byte[len];
            char[] hc = hex.toCharArray();
            for (int i = 0; i < len; i++) {
                int p = 2 * i;
                b[i] = (byte) (charToByte(hc[p]) << 4 | charToByte(hc[p + 1]) & 0xff);
            }
            return b;
        }

    }

    /**
     * 字符转换为字节
     *
     * @param c 字符
     * @return 字节
     */
    private static byte charToByte(char c) {
        return (byte) "0123456789ABCDEF".indexOf(c);
    }

}
