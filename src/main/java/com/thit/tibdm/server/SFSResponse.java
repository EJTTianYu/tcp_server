package com.thit.tibdm.server;

import com.thit.tibdm.CrcUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Arrays;

/**
 * 响应类
 */
public class SFSResponse {
    /**
     * 协议包头
     */
    private final static byte[] HEAD = new byte[]{(byte) 0xaa, (byte) 0xab, (byte) 0xac};
    /**
     * 协议包尾
     */
    private final static byte[] END = new byte[]{(byte) 0xba, (byte) 0xbb, (byte) 0xbc};

    public static byte[] getHEAD() {
        return HEAD;
    }

    public static byte[] getEND() {
        return END;
    }

    /**
     * 获取心跳包的响应
     *
     * @return
     */
    public static ByteBuf getHbResponse(SFSPacket packet) {
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(HEAD);
        buf.writeBytes(packet.getBody());
        buf.writeShort(packet.getCrc());
        buf.writeBytes(END);
        return buf;
    }

    /**
     * 获取消息数据包的响应
     *
     * @return
     */
    public static ByteBuf getStatusResponse(SFSPacket packet) {

        //数据包体
        ByteBuf tmp = Unpooled.buffer();
        //读取22个字节用来返回数据包
        byte[] lengthBefore = Arrays.copyOf(packet.getBody(), 7);
        byte[] lengthAfter = Arrays.copyOfRange(packet.getBody(), 11, 24);

        tmp.writeBytes(lengthBefore);
        tmp.writeInt(24);
        tmp.writeBytes(lengthAfter);
        byte[] array = new byte[24];
        tmp.readBytes(array);
        //获取
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(HEAD);
        buf.writeBytes(array);
        buf.writeShort(CrcUtil.crc(array));
        buf.writeBytes(END);
        return buf;
    }
}
