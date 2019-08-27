package com.thit.tibdm.server;

import io.netty.buffer.ByteBuf;

/**
 * 数据包实体
 * <p>
 * 解析完数据包初步形态，方便后面回复心跳包，状态包，给kafka发送数据
 */
public class SFSPacket {
    private byte cmd;
    /**
     * 除了包头包尾以及校验字段的所有字节长度
     */
    private int length;
    /**
     * 原始数据
     */
    private byte[] rawData;
    /**
     * 数据包内容
     */
    private byte[] body;
    /**
     * crc校验字段
     */
    private int crc;

    /**
     * 根据长度了类型构造数据包
     * @param length
     * @param cmd
     */
    SFSPacket(int length, byte cmd) {
        this.cmd = cmd;
        this.length = length;
    }

    public byte[] getRawData() {
        return rawData;
    }

    public void setRawData(byte[] rawData) {
        this.rawData = rawData;
    }

    public byte getCmd() {
        return cmd;
    }

    public void setCmd(byte cmd) {
        this.cmd = cmd;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public int getCrc() {
        return crc;
    }

    public void setCrc(int crc) {
        this.crc = crc;
    }

    /**
     * 解析数据包
     *
     * @param packet
     * @param in
     * @return
     */
    public static SFSPacket decodeSFSPacket(SFSPacket packet, ByteBuf in) {
        //正常解析1.包头，2.包体，3.包尾
        in.readUnsignedMedium();
        byte[] bytes = new byte[packet.length];

        in.readBytes(bytes);
        packet.body = bytes;
        packet.crc = in.readUnsignedShort();
        in.readUnsignedMedium();
        //日志记录数据包
        return packet;
    }
}
