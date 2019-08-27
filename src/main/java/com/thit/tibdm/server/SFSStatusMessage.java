package com.thit.tibdm.server;

import com.thit.tibdm.DateUtil;
import com.thit.tibdm.HexUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * 状态包消息
 */
public class SFSStatusMessage {
    /**
     * 车号
     */
    private int ch;
    /**
     * 时间
     */
    private long time;
    /**
     * 数据包排除校验和包头包尾的长度
     */
    private int length;

    /**
     * 设备号
     */
    private int deviceId;
    /**
     * 数据包转换为16进制字符串类型
     */
    private String rawData;
    /**
     * 二进制的原始数据包
     */
    private byte[] rawBinary;
    /**
     * 有效数据区的数据包
     */
    private byte[] usedBinary;

    /**
     * 序列号
     */
    private long serialNumber;

    /**
     * 构造消息的实体类
     * @param packet
     */
    public SFSStatusMessage(SFSPacket packet) {
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(packet.getBody());
        this.serialNumber = buf.getUnsignedInt(1);
        /**
         * 拿到车号和时间，以及有效数据包
         */
        this.ch = buf.getUnsignedShort(13);
        this.deviceId = buf.getUnsignedByte(15);
        this.length = (int) buf.getUnsignedInt(7);
        this.time = DateUtil.getTimeStame(buf);
        buf.readBytes(new byte[32]);
        this.usedBinary = new byte[(length - 32)];
        buf.readBytes(this.usedBinary);

        /**
         * 构建存储原始数据包
         */
        ByteBuf raw = Unpooled.buffer();
        raw.writeBytes(SFSResponse.getHEAD());
        raw.writeBytes(packet.getBody());
        raw.writeShort(packet.getCrc());
        raw.writeBytes(SFSResponse.getEND());
        this.rawBinary = new byte[raw.readableBytes()];
        raw.readBytes(this.rawBinary);
    }

    public int getCh() {
        return ch;
    }

    public void setCh(int ch) {
        this.ch = ch;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public String getRawData() {
        return rawData;
    }

    public void setRawData(String rawData) {
        this.rawData = rawData;
    }

    public byte[] getRawBinary() {
        return rawBinary;
    }

    public void setRawBinary(byte[] rawBinary) {
        this.rawBinary = rawBinary;
    }

    public byte[] getUsedBinary() {
        return usedBinary;
    }

    public void setUsedBinary(byte[] usedBinary) {
        this.usedBinary = usedBinary;
    }

    public int getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(int deviceId) {
        this.deviceId = deviceId;
    }

    public long getSerialNumber() {
        return serialNumber;
    }

    public void setSerialNumber(long serialNumber) {
        this.serialNumber = serialNumber;
    }
}
