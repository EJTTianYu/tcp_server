package com.thit.tibdm.parse;

/**
 * 解析规则
 */
public class Variable {
    /**
     * 变量名字
     */
    private String name;
    /**
     * 有无符号
     */
    private boolean isSign;
    /**
     * 字节偏移
     */
    private int byteOffset;
    /**
     * 位偏移
     */
    private int bitOffset;
    /**
     * 字节长度
     */
    private int byteLength;
    /**
     * 位长度
     */
    private int bitLength;
    /**
     * 表达式
     */
    private String conversion;
    /**
     * 类型
     */
    private String type;
    /**
     * 1表示头包，2表示尾包
     */
    private int deviceId;

    public Variable() {
    }


    public Variable(String name,
                    boolean isSign,
                    int byteOffset,
                    int bitOffset,
                    int byteLength,
                    int bitLength,
                    String conversion,
                    String type) {
        this.name = name;
        this.isSign = isSign;
        this.byteOffset = byteOffset;
        this.bitOffset = bitOffset;
        this.byteLength = byteLength;
        this.bitLength = bitLength;
        this.conversion = conversion;
        this.type = type;
    }

    public Variable(String name,
                    boolean isSign,
                    int byteOffset,
                    int bitOffset,
                    int byteLength,
                    int bitLength,
                    String conversion,
                    String type, int deviceId) {
        this.name = name;
        this.isSign = isSign;
        this.byteOffset = byteOffset;
        this.bitOffset = bitOffset;
        this.byteLength = byteLength;
        this.bitLength = bitLength;
        this.conversion = conversion;
        this.type = type;
        this.deviceId = deviceId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isSign() {
        return isSign;
    }

    public void setSign(boolean sign) {
        isSign = sign;
    }

    public int getByteOffset() {
        return byteOffset;
    }

    public void setByteOffset(int byteOffset) {
        this.byteOffset = byteOffset;
    }

    public int getBitOffset() {
        return bitOffset;
    }

    public void setBitOffset(int bitOffset) {
        this.bitOffset = bitOffset;
    }

    public int getByteLength() {
        return byteLength;
    }

    public void setByteLength(int byteLength) {
        this.byteLength = byteLength;
    }

    public int getBitLength() {
        return bitLength;
    }

    public void setBitLength(int bitLength) {
        this.bitLength = bitLength;
    }

    public String getConversion() {
        return conversion;
    }

    public void setConversion(String conversion) {
        this.conversion = conversion;
    }


    public int getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(int deviceId) {
        this.deviceId = deviceId;
    }

    @Override
    public String toString() {
        return "Variable{" +
                "name='" + name + '\'' +
                ", isSign=" + isSign +
                ", byteOffset=" + byteOffset +
                ", bitOffset=" + bitOffset +
                ", byteLength=" + byteLength +
                ", bitLength=" + bitLength +
                ", conversion='" + conversion + '\'' +
                '}';
    }
}
