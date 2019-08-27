package com.thit.tibdm.parse;

import java.util.List;

/**
 * 虚拟变量
 */
public class VirtualVariable {
    /**
     * 虚拟变量名字
     */
    private String name;
    /**
     * 虚拟变量计算表达式
     */
    private String conversion;
    /**
     * 设备ID
     */
    private int deviceId;
    /**
     * 虚拟变量计算需要的基础变量列表
     */
    private List<String> param;
    /**
     * 保留位数
     */
    private String CalcuAccuracy;

    public VirtualVariable(String name, String conversion, List<String> param, String calcuAccuracy) {
        this.name = name;
        this.conversion = conversion;
        this.param = param;
        this.CalcuAccuracy = calcuAccuracy;
    }

    public VirtualVariable(String name, String conversion, List<String> param) {
        this.name = name;
        this.conversion = conversion;
        this.param = param;
    }

    public VirtualVariable() {
    }

    public String getCalcuAccuracy() {
        return CalcuAccuracy;
    }

    public void setCalcuAccuracy(String calcuAccuracy) {
        CalcuAccuracy = calcuAccuracy;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getConversion() {
        return conversion;
    }

    public void setConversion(String conversion) {
        this.conversion = conversion;
    }

    public List<String> getParam() {
        return param;
    }

    public void setParam(List<String> param) {
        this.param = param;
    }

    public int getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(int deviceId) {
        this.deviceId = deviceId;
    }

    @Override
    public String toString() {
        return "VirtualVariable{" +
                "name='" + name + '\'' +
                ", conversion='" + conversion + '\'' +
                ", deviceId=" + deviceId +
                ", param=" + param +
                ", CalcuAccuracy='" + CalcuAccuracy + '\'' +
                '}';
    }
}
