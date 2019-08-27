package com.thit.tibdm.parse;

import java.util.List;

/**
 * 预警变量
 * @author: dongzhiquan  Date: 2018/5/21 Time: 11:56
 */
public class WarnVariable {

    /**
     * 变量名字
     */
    private String SerialNumber;
    /**
     * 运算需要的其他变量
     */
    private List<String> Param;
    /**
     * 运算需要的jexl表达式
     */
    private String Jexl;
    /**
     * 持续时间
     */
    private double duration;
    /**
     * 设备ID
     */
    private int deviceId;
    /**
     * 是否需要告警
     */
    private boolean report;
    /**
     * 聚合运算函数
     */
    private List<String> Func;

    public List<String> getFunc() {
        return Func;
    }

    public void setFunc(List<String> func) {
        Func = func;
    }

    public boolean isReport() {
        return report;
    }

    public void setReport(boolean report) {
        this.report = report;
    }

    public double getDuration() {
        return duration;
    }

    public void setDuration(double duration) {
        this.duration = duration;
    }

    public WarnVariable() {
    }

    public String getSerialNumber() {
        return SerialNumber;
    }

    public void setSerialNumber(String serialNumber) {
        SerialNumber = serialNumber;
    }

    public List<String> getParam() {
        return Param;
    }

    public void setParam(List<String> param) {
        Param = param;
    }

    public String getJexl() {
        return Jexl;
    }

    public void setJexl(String jexl) {
        Jexl = jexl;
    }


    public int getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(int deviceId) {
        this.deviceId = deviceId;
    }

    @Override
    public String toString() {
        return "WarnVariable{" +
                "SerialNumber='" + SerialNumber + '\'' +
                ", Param='" + Param + '\'' +
                ", Jexl='" + Jexl + '\'' +
                '}';
    }
}
