package com.thit.tibdm.realtime;


import com.thit.tibdm.Contents;

/**
 * 将COLLECT_TIME 直接去掉清晰一点
 */
public class RealTimeEntity {

    /**
     * 车号
     */
    private String machineId;
    /**
     * 头包时间
     */
    private long headTime;
    /**
     * 尾包时间
     */
    private long tailTime;

    /**
     * 构造实时数据类
     * @param machineId
     * @param deviceTime
     * @param type
     */
    public RealTimeEntity(String machineId, long deviceTime, String type) {
        this.machineId = machineId;
        if (Contents.HEAD_TIME.equals(type)) {
            this.headTime = deviceTime;
        } else {
            this.tailTime = deviceTime;
        }
    }


    public RealTimeEntity() {
    }

    public String getMachineId() {
        return machineId;
    }

    public void setMachineId(String machineId) {
        this.machineId = machineId;
    }


    public long getHeadTime() {
        return headTime;
    }

    public void setHeadTime(long headTime) {
        this.headTime = headTime;
    }

    public long getTailTime() {
        return tailTime;
    }

    public void setTailTime(long tailTime) {
        this.tailTime = tailTime;
    }
}
