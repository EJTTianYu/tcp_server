package com.thit.tibdm.fault;


/**
 * @author wanghaoqiang
 * @date 2018/7/4
 * @time 17:23
 */
public class FaultJobEntity {
    /**
     * 开始时间
     */
    private long startTime;
    /**
     * 结束时间
     */
    private long endTime;
    /**
     * 名字
     */
    private String name;
    /**
     * 协议名字
     */
    private String proName;

    public FaultJobEntity() {
    }

    public FaultJobEntity(long startTime, long endTime, String name, String proName) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.name = name;
        this.proName = proName;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getProName() {
        return proName;
    }

    public void setProName(String proName) {
        this.proName = proName;
    }

    @Override
    public String toString() {
        return "FaultJobEntity{" +
                "startTime=" + startTime +
                ", endTime=" + endTime +
                ", name='" + name + '\'' +
                ", proName='" + proName + '\'' +
                '}';
    }
}
