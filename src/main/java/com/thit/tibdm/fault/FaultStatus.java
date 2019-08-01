package com.thit.tibdm.fault;

public class FaultStatus {
    /**
     * 故障最后变化时间
     */
    private long time;
    /**
     * 故障状态
     */
    private int status;

    public FaultStatus(long time, int status) {
        this.time = time;
        this.status = status;
    }

    public FaultStatus() {
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "FaultStatus{" +
                "time=" + time +
                ", status=" + status +
                '}';
    }
}