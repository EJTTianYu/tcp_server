package com.thit.tibdm.fault;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;

import java.util.Map;

public class FaultEntity {
    /**
     * 故障时间
     */
    private long time;
    /**
     * 故障名称状态的容器
     */
    private Map<String, FaultStatus> faultMap;

    /**
     * 构造器
     * @param time
     * @param faultMap
     */
    public FaultEntity(long time, Map<String, FaultStatus> faultMap) {
        this.time = time;
        this.faultMap = faultMap;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public Map<String, FaultStatus> getFaultMap() {
        return faultMap;
    }

    public void setFaultMap(Map<String, FaultStatus> faultMap) {
        this.faultMap = faultMap;
    }

    public FaultEntity() {
        this.faultMap = Maps.newConcurrentMap();
        this.time = 0;
    }

    @Override
    public String toString() {
        return "FaultEntity{" +
                "time=" + time +
                ", faultMap=" + JSON.toJSONString(faultMap) +
                '}';
    }


}
