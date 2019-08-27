package com.thit.tibdm.historydata;

import com.codahale.metrics.Timer;
import com.thit.tibdm.Config;
import com.thit.tibdm.HttpUtil;
import com.thit.tibdm.MetricMonitor;
import org.kairosdb.client.builder.MetricBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 状态数据批处理类
 * 主要目的是为了将数据攒成一批数据然后发送
 */
public class StatusBatchJob implements Runnable {
    /**
     * 日志
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(StatusBatchJob.class);
    /**
     * 状态数据的map
     */
    private Map<String, String> otherMap;
    /**
     * 状态数据过期时间
     */
    private int ttl;

    /**
     * get set类
     * @param otherMap
     */
    public StatusBatchJob(Map<String, String> otherMap) {
        this.otherMap = otherMap;
    }

    /**
     * 构造方法
     * @param otherMap 状态变量
     * @param ttl   过期时间
     */
    public StatusBatchJob(Map<String, String> otherMap, int ttl) {
        this.otherMap = otherMap;
        this.ttl = ttl;
    }


    @Override
    public String toString() {
        return "StatusBatchJob{" +
                "otherMap=" + otherMap +
                '}';
    }

    /**
     * 执行逻辑将状态数据存储到kairosdb中去
     * 同时记录了发送到kairosdb中去的时间，
     * 这个时间监控是非常重要的防止系统撑不住发生意外的情况
     */
    @Override
    public void run() {
        Timer timer = MetricMonitor.I.registryTimer("status.send.kairosdb.time");
        Timer.Context ctx = timer.time();
        String machineId = otherMap.get("MACHINE_ID");
        long time = Long.parseLong(otherMap.get("COLLECT_TIME"));
        MetricBuilder builder = MetricBuilder.getInstance();
        otherMap.forEach((k, v) -> builder.addMetric(k)
                .addTag("machine_id", machineId)
                .addDataPoint(time, v));
        HttpUtil.sendKairosdb(builder);
        ctx.stop();
    }
}
