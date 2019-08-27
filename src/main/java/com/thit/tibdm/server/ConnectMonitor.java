package com.thit.tibdm.server;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.thit.tibdm.RedisUtil;
import com.thit.tibdm.ThreadPoolManager;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 连接监控类，主要收集连接上的车辆，同时打印
 * {"1701-1","1530692496000"}
 *
 * @author wanghaoqiang
 * @date 2018/7/4
 * @time 16:17
 */
public enum ConnectMonitor {
    /**
     * 单例
     */
    I;

    /**
     * 定时任务类
     */
    private TimerTask task;
    /**
     * 日志类
     */
    private Logger logger;
    /**
     * 分布式map类
     */
    private RMap<String, String> connectMap;

    /**
     * 定时监控任务启动类
     */
    ConnectMonitor() {
        logger = LoggerFactory.getLogger(ConnectMonitor.class);
        connectMap = RedisUtil.I.getRedisson().getMap("connect_monitor");
        int timeDiff = 60000;
        task = new TimerTask() {
            @Override
            public void run() {
                long now = System.currentTimeMillis();
                List<String> output = Lists.newArrayList();
                connectMap.forEach((k, v) -> {
                    if (Long.parseLong(v) != -1) {
                        if (Math.abs(now - Long.parseLong(v)) > timeDiff) {
                            connectMap.put(k, "-1");
                        }
                        output.add(k);
                    }
                });
                output.sort(String::compareTo);
                logger.info("已连接的设备共{}个，分别为：{}", output.size(), JSON.toJSONString(output));
            }
        };
        ThreadPoolManager.I.getConnectMonitorTimer().scheduleAtFixedRate(task, 1, 60, TimeUnit.SECONDS);
    }

    /**
     * 跟新连接信息方法
     * @param machineId 车号
     * @param deviceId  设备ID
     * @param time  时间
     */
    public void updateConnectInfo(int machineId, int deviceId, long time) {
        String key = machineId + "_" + deviceId;
        if (connectMap.containsKey(key)) {
            Long last = Long.parseLong(connectMap.get(key));

            if (time > last) {
                connectMap.put(key, String.valueOf(time));
            }
        } else {
            connectMap.put(key, String.valueOf(time));
        }
    }


}
