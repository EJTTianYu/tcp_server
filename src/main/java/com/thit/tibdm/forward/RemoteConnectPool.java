package com.thit.tibdm.forward;

import com.thit.tibdm.MetricMonitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 远程连接池
 */
public enum RemoteConnectPool {
    /**
     * 实例化
     */
    I;

    /**
     * 线程池监控名字
     */
    private static final String FORWARD_MACHINE_COUNT = "forward.machine.count";

    /**
     * 远程连接池
     */
    private Map<String, RemoteConnect> remoteConnectPool;

    /**
     * 远程连接池初始化
     */
    RemoteConnectPool() {
        remoteConnectPool = new ConcurrentHashMap<>();
        MetricMonitor.I.registryGauges(FORWARD_MACHINE_COUNT, () -> remoteConnectPool.size());
    }

    /**
     * 获取连接，
     * 1、如果存在则取出以前的连接
     * 2、不存在则新建连接
     *
     * @param ch
     * @return
     */
    public RemoteConnect getConnect(String ch) {
        if (remoteConnectPool.containsKey(ch)) {
            return remoteConnectPool.get(ch);
        } else {
            RemoteConnect connect = new RemoteConnect();
            remoteConnectPool.put(ch, connect);
            return connect;
        }
    }
}
