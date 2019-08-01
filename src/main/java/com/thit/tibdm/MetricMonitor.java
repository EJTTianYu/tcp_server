package com.thit.tibdm;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.google.common.collect.Maps;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 设置监控
 * @author wanghaoqiang
 */
public enum MetricMonitor {
    /**
     * 实例化
     */
    I;

    /**
     * 监控注册
     */
    private MetricRegistry registry;

    /**
     * 监控的指标
     */
    private Map<String, Meter> meters;
    /**
     * 监控的计时列表
     */
    private Map<String, Timer> timers;
    /**
     * 监控的度量列表
     */
    private Map<String, Gauge<Integer>> gauges;

    /**
     * 构造监控类
     */
    MetricMonitor() {
        meters = Maps.newConcurrentMap();
        gauges = Maps.newConcurrentMap();
        timers = Maps.newConcurrentMap();
        registry = new MetricRegistry();
        Slf4jReporter reporter = Slf4jReporter.forRegistry(registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.MINUTES);

    }


    /**
     * 注册meter监听
     *
     * @param name
     * @return
     */
    public Meter registryMeter(String name) {
        Meter meter;
        if (meters.containsKey(name)) {
            meter = meters.get(name);
        } else {
            meter = registry.meter(name);
        }
        return meter;
    }

    /**
     * 注册meter监听
     *
     * @param name
     * @return
     */
    public Timer registryTimer(String name) {
        Timer timer;
        if (timers.containsKey(name)) {
            timer = timers.get(name);
        } else {
            timer = registry.timer(name);
        }
        return timer;
    }

    /**
     * 监控当时的个数
     */
    public void registryGauges(String name, Gauge<Integer> gauge) {
        if (!gauges.containsKey(name)) {
            gauges.put(name, registry.register(name, gauge));
        }
    }

    public Set<String> getMeters() {
        return meters.keySet();
    }

    public Set<String> getGauges() {
        return gauges.keySet();
    }

    public Set<String> getTimers() {
        return timers.keySet();
    }

}
