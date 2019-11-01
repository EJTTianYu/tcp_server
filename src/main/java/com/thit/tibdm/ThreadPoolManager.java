package com.thit.tibdm;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.*;

/**
 * 线程池管理类
 * <p>
 * 建立几个定长线程池，然后通过方法去获取
 * <p>
 * 现成池自己构建，设置最大最小，以及阻塞队列大小等，暂时全部的线程池全部设置为一样
 *
 * @author wanghaoqiang
 */
public enum ThreadPoolManager {
    /**
     * 实例化
     */
    I;
    /**
     * 实时数据更新+故障报警 线程池
     */
    private ExecutorService mqThread;
    /**
     * 原始数据存储线程池
     */
    private ExecutorService rawSaveThread;
    /**
     * 数据转发线程池
     */
    private ExecutorService forwardThread;
    /**
     * kairosdb存储线程池
     */
    private ExecutorService kairosdbThread;

    /**
     * 状态存储线程池
     */
    private ExecutorService statusThread;

    /**
     * 数据发送模拟
     */
    private ExecutorService dataSendThread;

    /**
     * 延迟执行线程池
     */
    private ScheduledExecutorService connectMonitortimer;


    /**
     * 线程池构造方法
     */
    ThreadPoolManager() {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("tcp-pool-%d").build();
        statusThread = new ThreadPoolExecutor(4, 1024,
                8L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(4096), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
        mqThread = new ThreadPoolExecutor(4, 1024,
                8L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(4096), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
        rawSaveThread = new ThreadPoolExecutor(24, 1024,
                8L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(4096), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
        forwardThread = new ThreadPoolExecutor(4, 1024,
                8L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(4096), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());

        dataSendThread = new ThreadPoolExecutor(4, 1024,
                8L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(4096), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());

        kairosdbThread = new ThreadPoolExecutor(128, 1024,
                8L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(4096), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());

        connectMonitortimer = new ScheduledThreadPoolExecutor(1, namedThreadFactory);
        MetricMonitor.I.registryGauges("status.pending.job", () -> ((ThreadPoolExecutor) statusThread).getQueue().size());
        MetricMonitor.I.registryGauges("parse.pending.job", () -> ((ThreadPoolExecutor) mqThread).getQueue().size());
        MetricMonitor.I.registryGauges("raw.save.pending.job", () -> ((ThreadPoolExecutor) rawSaveThread).getQueue().size());
        MetricMonitor.I.registryGauges("forward.pending.job", () -> ((ThreadPoolExecutor) forwardThread).getQueue().size());
        MetricMonitor.I.registryGauges("kairosdb.pending.job", () -> ((ThreadPoolExecutor) kairosdbThread).getQueue().size());
        MetricMonitor.I.registryGauges("datasend.pending.job", () -> ((ThreadPoolExecutor) dataSendThread).getQueue().size());

    }

    public ExecutorService getMqThread() {
        return mqThread;
    }

    public ExecutorService getRawSaveThread() {
        return rawSaveThread;
    }

    public ExecutorService getForwardThread() {
        return forwardThread;
    }

    public ExecutorService getDataSendThread() {
        return dataSendThread;
    }

    public ExecutorService getKairosdbThread() {
        return kairosdbThread;
    }

    public ExecutorService getStatusThread() {
        return statusThread;
    }

    public ScheduledExecutorService getConnectMonitorTimer() {
        return connectMonitortimer;
    }

}
