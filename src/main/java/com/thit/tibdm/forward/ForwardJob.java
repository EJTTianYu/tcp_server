package com.thit.tibdm.forward;

import com.thit.tibdm.Config;
import com.thit.tibdm.MetricMonitor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 执行数据转发任务
 * <p>
 * 需要将每一个连接的车号都构造一个连接
 */
public class ForwardJob implements Runnable {
    /**
     * 日志类
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteConnect.class);
    /**
     * 统计报表名称
     */
    private static final String FORWARD_REQUEST = "forward.request";
    /**
     * 原始数据包
     */
    private byte[] rawBinary;
    /**
     * 车号
     */
    private int ch;

    /**
     * 构造线程池任务
     * @param rawBinary
     * @param ch
     */
    public ForwardJob(byte[] rawBinary, int ch) {
        this.rawBinary = rawBinary;
        this.ch = ch;
    }

    /**
     * 运行逻辑
     * 主要包含了数据转发的任务
     */
    @Override
    public void run() {
        if (Config.I.isRemoteForwardSwitch()) {
            if (Config.I.getRemoteForwardChList().contains(String.valueOf(ch))) {
                RemoteConnect connect = RemoteConnectPool.I.getConnect(String.valueOf(ch));
                if (connect.getStatus()) {
                    MetricMonitor.I.registryMeter(FORWARD_REQUEST).mark();
                    ByteBuf buf = Unpooled.buffer();
                    buf.writeBytes(rawBinary);
                    connect.getConnect().writeAndFlush(buf);
                } else {
                    LOGGER.error("不能进行数据转发，远程服务器{}连接不上,请检查", connect.getConnect().remoteAddress());
                }
            }
        }

    }
}
