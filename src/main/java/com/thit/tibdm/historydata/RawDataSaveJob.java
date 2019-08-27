package com.thit.tibdm.historydata;


import com.thit.tibdm.HttpUtil;
import org.kairosdb.client.builder.MetricBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;

/**
 * 原始数据存储
 * 修改：
 * 如果是分单包和多包发送的时候需要多增加一个tag 表明这个是设备1发的还是设备2发的数据
 *
 * @author wanghaoqiang
 * @version 1.0
 * @time 2017-09-25 10:51
 **/
public class RawDataSaveJob implements Runnable {
    /**
     * 日志类
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(RawDataSaveJob.class);

    /**
     * 车号
     */
    private String ch;
    /**
     * 原始数据包时间
     */
    private long time;
    /**
     * 设备ID
     */
    private int deviceId;
    /**
     * 有效数据区
     */
    private String useBinary;
    /**
     * 序列号
     */
    private long serialNumber;

    /**
     * 构造原始数据存储任务
     * @param ch
     * @param time
     * @param deviceId
     * @param useBinary
     * @param serialNumber
     */
    public RawDataSaveJob(String ch, long time, int deviceId, String useBinary, long serialNumber) {
        this.ch = ch;
        this.time = time;
        this.deviceId = deviceId;
        this.useBinary = useBinary;
        this.serialNumber = serialNumber;
    }

    /**
     * 原始数据存储逻辑
     * 将数据发送到kairosdb数据库
     */
    @Override
    public void run() {
        MetricBuilder builder = MetricBuilder.getInstance();
        builder.addMetric("use_rawdata")
                .addTag("machine_id", ch)
                .addTag("device_id", String.valueOf(deviceId))
                .addDataPoint(time, useBinary);
        builder.addMetric("serial_number")
                .addTag("machine_id",ch)
                .addTag("device_id",String.valueOf(deviceId))
                .addDataPoint(time,serialNumber);
        HttpUtil.sendKairosdb(builder);
    }

    @Override
    public String toString() {
        return "RawDataSaveJob{" +
                ", ch='" + ch + '\'' +
                ", time=" + time +
                '}';
    }
}
