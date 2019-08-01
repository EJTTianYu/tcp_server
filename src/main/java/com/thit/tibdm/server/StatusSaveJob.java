package com.thit.tibdm.server;

import com.alibaba.fastjson.JSON;
import com.codahale.metrics.Timer;
import com.google.common.collect.Maps;
import com.thit.tibdm.*;
import com.thit.tibdm.fault.FaultCache;
import com.thit.tibdm.historydata.RawDataSaveJob;
import com.thit.tibdm.historydata.StatusBatchJob;
import com.thit.tibdm.parse.ContentDecode;
import com.thit.tibdm.parse.WarnVariable;
import com.thit.tibdm.realtime.RealTimeCache;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.kairosdb.client.builder.MetricBuilder;
import org.redisson.api.RMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 异步任务
 * 提交数据到kairosdb
 */
public class StatusSaveJob implements Runnable {
    /**
     * 日志记录
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(RawDataSaveJob.class);
    /**
     * 车号
     */
    private int ch;
    /**
     * 数据包时间
     */
    private long time;
    /**
     * 有效数据区
     */
    private byte[] useBytes;
    /**
     * 设备ID
     */
    private int deviceId;

    /**
     * 车号时间，有效数据包构造方法
     * @param ch
     * @param time
     * @param useBytes
     */
    public StatusSaveJob(int ch, long time, byte[] useBytes) {
        this.ch = ch;
        this.time = time;
        this.useBytes = useBytes;
    }

    /**
     * 车号，时间，有效数据包，设备ID构造方法
     * @param ch
     * @param time
     * @param useBytes
     * @param deviceId
     */
    public StatusSaveJob(int ch, long time, byte[] useBytes, int deviceId) {
        this.ch = ch;
        this.time = time;
        this.useBytes = useBytes;
        this.deviceId = deviceId;
    }

    /**
     * 更新redis
     * <p>
     * 先清理掉原来的数据然后在更新现在的数据
     * 在支持双包协议的时候，需要对比的是各自的时间而不是最新的COLLECT_TIME
     *
     * @param ortherMap
     */
    private void sendRedis(Map<String, String> ortherMap) {
        Timer timer = MetricMonitor.I.registryTimer("send.redis.timer");
        Timer.Context ctx = timer.time();
        String type = ortherMap.containsKey(Contents.HEAD_TIME) ? Contents.HEAD_TIME : Contents.TAIL_TIME;
        boolean update = RealTimeCache.I.isUpdate(ortherMap.get("MACHINE_ID"),
                Long.parseLong(ortherMap.get(Contents.COLLECT_TIME)), type);
        if (update) {
            RMap<String, String> hash = RedisUtil.I.getRedisson().getMap(Contents.STATUS_PREFIX + ortherMap.get("MACHINE_ID"));
            hash.putAll(ortherMap);
        }
        ctx.stop();
    }

    /**
     * 发送故障到kafka
     * @param gzMap
     * @param messageMap
     */
    private void sendFaultKafka(Map<String, String> gzMap, Map<String, String> messageMap) {
        Timer timer = MetricMonitor.I.registryTimer("send.fault.timer");
        Timer.Context ctx = timer.time();
        List<Map<String, String>> faultList = FaultCache.I.reportFault(Contents.FAULT, gzMap.get("MACHINE_ID"), gzMap, messageMap, null);
        if (faultList.size() != 0) {
            Map<String, Object> result = Maps.newHashMap();
            result.put("messageType", "TROUBLE");
            Map<String, Object> content = Maps.newHashMap();
            content.put("flag", "TROUBLE");
            content.put("troubleArr", faultList);
            result.put("content", content);
            String msg = JSON.toJSONString(result);
            try {
                KafkaProducer.I.send(Config.I.getKafkaTopic(), null, msg.getBytes());
                LOGGER.info("推送故障：{}", msg);
            } catch (Exception e) {
                LOGGER.error("推送故障失败：{}", msg);
                FileHandle.save2File(msg, Config.I.getFaultFilePath() + File.separator + FileHandle.getFilename(System.currentTimeMillis()) + FileHandle.getFix());

            }
            MetricBuilder builder = MetricBuilder.getInstance();
            faultList.forEach(fault -> builder.addMetric(fault.get(Contents.BOM_CODE))
                    .addTag("machine_id", fault.get(Contents.BOM_TRAINNUMBER))
                    .addDataPoint(Long.parseLong(fault.get(Contents.TIME)),
                            fault.get(Contents.BOM_HANDLE)));
            HttpUtil.sendKairosdb(builder);
        }
        ctx.stop();
    }


    /**
     * 发送告警到kafka
     * @param warnMap
     * @param messageMap
     * @param warnVariableMap
     */
    private void sendWarnKafka(Map<String, String> warnMap, Map<String, String> messageMap, Map<String, WarnVariable> warnVariableMap) {
        Timer timer = MetricMonitor.I.registryTimer("send.warn.timer");
        Timer.Context ctx = timer.time();
        List<Map<String, String>> warnList = FaultCache.I.reportFault(Contents.WARN, warnMap.get("MACHINE_ID"), warnMap, messageMap, warnVariableMap);
        if (warnList.size() != 0) {
            Map<String, Object> result = Maps.newHashMap();
            result.put("messageType", "TROUBLE");
            Map<String, Object> content = Maps.newHashMap();
            content.put("flag", "WARN");
            content.put("troubleArr", warnList);
            result.put("content", content);

            String msg = JSON.toJSONString(result);
            try {
                KafkaProducer.I.send(Config.I.getKafkaTopic(), null, msg.getBytes());
                LOGGER.info("推送异常 {}", msg);
            } catch (Exception e) {
                LOGGER.error("推送异常失败 {}", msg);
                FileHandle.save2File(msg, Config.I.getWarnFilePath() + File.separator + FileHandle.getFilename(System.currentTimeMillis()) + FileHandle.getFix());
            }

        }
        ctx.stop();
    }

    /**
     * 运行数据包逻辑，包括数据的解析，实时数据存储，故障发送，异常发送
     */
    @Override
    public void run() {
        ByteBuf buf = Unpooled.buffer();
        buf.writeShort(ch);
        buf.writeLong(time);
        buf.writeBytes(useBytes);
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        Map<String, Object> parse = ContentDecode.I.parse(bytes, deviceId);
        Map<String, String> messageMap = (Map<String, String>) parse.get("MESSAGE_MAP");
        Map<String, String> ortherMap = (Map<String, String>) parse.get("ORTHER_MAP");
        Map<String, String> gzMap = (Map<String, String>) parse.get("GZ_MAP");
        Map<String, String> warnMap = (Map<String, String>) parse.get("WARN_MAP");
        Map<String, WarnVariable> warnVariableMap = (Map<String, WarnVariable>) parse.get(Contents.warnVari_Map);
        if (ortherMap.size() > 0) {
            try {
                sendRedis(ortherMap);
                sendFaultKafka(gzMap, messageMap);
                sendWarnKafka(warnMap, messageMap, warnVariableMap);
                ThreadPoolManager.I.getKairosdbThread().submit(new StatusBatchJob(ortherMap, Config.I.getHistioryDataFrequency()));
            } catch (Exception e) {
                LOGGER.error("发生异常：{}", e);
            }
        }
    }
}
