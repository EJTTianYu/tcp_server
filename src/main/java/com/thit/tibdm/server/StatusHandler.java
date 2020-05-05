package com.thit.tibdm.server;


import com.thit.tibdm.*;
import com.thit.tibdm.forward.ForwardJob;
import com.thit.tibdm.historydata.RawDataSaveJob;
import com.thit.tibdm.parse.ContentDecode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 状态数据包处理类
 * 首先发送数据包到kafka
 * @author wanghaoqiang
 */
public class StatusHandler {
    /**
     * 日志类
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(StatusHandler.class);
    /**
     * 数据包过滤监控类
     */
    private static final String FILTER_PACKET = "filter.packet";
    /**
     * 头包数量统计
     */
    private static final String HEAD_PACKET_COUNT = "head.packet.count";
    /**
     * 尾包数量统计
     */
    private static final String TAIL_PACKET_COUNT = "tail.packet.count";

    /**
     * 处理类
     * 如果来的车属于分包协议的话，起码在这里就不能过滤了
     * 如果车不是分包协议的话继续往下走，来判断是否需要进行数据的过滤
     *
     * @param packet
     */
    public void handler(SFSPacket packet) {
        SFSStatusMessage statusMessage = new SFSStatusMessage(packet);
        ConnectMonitor.I.updateConnectInfo(statusMessage.getCh(), statusMessage.getDeviceId(), statusMessage.getTime());
        if (statusMessage.getDeviceId() == 1) {
            MetricMonitor.I.registryMeter(HEAD_PACKET_COUNT).mark();
        } else {
            MetricMonitor.I.registryMeter(TAIL_PACKET_COUNT).mark();
        }
        ThreadPoolManager.I.getRawSaveThread().submit(
                new RawDataSaveJob(String.valueOf(statusMessage.getCh()),
                        statusMessage.getTime(),
                        statusMessage.getDeviceId(),
                        HexUtil.getFrame(statusMessage.getRawBinary(), ""),
                        statusMessage.getSerialNumber()));
        ThreadPoolManager.I.getForwardThread().submit(
                new ForwardJob(statusMessage.getRawBinary(), statusMessage.getCh()));
        boolean isSingle = ContentDecode.I.isSinglePacket(statusMessage.getCh());
        if ((!isSingle) || (!PacketFilterDistributed.I.isExis(FILTER_PACKET,
                String.valueOf(statusMessage.getCh()),
                statusMessage.getTime(),
                Config.I.getEveryMachineFrequency()))) {
            /**
             * 发送有效数据包到kafka
             */

            ThreadPoolManager.I.getMqThread().submit(
                    new StatusSaveJob(statusMessage.getCh(),
                            statusMessage.getTime(),
                            statusMessage.getUsedBinary(),
                            statusMessage.getDeviceId()));
        } else {
            /**
             * 记录一下过滤掉了多少数据
             */
            LOGGER.debug("过滤数据包：车号{},时间{}", statusMessage.getCh(), statusMessage.getTime());
            MetricMonitor.I.registryMeter(FILTER_PACKET).mark();
        }
    }
}
