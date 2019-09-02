package com.thit.tibdm;


import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;

/**
 * 配置类
 *
 * @author administartor
 */
public enum Config {
    /**
     * 实例化
     */
    I;

    /**
     * 服务端端口
     */
    private int tcpPort;
    /**
     * kafka地址列表
     */
    private String kafkaIpList;
    /**
     * kafka的生产者数量
     */
    private int kafkaProducerNum;
    /**
     * kafka的topic
     */
    private String kafkaTopic;
    /**
     * 远程转发是否开启
     */
    private boolean remoteForwardSwitch;
    /**
     * 远程转发IP
     */
    private String remoteForwardIp;
    /**
     * 远程转发端口
     */
    private int remoteForwardPort;
    /**
     * 远程转发的车号列表
     */
    private String remoteForwardChList;
    /**
     * 数据包频率
     */
    private int everyMachineFrequency;
    /**
     * 实时数据更新频率
     */
    private int realTimeMachineFrequency;
    /**
     * 原始数据存储开关
     */
    private boolean rawDataSaveSwitch;
    /**
     * kairosdb的地址
     */
    private String kairosdbUrl;
    /**
     * kairosdb的存储地址列表
     */
    private List<String> kairosdbUrlList;
    /**
     * redis的地址
     */
    private String redisIp;
    /**
     * 故障频率
     */
    private double faultFrequency;
    /**
     * 历史数据有效期
     */
    private int historyDataTime;
    /**
     * 历史数据频率
     */
    private int histioryDataFrequency;
    /**
     * 故障出错的默认日志存储地址
     */
    private String faultFilePath;
    /**
     * 异常推送出错的默认日志存储地址
     */
    private String warnFilePath;

    private String dataSendFilePath;
    /**
     * 配置文件类
     */
    private ResourceBundle conf;
    /**
     * IKR的接收数据端口
     */
    private String ikrUrl;


    /**
     * 载入配置文件
     */
    Config() {
        conf = ResourceBundle.getBundle("tcpserver");
        initConfig();
    }

    /**
     * 载入配置文件
     */
    private void initConfig() {
        tcpPort = Integer.parseInt(getConfigByKey("tcp.port", "8099"));
        kafkaIpList = getConfigByKey("kafka.ip.list", "localhost:9092");
        kafkaProducerNum = Integer.parseInt(getConfigByKey("kafka.producer.num", "30"));
        kafkaTopic = getConfigByKey("kafka.topic", "push-topic");
        remoteForwardSwitch = Boolean.parseBoolean(getConfigByKey("remote.forward.switch", "false"));
        remoteForwardIp = getConfigByKey("remote.forward.ip", "0");
        remoteForwardPort = Integer.parseInt(getConfigByKey("remote.forward.port", "8099"));
        remoteForwardChList = getConfigByKey("remote.forward.ch.list", "");
        everyMachineFrequency = Integer.parseInt(getConfigByKey("every.machine.frequency", "500"));
        realTimeMachineFrequency = Integer.parseInt(getConfigByKey("realtime.machine.frequency", "1000"));
        rawDataSaveSwitch = Boolean.parseBoolean(getConfigByKey("rawdata.save.switch", "true"));
        redisIp = getConfigByKey("redis.ip", "redis://localhost:6379");
        faultFrequency = Double.parseDouble(getConfigByKey("fault.frequency", "1000"));
        historyDataTime = Integer.parseInt(getConfigByKey("history.data.time", "180")) * 3600 * 24;
        histioryDataFrequency = Integer.parseInt(getConfigByKey("history.data.frequency", "1000"));
        faultFilePath = getConfigByKey("fault.filepath", FileHandle.getPath() + "fault");
        warnFilePath = getConfigByKey("warn.filepath", FileHandle.getPath() + "warn");
        dataSendFilePath = getConfigByKey("datasend.filepath", FileHandle.getPath() + "datasend");
        ikrUrl = getConfigByKey("ikr.ip","http://localhost:6666");
    }

    /**
     * @param key
     * @param defaultValue
     * @return
     */
    private String getConfigByKey(String key, String defaultValue) {
        try {
            return conf.getString(key);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public String getDataSendFilePath() {
        return dataSendFilePath;
    }

    public int getTcpPort() {
        return tcpPort;
    }

    public String getKafkaIpList() {
        return kafkaIpList;
    }

    public int getKafkaProducerNum() {
        return kafkaProducerNum;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public boolean isRemoteForwardSwitch() {
        return remoteForwardSwitch;
    }

    public String getRemoteForwardIp() {
        return remoteForwardIp;
    }

    public int getRemoteForwardPort() {
        return remoteForwardPort;
    }

    public String getRemoteForwardChList() {
        return remoteForwardChList;
    }

    public int getEveryMachineFrequency() {
        return everyMachineFrequency;
    }

    public int getRealTimeMachineFrequency() {
        return realTimeMachineFrequency;
    }

    public boolean isRawDataSaveSwitch() {
        return rawDataSaveSwitch;
    }

    public String getKairosdbUrl() {
        return kairosdbUrl;
    }

    public List<String> getKairosdbUrlList() {
        return kairosdbUrlList;
    }

    public String getRedisIp() {
        return redisIp;
    }

    public double getFaultFrequency() {
        return faultFrequency;
    }

    public int getHistoryDataTime() {
        return historyDataTime;
    }

    public int getHistioryDataFrequency() {
        return histioryDataFrequency;
    }

    public String getFaultFilePath() {
        return faultFilePath;
    }

    public String getWarnFilePath() {
        return warnFilePath;
    }

    public String getIkrUrl() {return ikrUrl; }
}
