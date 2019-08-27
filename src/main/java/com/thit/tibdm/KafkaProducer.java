package com.thit.tibdm;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.DefaultEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * kafka生产者类
 * <p>
 * 生成多个生产者增加性能
 *
 * @author wanghaoqiang
 */
public enum KafkaProducer {
    /**
     * 实例化
     */
    I;

    /**
     * 生产者列表
     */
    private List<Producer> pool;
    /**
     * 索引
     */
    private int index = 0;
    /**
     * 生产者数据量
     */
    private final int producerNum = Config.I.getKafkaProducerNum();

    /**
     * 生产者构造
     */
    KafkaProducer() {
        pool = new ArrayList<>();
        Properties properties = new Properties();
        properties.put("key.serializer.class", "kafka.serializer.StringEncoder");
        properties.put("serializer.class", DefaultEncoder.class.getName());
        properties.put("metadata.broker.list", Config.I.getKafkaIpList());
        for (int i = 0; i < producerNum; i++) {
            pool.add(new Producer(new ProducerConfig(properties)));
        }
    }

    /**
     * 获取生产者池数量大小
     * @return
     */
    public int getSizeConnKafka() {
        return pool.size();
    }

    /**
     * 数据推送方法
     * @param topic
     * @param key
     * @param bytes
     */
    public void send(String topic, String key, byte[] bytes) {
        pool.get(index++ % producerNum).send(new KeyedMessage(topic, key, bytes));
    }

    /**
     * 数据推送方法
     * @param list
     */
    public void sendList(List<KeyedMessage> list) {
        pool.get(index++ % producerNum).send(list);
    }
}
