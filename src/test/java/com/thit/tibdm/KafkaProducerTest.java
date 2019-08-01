package com.thit.tibdm;

import com.google.common.collect.Lists;
import kafka.producer.KeyedMessage;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class KafkaProducerTest {

    @Test
    public void getSizeConnKafka() {
        int sizeConnKafka = KafkaProducer.I.getSizeConnKafka();
        Assert.assertEquals(sizeConnKafka, Config.I.getKafkaProducerNum());
    }

    @Test
    public void send() {
        KafkaProducer.I.send("test","aaa",new byte[]{0x10,0x09,0x10});
    }

    @Test
    public void sendList() {
        byte[] bytes = {0x10, 0x09, 0x10};
        List<KeyedMessage> list= Lists.newArrayList();
        list.add(new KeyedMessage("test","aaa",bytes));
        list.add(new KeyedMessage("test","aaa",bytes));
        list.add(new KeyedMessage("test","aaa",bytes));
        KafkaProducer.I.sendList(list);
    }
}