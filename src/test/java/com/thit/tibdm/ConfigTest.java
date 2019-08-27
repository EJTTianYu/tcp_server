package com.thit.tibdm;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConfigTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigTest.class);

    @Test
    public void testConfig() {
        Assert.assertNotNull(Config.I.getRemoteForwardIp());
        Assert.assertNotNull(Config.I.getKafkaIpList());
        Assert.assertNotNull(Config.I.getKafkaTopic());
        Assert.assertNotNull(Config.I.getRemoteForwardChList());
        Assert.assertNotNull(Config.I.getRemoteForwardPort());
        Assert.assertNotNull(Config.I.getEveryMachineFrequency());
        Assert.assertNotNull(Config.I.getKafkaProducerNum());
        Assert.assertNotNull(Config.I.getTcpPort());
        Assert.assertNotNull(Config.I.getHistoryDataTime());
        Assert.assertNotNull(Config.I.getHistioryDataFrequency());
        Assert.assertNotNull(Config.I.getDataSendFilePath());
    }

}