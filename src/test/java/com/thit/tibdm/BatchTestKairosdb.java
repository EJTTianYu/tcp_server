package com.thit.tibdm;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.kairosdb.client.builder.MetricBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BatchTestKairosdb {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchTestKairosdb.class);

    public static int tagNumber = 20;
    public static int metricNumber = 20;
    public static int timeNumber = 3600;

    private List<Long> init() {
        List<Long> list = Lists.newArrayList();
        long curr = System.currentTimeMillis();
        for (int i = 0; i < timeNumber; i++) {
            list.add(curr + i);
        }
        return list;
    }

    private List<String> initMetric() {
        List<String> list = Lists.newArrayList();
        for (int i = 0; i < metricNumber; i++) {
            list.add("TT" + i);
        }
        return list;
    }

    private List<String> initTag() {
        List<String> list = Lists.newArrayList();
        for (int i = 0; i < tagNumber; i++) {
            list.add(i + "");
        }
        return list;
    }

    @Test
    public void batchTest() throws InterruptedException {
        BatchTestKairosdb batch = new BatchTestKairosdb();
        List<Long> time = batch.init();
        List<String> metric = batch.initMetric();
        List<String> tag = batch.initTag();
        /**
         * 一次传输120条数据
         */
        for (int i = 0; i < time.size(); i++) {
            for (String t : tag) {
                MetricBuilder builder = MetricBuilder.getInstance();
                for (String m : metric) {
                    builder.addMetric(m)
                            .addTag("machine_id", t)
                            .addDataPoint(time.get(i), java.util.Optional.of(1.02));
                }
                long start = System.currentTimeMillis();
                HttpUtil.sendKairosdb(builder);
                long end = System.currentTimeMillis();
                LOGGER.info("消耗时间：{}", end - start);
                LOGGER.info("写入的标签数量：{}", metric.size());
            }
        }
    }

    public void testInsert(){

    }
}
