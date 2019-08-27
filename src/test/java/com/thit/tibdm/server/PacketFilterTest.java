package com.thit.tibdm.server;

import com.google.common.collect.Lists;
import com.thit.tibdm.BatchTestKairosdb;
import com.thit.tibdm.RedisUtil;
import org.junit.Assert;
import org.junit.Test;
import org.redisson.api.RBloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.xml.Atom;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class PacketFilterTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(PacketFilterTest.class);

    @Test
    public void isExis() {
        List<Long> timeList = new ArrayList<>();
        long curr = System.currentTimeMillis();
        /**
         * 构建14400个时间点
         */
        for (int i = 0; i < 14400; i++) {
            timeList.add(curr + 250 * i);
        }
        int tcount = 0;
        int fcount = 0;
        int clusterTcount = 0;
        int clusterFcount = 0;

        long start = System.currentTimeMillis();
        RedisUtil.I.getRedisson().getBloomFilter("bloomfilter_test:1000").delete();
        for (Long time : timeList) {
            boolean exisCluster = PacketFilterDistributed.I.isExis("test", "1000", time, 500);
            boolean exis = PacketFilter.I.isExis("1000", time);
            if (exis) {
                tcount++;
            } else {
                fcount++;
            }
            if (exisCluster) {
                clusterTcount++;
            } else {
                clusterFcount++;
            }
        }
        LOGGER.info("插入个数：{}", fcount);
        LOGGER.info("未插入个数：{}", tcount);
        LOGGER.info("插入个数：{}", clusterFcount);
        LOGGER.info("未插入个数：{}", clusterTcount);
        long end = System.currentTimeMillis();
        LOGGER.info("消耗时间：{}", end - start);
        RedisUtil.I.getRedisson().getBloomFilter("bloomfilter_test:1000").delete();
        boolean clusterFlag = false;
        boolean flag = false;
        if (Math.abs(tcount - fcount) < 50) {
            flag = true;
        }

        if (Math.abs(clusterTcount - clusterFcount) < 50) {
            clusterFlag = true;
        }
        Assert.assertTrue(flag);
        Assert.assertTrue(clusterFlag);
    }

    @Test
    public void testBloomFilterCluster() {
        int sampleSize = 10000;
        List<Long> timeList = Lists.newArrayList();
        long curr = System.currentTimeMillis();
        for (int i = 0; i < sampleSize; i++) {
            timeList.add(curr);
            curr = curr + 500;
        }
        AtomicInteger insert = new AtomicInteger(0);
        AtomicInteger noInsert = new AtomicInteger(0);
        RBloomFilter<Long> test = RedisUtil.I.getRedisson().getBloomFilter("bloom_filter_test");
        test.tryInit(144000L, 0.0003);
        long start = System.currentTimeMillis();
        timeList.forEach(time -> {
            time = time / 1000;
            if (test.contains(time)) {
                noInsert.incrementAndGet();
            } else {
                insert.incrementAndGet();
                test.add(time);
            }
        });
        long end = System.currentTimeMillis();
        boolean flag = false;
        if (Math.abs(noInsert.get() - insert.get()) < 10) {
            flag = true;
        }
        Assert.assertTrue(flag);
        LOGGER.info("消耗时间：{}", end - start);
        LOGGER.info("插入数据：{}个", insert.get());
        LOGGER.info("未插入数据：{}个", noInsert.get());
        LOGGER.info("数据总量：{}个", test.count());
        test.delete();
    }
}