package com.thit.tibdm.server;

import com.thit.tibdm.Config;
import com.thit.tibdm.RedisUtil;
import org.redisson.api.RBloomFilter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 分布式的数据过滤
 *
 * @author administartor
 */
public enum PacketFilterDistributed {
    /**
     * 实例
     */
    I;

    /**
     * 存放20个小时的数据
     */
    private int size = 14400;
    /**
     * 错误概率
     */
    private double fpp = 0.001;
    /**
     * 过滤器容器
     */
    private Map<String, RBloomFilter<Long>> machinePacketCache;

    /**
     * 单例启动
     */
    PacketFilterDistributed() {
        machinePacketCache = new ConcurrentHashMap<>();
    }


    /**
     * 判断是否存在
     *
     * @param machineId
     * @param time
     * @return
     */
    public boolean isExis(String prefix, String machineId, long time, int frequency) {
        String name = prefix + ":" + machineId;
        boolean flag = false;
        if (machinePacketCache.containsKey(name)) {
            RBloomFilter<Long> filter = machinePacketCache.get(name);
            if (filter.count() > size) {
                filter = createFilter(name);
                machinePacketCache.put(name, filter);
            } else {
                flag = filter.contains(time / frequency);
                if (!flag) {
                    filter.add(time / frequency);
                }
            }
        } else {
            RBloomFilter<Long> filter = createFilter(name);
            filter.add(time / frequency);
            machinePacketCache.put(name, filter);
        }
        return flag;
    }


    /**
     * 现将上一个bloomfilter删除
     * 然后生成新的bloomfilter
     *
     * @param name
     * @return
     */
    private RBloomFilter<Long> createFilter(String name) {
        RBloomFilter<Long> bloomFilter = RedisUtil.I.getRedisson().getBloomFilter("bloomfilter_" + name);
        bloomFilter.delete();
        bloomFilter = RedisUtil.I.getRedisson().getBloomFilter("bloomfilter_" + name);
        bloomFilter.tryInit(size * 2, fpp);
        return bloomFilter;
    }
}
