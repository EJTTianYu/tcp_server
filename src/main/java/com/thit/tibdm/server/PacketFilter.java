package com.thit.tibdm.server;

import com.thit.tibdm.Config;
import com.thit.tibdm.RedisUtil;
import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import org.redisson.api.RBloomFilter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 数据包过滤
 * 在同一个500MS内每个车只允许有一个数据包
 * 采用并发安全的map和布隆过滤器来判断是否已经存在数据包了
 * 分布式的版本在等等吧
 *
 *
 * @author administartor
 */
public enum PacketFilter {
    /**
     * 实例化
     */
    I;

    /**
     * 存放20个小时的数据
     */
    private int size = 144000;
    /**
     * 允许错误概率
     */
    private double fpp = 0.001;
    /**
     * 布隆过滤器的缓存
     */
    private Map<String, BloomFilter<Long>> machinePacketCache;
    /**
     * 数据过滤的频率
     */
    private int frequency;

    /**
     * 单例启动
     */
    PacketFilter() {
        machinePacketCache = new ConcurrentHashMap<>();
        frequency = Config.I.getEveryMachineFrequency();
    }


    /**
     * 这个数据包是否已经存在系统中
     * @param machineId
     * @param time
     * @return
     */
    public boolean isExis(String machineId, long time) {
        boolean flag = false;
        if (machinePacketCache.containsKey(machineId)) {
            BloomFilter<Long> filter = machinePacketCache.get(machineId);
            if (filter.getEstimatedPopulation() > size) {
                filter = createFilter();
                machinePacketCache.put(machineId, filter);
            } else {
                flag = filter.contains(time / frequency);
                if (!flag) {
                    filter.add(time / frequency);
                }
            }
        } else {
            BloomFilter<Long> filter = createFilter();
            filter.add(time / frequency);
            machinePacketCache.put(machineId, filter);
        }

        return flag;
    }

    /**
     * 创建布隆过滤器
     * @return
     */
    private BloomFilter<Long> createFilter() {
        return new FilterBuilder(size * 2, fpp)
                .buildBloomFilter();
    }

}
