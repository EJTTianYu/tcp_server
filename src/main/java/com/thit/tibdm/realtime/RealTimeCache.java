package com.thit.tibdm.realtime;

import com.google.common.collect.Maps;
import com.thit.tibdm.Config;
import com.thit.tibdm.Contents;
import com.thit.tibdm.RedisUtil;
import org.redisson.api.RMap;

import java.util.Map;

/**
 * 由于增加了双协议支持，所以需要增加头尾包的时间参数来区分
 */
public enum RealTimeCache {
    /**
     * 实例化
     */
    I;

    /**
     * 实时数据的缓存
     */
    private Map<String, RealTimeEntity> lastCache;

    /**
     * 单例构造实时数据容器
     */
    RealTimeCache() {
        lastCache = Maps.newConcurrentMap();
    }


    /**
     * 传输车号，时间以及类型
     *
     * @param machineId
     * @param time
     * @param type
     * @return
     */
    public boolean isUpdate(String machineId, long time, String type) {
        boolean flag = false;
        if (!lastCache.containsKey(machineId)) {
            long l = loadFromRedis(machineId, type);
            lastCache.put(machineId, new RealTimeEntity(machineId, l, type));
            if (timeTransport(time) > l) {
                flag = true;
            }
        } else {
            /**
             * 数据的时间需要大于上一次数据包的时间，并且不能大于现在系统时间过多
             */
            long curr = System.currentTimeMillis();
            long last;
            if (Contents.HEAD_TIME.equals(type)) {
                last = lastCache.get(machineId).getHeadTime();
            } else {
                last = lastCache.get(machineId).getTailTime();
            }
            if (timeTransport(time) > last) {
                if (time < curr || ((time - curr) < 300000)) {
                    lastCache.put(machineId, new RealTimeEntity(machineId, timeTransport(time), type));
                    flag = true;
                }
            }
        }
        return flag;
    }


    /**
     * 从redis中载入实时数据
     * @param machineId 车号
     * @param type  类型
     * @return
     */
    private long loadFromRedis(String machineId, String type) {
        long time;
        RMap<String, String> map = RedisUtil.I.getRedisson().getMap("status_" + machineId);
        if (map == null || map.size() == 0 || (!map.containsKey(type))) {
            time = 0;
        } else {
            time = timeTransport(Long.parseLong(map.get(type)));
        }
        return time;
    }

    /**
     * 时间转换
     * @param timestamp 时间戳
     * @return
     */
    private long timeTransport(long timestamp) {
        return timestamp / Config.I.getRealTimeMachineFrequency() * Config.I.getRealTimeMachineFrequency();
    }
}