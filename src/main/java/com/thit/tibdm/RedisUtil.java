package com.thit.tibdm;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.TransportMode;

/**
 * redis工具类
 * @author administartor
 */
public enum  RedisUtil {
    /**
     * 实例化
     */
    I;

    /**
     * redisson客户端类
     */
    private RedissonClient redisson;

    /**
     * 客户端生成
     */
    RedisUtil(){
        org.redisson.config.Config config = new org.redisson.config.Config();
        config.setTransportMode(TransportMode.NIO);
        config.useSingleServer().setAddress(Config.I.getRedisIp());
        config.setCodec(StringCodec.INSTANCE);
        redisson = Redisson.create(config);
    }

    /**
     * 获取redis操作客户端
     * @return
     */
    public RedissonClient getRedisson() {
        return redisson;
    }
}
