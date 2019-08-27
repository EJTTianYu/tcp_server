package com.thit.tibdm.realtime;

import com.google.inject.AbstractModule;

/**
 * 实时数据模块
 * 核心：
 *    将实时数据存储到实时数据库中，可能存储到不同的实时数据库，例如直接存储redis,memcache等，或者直接
 * 直接存储本地的服务。
 *
 * @author wanghaoqiang
 * @date 2018/4/12
 * @time 11:22
 */
public class RealTimeModule extends AbstractModule {
    @Override
    protected void configure() {

    }
}
