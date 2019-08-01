package com.thit.tibdm.net;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

import java.util.Map;

/**
 * 网络模块
 * 核心：
 * 从车载ERM接收数据，可能存在不同的协议，比如四方所私有协议，MQTT协议等
 *
 * @author wanghaoqiang
 * @date 2018/4/12
 * @time 11:25
 */
public class NetModule extends AbstractModule {

    private Map<String,String> prop;

    @Override
    protected void configure() {
        Names.bindProperties(binder(),prop);
    }

    public NetModule(Map<String,String> properties) {
        this.prop = properties;
    }
}
