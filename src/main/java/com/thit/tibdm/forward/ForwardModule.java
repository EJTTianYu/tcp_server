package com.thit.tibdm.forward;

import com.google.inject.AbstractModule;

/**
 * 转发模块
 * 核心：
 *    将数据转发到某个服务器，可能拥有不同的转发策略，比如按照车号过滤，按照协议过滤等
 * @author wanghaoqiang
 */
public class ForwardModule extends AbstractModule {
    @Override
    protected void configure() {

    }
}
