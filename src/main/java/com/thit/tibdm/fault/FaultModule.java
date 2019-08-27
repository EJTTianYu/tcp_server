package com.thit.tibdm.fault;

import com.google.inject.AbstractModule;

/**
 * 载入故障模块
 * 核心：
 *    根据给出的故障数据然后出具故障报表，可能存在变化的就是不同的故障算法实现
 *
 * @author wanghaoqiang
 */
public class FaultModule extends AbstractModule {
    @Override
    protected void configure() {

    }
}
