package com.thit.tibdm.historydata;

import com.google.inject.AbstractModule;

/**
 * 历史数据保存模块
 * 核心：
 *    将历史数据存放到时序数据库中，可能存在存放到不同的时序数据库中，kairosdb,opentsdb等
 * @author wanghaoqiang
 */
public class HistoryDataSaveModule extends AbstractModule {
    @Override
    protected void configure() {

    }
}
