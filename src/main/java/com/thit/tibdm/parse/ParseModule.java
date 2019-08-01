package com.thit.tibdm.parse;

import com.google.inject.AbstractModule;

/**
 * 数据解析模块
 * 核心：
 *    将数据从二进制数据转换为实际的数据，可能存在不同的解析规则，比如直接将二进制转换为数值，或者二进制转
 * String 然后直接存储。
 *
 * @author wanghaoqiang
 */
public class ParseModule extends AbstractModule {
    @Override
    protected void configure() {

    }
}
