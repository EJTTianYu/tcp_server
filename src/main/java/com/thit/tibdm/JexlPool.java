package com.thit.tibdm;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * jexl运算池
 * jexl表达式里面为运算引擎
 *
 * @author wanghaoqiang
 */
public enum JexlPool {
    /**
     * 实例化
     */
    I;

    /**
     * jexl引擎
     */
    private JexlEngine jexlEngine;
    /**
     * guava的缓存实例
     */
    private LoadingCache<String, Expression> cache;
    /**
     * 日志类
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(JexlPool.class);

    /**
     * 构造guava缓存实例
     * 在jexl表达式运算的过程中，最消耗时间的过程就是在生成jexl表达式Expression实体
     * 中，这个过程复杂的消耗的时间甚至能够达到40MS，相对于解析时间这个时间是非常多的，
     * 所以在这里增加缓存
     */
    JexlPool() {
        jexlEngine = new JexlEngine();
        cache = CacheBuilder.newBuilder()
                .maximumSize(4096)
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .build(new CacheLoader<String, Expression>() {
                    @Override
                    public Expression load(String jexlExp) {
                        return generateValueByKey(jexlExp);
                    }
                });
    }

    /**
     * 根据表达式获取表达式计算
     *
     * @param jexlExp
     * @return
     */
    private Expression generateValueByKey(String jexlExp) {
        return jexlEngine.createExpression(jexlExp);
    }

    /**
     * 进行表达式的运算
     *
     * @param jexlExp
     * @param jc
     * @return
     */
    public String doConvert(String jexlExp, JexlContext jc) {
        String result = "0";

        try {
            result = cache.get(jexlExp).evaluate(jc).toString();
        } catch (Exception e) {
            //TODO 太多报错了先注释了把
        }
        return result;
    }
}
