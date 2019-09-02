package com.thit.tibdm;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.RollupTask;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Response;
import org.kairosdb.client.response.RollupResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 发送数据到kairosdb
 *
 * @author wanghaoqiang
 */
public class HttpUtil {

    /**
     * 日志
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtil.class);

    /**
     * 静态内部类单例
     */
    private static class SingletonHolder {
        private static final HttpUtil INSTANCE = new HttpUtil();
    }

    /**
     * 获取单例对象
     * @return
     */
    public static HttpUtil getInstance() {
        return SingletonHolder.INSTANCE;
    }

    /**
     * 客户端类
     */
    private HttpClient client;
    /**
     * 自增类
     */
    private AtomicInteger index = new AtomicInteger();

    /**
     * IKR的客户端
     */
    private HttpClient ikrClient;

    /**
     * 客户端负载均衡方法
     * @return
     */
    public HttpClient getIkrClient(){
        return ikrClient;
    }

    /**
     * 私有构造类
     */
    private HttpUtil() {
        LOGGER.info(Config.I.getIkrUrl());
        try {
            ikrClient = new HttpClient(Config.I.getIkrUrl());
            ikrClient.setMaxTotal(128);
        }catch (MalformedURLException e) {
            LOGGER.error("发生异常{}", e);
        }
    }

    /**
     * 发送数据写入请求
     * @param builder
     */

    public static void sendKairosdb(MetricBuilder builder) {
        try {
            Response response = getInstance().getIkrClient().pushMetrics(builder);
            if (response.getStatusCode()!=204){
                LOGGER.error("插入失败{}",response.getErrors());
            }
//            if (!builder.getMetrics().get(0).getName().toString().equals("use_rawdata")) {
//                LOGGER.info("IKR写入成功,写入tag为:{},写入第一个metric为:{}",
//                    builder.getMetrics().get(0).getTags().toString(),
//                    builder.getMetrics().get(0).getName().toString());
//            }
        } catch (IOException e) {
            LOGGER.error("发生异常{}", e);
        }
    }

    /**
     * 发送数据查询请求
     * @param builder
     * @return
     */
    public static QueryResponse sendQuery(QueryBuilder builder) {
        QueryResponse response = null;
        try {
            response = getInstance().getIkrClient().query(builder);
        } catch (IOException e) {
            LOGGER.error("发生异常{}", e);
        }
        return response;
    }

    public static ImmutableList<RollupTask> getrollupTasks() {
        RollupResponse rollupTasks = null;
        ImmutableList<RollupTask> tasks = null;
        try {
            rollupTasks = getInstance().getIkrClient().getRollupTasks();
            tasks = rollupTasks.getRollupTasks();
        } catch (IOException e) {
            LOGGER.error("{}", e);
        }
        return tasks;
    }
}
