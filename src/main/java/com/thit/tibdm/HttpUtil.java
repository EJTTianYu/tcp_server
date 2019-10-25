package com.thit.tibdm;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.RollupTask;
import org.kairosdb.client.response.QueryResponse;
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
     * 获取客户端列表
     */
    private List<HttpClient> clients = Lists.newArrayList();
    /**
     * 客户端类
     */
    private HttpClient client;
    /**
     * 自增类
     */
    private AtomicInteger index = new AtomicInteger();

    /**
     * 客户端负载均衡方法
     * @return
     */
    public HttpClient getClient() {
        int listIndex = index.incrementAndGet() % clients.size();
        return clients.get(listIndex);
    }

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
        LOGGER.info(Config.I.getKairosdbUrlList().toString());
        LOGGER.info(Config.I.getIkrUrl());
        Config.I.getKairosdbUrlList().forEach(http -> {
            try {
                client = new HttpClient(http);
                client.setMaxTotal(128);
            } catch (MalformedURLException e) {
                LOGGER.error("发生异常{}", e);
            }
            clients.add(client);
        });
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
        int count = 0;
        try {
            getInstance().getClient().pushMetrics(builder);
        } catch (IOException e) {
            LOGGER.error("发生异常{}", e);
            count += 1;
        }
        try {
            getInstance().getIkrClient().pushMetrics(builder);
        } catch (IOException e) {
            LOGGER.error("发生异常{}", e);
            count += 2;
        }
        switch (count) {
            case 0:
                if (!builder.getMetrics().get(0).getName().toString().equals("use_rawdata")) {
                    LOGGER.info("数据写入成功,写入tag为:{}",
                        builder.getMetrics().get(0).getTags().toString());
                }
                break;
            case 3:
                LOGGER.error("写入数据异常,写入tag为{}", builder.getMetrics().get(0).getTags().toString());
                break;
            case 1:
                LOGGER.error("写入kairosDB异常,写入tag为:{}", builder.getMetrics().get(0).getTags().toString());
                break;
            case 2:
                LOGGER.error("写入ikr异常，写入tag为:{}", builder.getMetrics().get(0).getTags().toString());
                break;
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
            response = getInstance().getClient().query(builder);
        } catch (IOException e) {
//            LOGGER.error("发生异常{}", e);
            try {
                response = getInstance().getIkrClient().query(builder);
            } catch (IOException e1) {
                LOGGER.error("发生异常{}", e1);
            }
        }
        return response;
    }

    public static ImmutableList<RollupTask> getrollupTasks(){
        RollupResponse rollupTasks=null;
        ImmutableList<RollupTask> tasks =null;
        try {
             rollupTasks = getInstance().getClient().getRollupTasks();
             tasks = rollupTasks.getRollupTasks();
        } catch (IOException e) {
            try{
                rollupTasks = getInstance().getIkrClient().getRollupTasks();
                tasks = rollupTasks.getRollupTasks();
            } catch (IOException e1) {
                LOGGER.error("{}",e1);
            }
        }
        return tasks;
    }
}
