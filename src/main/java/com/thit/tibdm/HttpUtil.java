package com.thit.tibdm;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
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

    private static AtomicInteger packetCnt = new AtomicInteger(0);

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
        FutureTask<Boolean> kairosSendTask = new FutureTask<Boolean>((Callable<Boolean>) () -> {
            try {
                getInstance().getClient().pushMetrics(builder);
                return true;
            } catch (Exception e) {
                LOGGER.error("写入Kairos发生异常,写入tag为:{}",
                    builder.getMetrics().get(0).getTags().toString(), e);
                return false;
            }
        });
        FutureTask<Boolean> ikrSendTask = new FutureTask<Boolean>((Callable<Boolean>) () -> {
            try {
                getInstance().getIkrClient().pushMetrics(builder);
                return true;
            } catch (Exception e) {
                LOGGER.error("发生Ikr异常{},写入tag为:{}",
                    builder.getMetrics().get(0).getTags().toString(), e);
                return false;
            }
        });
        try {
            ThreadPoolManager.I.getKairosdbSenderThreadPool().submit(new Thread(kairosSendTask));
        } catch (Exception e) {
            LOGGER.error("创建Kairos写入线程异常{}", e);
        }
        try {
            ThreadPoolManager.I.getKairosdbSenderThreadPool().submit(new Thread(ikrSendTask));
        } catch (Exception e) {
            LOGGER.error("创建Ikr写入线程异常{}", e);
        }
        try {
            if (kairosSendTask.get() && ikrSendTask.get()) {
                packetCnt.getAndIncrement();
                if (packetCnt.get() % 50 == 0) {
                    LOGGER
                        .info("数据写入成功,写入tag为:{}", builder.getMetrics().get(0).getTags().toString());
                    packetCnt.set(0);
                }
            }
        } catch (Exception e) {
            LOGGER.error("写入流程发生异常{}", e);
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
