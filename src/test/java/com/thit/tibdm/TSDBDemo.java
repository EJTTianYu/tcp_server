package com.thit.tibdm;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.thit.tibdm.fault.FaultJobEntity;
import org.junit.Test;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.*;
import org.kairosdb.client.builder.grouper.TagGrouper;
import org.kairosdb.client.response.Query;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Result;
import org.kairosdb.client.response.RollupResponse;
import org.redisson.api.RBlockingDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wanghaoqiang
 * @date 2018/5/3
 * @time 11:08
 */
public class TSDBDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(TSDBDemo.class);

    private ImmutableList<Integer> speedDataPoints = ImmutableList.of(1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 33, 44,
            55, 667, 33, 889, 0, 2, 33);
    private ImmutableList<Integer> shockDataPoints = ImmutableList.of(88, 2, 3, 2, 7, 8, 88, 22, 31, 35, 5, 44, 44,
            66, 100, 3, 22, 0, 1, 111);

    private final String motorId = "87uuf";
    private final String boxId = "uujjnfi00";
    private final long startTime = System.currentTimeMillis();
    private final String url = "http://192.168.70.53:8080";
    private final String metric1 = "speed";
    private final String metric2 = "shock";
    private final String stringTest = "stringTest";

    /**
     * 数据插入测试
     */
    @Test
    public void insert() {
        MetricBuilder builder = MetricBuilder.getInstance();
        Metric speed = builder.addMetric(metric1)
                .addTag("motor_id", motorId)
                .addTag("box_id", boxId);
        for (int i = 0; i < speedDataPoints.size(); i++) {
            speed.addDataPoint(startTime + i, speedDataPoints.get(i));
        }
        Metric shock = builder.addMetric(metric2)
                .addTag("motor_id", motorId)
                .addTag("box_id", boxId);
        for (int i = 0; i < shockDataPoints.size(); i++) {
            shock.addDataPoint(startTime + i, shockDataPoints.get(i));
        }
        sendKairosdb(builder);
    }


    /**
     * 发送插入请求
     *
     * @param builder
     */
    private void sendKairosdb(MetricBuilder builder) {
        try {
            HttpClient client = HttpUtil.getInstance().getClient();
            client.pushMetrics(builder);
        } catch (IOException e) {
            LOGGER.error("发生异常{}", e);
        }
    }

    /**
     * 查询数据
     */
    @Test
    public void query() {
        QueryBuilder builder = QueryBuilder.getInstance();
        builder.setStart(new Date(startTime))
                .setEnd(new Date(startTime + speedDataPoints.size()))
                .addMetric(metric1)
                .addTag("motor_id", motorId)
                .addTag("box_id", boxId);
        builder.setStart(new Date(startTime))
                .setEnd(new Date(startTime + shockDataPoints.size()))
                .addMetric(metric2)
                .addTag("motor_id", motorId)
                .addTag("box_id", boxId);
        QueryResponse response = sendQuery(builder);
        LOGGER.info(response.getBody());
    }

    private QueryResponse sendQuery(QueryBuilder builder) {
        QueryResponse response = null;
        try {
            HttpClient client = new HttpClient(url);
            response = client.query(builder);
            client.shutdown();
        } catch (IOException e) {
            LOGGER.error("发生异常{}", e);
        }
        return response;
    }

    /**
     * 查询原始的数值
     * 单个
     */
    @Test
    public void queryRaw() throws IOException {
        long end = System.currentTimeMillis();
        long start = System.currentTimeMillis() - 60 * 60 * 1000;
        QueryBuilder builder = QueryBuilder.getInstance();
        builder.setStart(new Date(start))
                .setEnd(new Date(end))
                .addMetric("use_rawdata")
                .addTag("machine_id", "1701");
        QueryResponse response = sendQuery(builder);
        Query query = response.getQueries().get(0);
        Result result = query.getResults().get(0);
        List<DataPoint> dataPoints = result.getDataPoints();
        final long[] count = {0};
        dataPoints.forEach(dataPoint -> {
            String s = dataPoint.getValue().toString();
            long startTime = System.currentTimeMillis();
            byte[] bytes = HexUtil.hexString2Bytes(s);
            long endTime = System.currentTimeMillis();
            count[0] = count[0] + (endTime - startTime);
        });
//        LOGGER.info(response.getBody());
        LOGGER.info("消耗时间：{}", count[0]);
    }

    /**
     * 查询原始的数值
     * 多个依次类推
     */
    @Test
    public void queryRawMore() {
        long end = System.currentTimeMillis();
        long start = System.currentTimeMillis() - 10 * 60 * 1000;
        QueryBuilder builder = QueryBuilder.getInstance();
        builder.setStart(new Date(start))
                .setEnd(new Date(end))
                .addMetric("CY29")
                .addTag("machine_id", "1704");
        builder.setStart(new Date(start))
                .setEnd(new Date(end))
                .addMetric("CY20")
                .addTag("machine_id", "1704");
        QueryResponse response = sendQuery(builder);
        LOGGER.info(response.getBody());
    }

    /**
     * 查询原始的数值
     * 单个通过函数计算
     */
    @Test
    public void queryFun() {
        long end = System.currentTimeMillis();
        long start = System.currentTimeMillis() - 10 * 60 * 1000;
        QueryBuilder builder = QueryBuilder.getInstance();
        builder.setStart(new Date(start))
                .setEnd(new Date(end))
                .addMetric("CY29")
                .addTag("machine_id", "1704")
                .addAggregator(AggregatorFactory.createAverageAggregator(1, TimeUnit.MINUTES));
        QueryResponse response = sendQuery(builder);
        LOGGER.info(response.getBody());
    }

    @Test
    public void insertFault() throws InterruptedException {
        AtomicLong start = new AtomicLong(System.currentTimeMillis());
        ImmutableList<String> gzList = ImmutableList.of("0", "0", "1", "1", "1", "1", "0", "1", "0",
                "1", "1", "1", "0", "1", "1", "0", "1", "1", "0", "0", "1", "0", "0", "0", "0", "1", "0",
                "1", "1", "1", "1");
        MetricBuilder builder = MetricBuilder.getInstance();
        Metric metric = builder.addMetric("GZ_TEST_001")
                .addTag("machine_id", "222");
        gzList.forEach(l -> metric.addDataPoint(start.getAndAdd(500), l));
        for (int i = 0; i < 3600; i++) {
            Thread.sleep(200);
        }
        HttpUtil.sendKairosdb(builder);
    }

    /**
     * 主要为测试kairosdb使用scylladb 和 原生cassandra的性能区别
     * 模拟真实的使用场景，大概每次写入5000个左右的属性GZ,ZT,然后一个
     */
    @Test
    public void batchTestInsert() throws InterruptedException {
        for (int i = 0; i < 100000000; i++) {
            long time = System.currentTimeMillis();
            for (int j = 0; j < 400; j++) {
                long finalTime = time;
                int finalI = i;
                int finalJ = j;
                ThreadPoolManager.I.getStatusThread().execute(() -> {
                    MetricBuilder builder = MetricBuilder.getInstance();
                    for (int k = 0; k < 2500; k++) {
                        builder.addMetric("batchtest" + k)
                                .addTag("id", "test" + finalJ)
                                .addDataPoint(finalTime, finalI);
                    }
                    HttpUtil.sendKairosdb(builder);
                });
            }
            LOGGER.info("正在执行写入第{}时间点", i);
            Thread.sleep(5000);
        }
    }

    @Test
    public void testTimeInsert() throws InterruptedException {
        AtomicInteger count = new AtomicInteger();
        for (int i = 0; i < 1; i++) {
            for (int j = 0; j < 200; j++) {
                for (int k = 0; k < 2500; k++) {
                    int finalI = i;
                    int finalK = k;
                    int finalJ = j;
                    MetricBuilder builder = MetricBuilder.getInstance();
                    Thread.sleep(1);
//                    ThreadPoolManager.I.getKairosdbThread().execute(()->{
                    long time = System.currentTimeMillis();
                    for (int l = 0; l < 3600; l++) {
                        builder.addMetric("batchtest" + finalK)
                                .addTag("id", "test" + finalJ)
                                .addDataPoint(time, finalI);
                        time = time + 500;
                    }
                    HttpUtil.sendKairosdb(builder);
                    LOGGER.info("正在执行写入第{}点", count.incrementAndGet());
//                    });
                }
            }
        }
    }

    @Test
    public void testGZRollUp() throws InterruptedException {
        long curr = System.currentTimeMillis();
        Random random = new Random();
        while (true) {
            int gz23 = random.nextInt(2);
            int gz24 = random.nextInt(2);
            MetricBuilder builder = MetricBuilder.getInstance();
            builder.addMetric("GZ500")
                    .addTag("machine_id", "23")
                    .addDataPoint(curr, gz23);
            builder.addMetric("GZ500")
                    .addTag("machine_id", "24")
                    .addDataPoint(curr, gz24);
            curr = curr + 500;
            HttpUtil.sendKairosdb(builder);
            Thread.sleep(500);
        }
    }


    @Test
    public void sendJob() {
        long curr = System.currentTimeMillis();
        RBlockingDeque<String> rollUpQueue = RedisUtil.I.getRedisson().getBlockingDeque("roll_up_queue");
        FaultJobEntity job = new FaultJobEntity(1530769200000L, curr, "GZ146", "protocol34");
        rollUpQueue.offer(JSON.toJSONString(job));
    }


    @Test
    public void testGroup() {
        for (int i = 0; i < 100; i++) {
            long curr = System.currentTimeMillis();
            Map<String, String> machineIds = Maps.newHashMap();
            machineIds.put("machine_id", "1701");
            machineIds.put("machine_id", "1702");
            QueryBuilder builder = QueryBuilder.getInstance();
            QueryMetric queryMetric = builder.setStart(new Date(1))
                    .setEnd(new Date(curr))
                    .addMetric("CY29")
                    .addTags(machineIds)
                    .addGrouper(new TagGrouper("machine_id"));
            queryMetric.setOrder(QueryMetric.Order.DESCENDING);
            queryMetric.setLimit(1);
            LOGGER.info("系统进行了查询操作，进行日志记录");
            QueryResponse response = sendQuery(builder);
            response.getQueryResponse("");
            long end = System.currentTimeMillis();
            LOGGER.info("消耗时间:{}",end - curr);
            LOGGER.info("响应：{}", response.getBody());
        }
    }

    @Test
    public void generate() throws InterruptedException {
        Random random = new Random();
        long curr = System.currentTimeMillis();
        MetricBuilder builder = MetricBuilder.getInstance();
        while (true) {
            Thread.sleep(10);
            curr = curr + 1000;
            Metric metric = builder.addMetric("test_roll")
                    .addTag("machine_id", "1203");
            metric.addDataPoint(curr, random.nextInt(100));
            HttpUtil.sendKairosdb(builder);
            builder = MetricBuilder.getInstance();
        }
    }

    @Test
    public void testRollUp() throws IOException {
        HttpClient client = new HttpClient("http://node2:8080");
        RollupResponse rollup = client.getRollupTasks();
        ImmutableList<RollupTask> rollupTasks = rollup.getRollupTasks();
        LOGGER.info(rollupTasks.toString());
    }

}
