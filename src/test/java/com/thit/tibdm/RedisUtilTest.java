package com.thit.tibdm;

import com.codahale.metrics.Timer;
import com.google.common.collect.Sets;
import com.thit.tibdm.parse.Variable;
import org.junit.Test;
import org.redisson.api.RBlockingDeque;
import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.redisson.api.RTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.plaf.ViewportUI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class RedisUtilTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisUtilTest.class);

    @Test
    public void testHget() throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            Thread.sleep(10);
            RMap<String, String> pro_detail = RedisUtil.I.getRedisson().getMap("pro_detail");
            RMap<String, String> machine_mapper_pro = RedisUtil.I.getRedisson().getMap("machine_mapper_pro");
            if (pro_detail != null) {
                LOGGER.info(pro_detail.entrySet().size() + "");
            }
            if (machine_mapper_pro != null) {
                LOGGER.info(pro_detail.entrySet().size() + "");
            }
        }
    }

    @Test
    public void testHset() throws InterruptedException {
        Set<String> field= Sets.newHashSet();
        field.add("COLLECT_TIME");
        field.add("CY29");
        field.add("CY10");

        RMap<String, String> test = RedisUtil.I.getRedisson().getMap("status_1701");
        for (int i = 0; i < 100; i++) {
            Map<String, String> stringStringMap = test.getAll(field);
            LOGGER.info(stringStringMap.toString());
            Thread.sleep(5);
        }
    }

    @Test
    public void testPub() throws InterruptedException {
        RTopic<String> topic = RedisUtil.I.getRedisson().getTopic("channel_test");
        topic.addListener((channel, message) -> {
            LOGGER.info(message);
        });

        for (int i = 0; i < 10; i++) {
            topic.publish(i + "");
            Thread.sleep(10);
        }
    }



    /**
     * 经测试，可以放心使用分布式缓存
     * @throws InterruptedException
     */
    @Test
    public void testTime() throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            Timer timer = MetricMonitor.I.registryTimer("test");
            Timer.Context context = timer.time();
            RMap<String, String> map = RedisUtil.I.getRedisson().getMap("fault_" + 344);
            map.get("GZ2880");
            context.stop();
            Thread.sleep(10);
        }
    }

    @Test
    public void testRealTime(){
        long curr = System.currentTimeMillis();
        LOGGER.info(curr/1000*1000+"");
    }


    @Test
    public void testQueue() throws InterruptedException {
        RBlockingDeque<String> rollUpQueue = RedisUtil.I.getRedisson().getBlockingDeque("roll_up_queue");
        for (int i = 0; i < 3; i++) {
            new Thread(()->{
                while (true){
                    LOGGER.info("{}线程拿到队列信息：{}",Thread.currentThread().getName(),rollUpQueue.poll());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }

        Thread.sleep(5000);
    }

    @Test
    public void testQueueAdd() throws InterruptedException {
        RBlockingDeque<String> rollUpQueue = RedisUtil.I.getRedisson().getBlockingDeque("roll_up_queue");
        for (int i = 0; i < 10; i++) {
            rollUpQueue.offer(String .valueOf(i));
            Thread.sleep(500);
        }
    }

    @Test
    public void test(){

        RMap<Object, Object> warncal_1701 = RedisUtil.I.getRedisson().getMap("warncal_1701");
        Map<String,String> cal=new HashMap<>();
        cal.put("ss1","sss");
        warncal_1701.putAll(cal);
        warncal_1701.put("ww1","ww");



    }

}