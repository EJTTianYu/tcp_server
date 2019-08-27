package com.thit.tibdm.mqtt;

import com.thit.tibdm.BatchTestKairosdb;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * mqtt测试类
 */
public class MqttTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MqttTest.class);
    private static String host = "tcp://166.111.80.45:61613";

    private static String topicStr = "mqtt/topic";


    private MqttClient initConnect(String clientId, String userName) throws MqttException {
        //host为主机名，test为clientid即连接MQTT的客户端ID，一般以客户端唯一标识符表示，
        //MemoryPersistence设置clientid的保存形式，默认为以内存保存
        MqttClient client = new MqttClient(host, clientId, new MemoryPersistence());
        MqttConnectOptions options = new MqttConnectOptions();
        //设置是否清空session,这里如果设置为false表示服务器会保留客户端的连接记录，
        //这里设置为true表示每次连接到服务器都以新的身份连接
        options.setCleanSession(true);
        //设置连接的用户名
        options.setUserName("admin");
        //设置连接的密码
        options.setPassword("password".toCharArray());
        // 设置超时时间 单位为秒
//        options.setConnectionTimeout(10);
        // 设置会话心跳时间 单位为秒 服务器会每隔1.5*20秒的时间向客户端发送个消息判断客户端是否在线，但这个方法并没有重连的机制
//        options.setKeepAliveInterval(20);

        options.setAutomaticReconnect(true);
        //回调
        client.setCallback(new MqttCallbackExtended() {

            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                LOGGER.info("完成连接");
            }

            @Override
            public void connectionLost(Throwable cause) {
                // //连接丢失后，一般在这里面进行重连
                LOGGER.info("断开连接");
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                //subscribe后得到的消息会执行到这里面
                LOGGER.info("消息到达----------");
                LOGGER.info("话题:{},消息:{}", topic, message.toString());
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                //publish后会执行到这里
                LOGGER.info("推送消息完毕：{}"
                        , token.isComplete());
            }
        });
        //链接
        client.connect(options);
        return client;
    }

    @Test
    public void testSub() throws MqttException, InterruptedException {
        MqttClient sub = initConnect("sub", "sub");
        //订阅
        sub.subscribe(topicStr, 1);
        Thread.sleep(10000);
    }

    @Test
    public void testPub() throws MqttException, InterruptedException {
        MqttClient pub = initConnect("pub", "pub");
        for (int i = 0; i < 100; i++) {
            String s = "hello world" + i;
            pub.publish(topicStr, new MqttMessage(s.getBytes()));
            Thread.sleep(10);
        }
    }

}
