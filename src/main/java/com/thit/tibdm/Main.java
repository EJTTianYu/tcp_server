package com.thit.tibdm;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.thit.tibdm.net.NetModule;
import com.thit.tibdm.net.SFSNet.SFSConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 启动类
 */
public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private Injector injector;
    /**
     * 主要功能：
     * 1.解析命令行参数
     * 2.设置环境变量，加载配置文件
     * 3.启动程序
     * @param args
     */
    public static void main(String[] args) {
        //加载配置文件，以及绑定Guice类
        Main main = new Main();
        main.loadMoudle();
        //启动程序，增加钩子，在停止的时候优雅停止
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            //执行stop程序
        }));
        SFSConfiguration instance = main.injector.getInstance(SFSConfiguration.class);
        LOGGER.info("测试注入端口：{}",instance.getTcpPort());
    }

    public Main() {
    }

    private void loadMoudle(){
        Map<String,String> props=new HashMap<>();
        ResourceBundle prop = ResourceBundle.getBundle("tcpserver");
        Enumeration<String> keys = prop.getKeys();
        while (keys.hasMoreElements()){
            String key = keys.nextElement();
            props.put(key,prop.getString(key));
        }
        injector = Guice.createInjector(new NetModule(props));
    }

    /**
     * 启动服务
     * 启动guice中所有绑定的继承了ERM接收服务的类
     */
    private void startService(){

    }
}
