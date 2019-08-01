package com.thit.tibdm.forward;

import com.thit.tibdm.Config;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author: dongzhiquan  Date: 2017/9/3 Time: 12:10
 */
public class RemoteConnect {


    /**
     * 日志
     */
    public static final Logger LOGGER = LoggerFactory.getLogger(RemoteConnect.class);


    /**
     * 连接状态
     */
    private AtomicBoolean flag = new AtomicBoolean(false);

    /**
     * 创建的开始类
     * 1、构建远程连接处理类
     * 2、设置了NIO的两个线程池处理类
     * 3、设置连接处理
     * 4、监控了数据的连接
     * 5、当连接中断的时候进行重新连接
     * @param bootstrap
     * @param eventLoop
     * @return
     */
    public Bootstrap createBootstrap(Bootstrap bootstrap, EventLoopGroup eventLoop) {
        if (bootstrap != null) {
            final RemoteConnectionHandler handler = new RemoteConnectionHandler(this);
            bootstrap.group(eventLoop);
            bootstrap.channel(NioSocketChannel.class);
            bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel socketChannel) {
                    socketChannel.pipeline().addLast(handler);
                }
            });
            LOGGER.error("地址：{}:{}", Config.I.getRemoteForwardIp(), Config.I.getRemoteForwardPort());
            ChannelFuture future = bootstrap.connect(Config.I.getRemoteForwardIp(),
                    Config.I.getRemoteForwardPort());
            future.addListener((ChannelFutureListener) futureListener -> {
                if (futureListener.isSuccess()) {
                    instance = futureListener.channel();
                    flag.compareAndSet(false, true);
                    LOGGER.info("连接成功!");
                } else {
                    LOGGER.error("连接失败，十秒后重试");
                    final EventLoop loop = futureListener.channel().eventLoop();
                    loop.schedule(() -> {
                        this.createBootstrap(new Bootstrap(), loop);
                    }, 10, TimeUnit.SECONDS);
                }
            });
        }
        return bootstrap;
    }

    /**
     * nio类
     */
    private EventLoopGroup loop = new NioEventLoopGroup();

    /**
     * 构造远程连接实体类
     */
    RemoteConnect() {
        createBootstrap(new Bootstrap(), loop);
    }


    /**
     * 管道
     */
    private Channel instance;


    /**
     * getConnect
     *
     * @return Channel
     */
    public Channel getConnect() {
        return instance;
    }

    /**
     * 获取可连接状态
     *
     * @return
     */
    public boolean getStatus() {
        return flag.get();
    }

    /**
     * 修改可连接状态
     *
     * @return
     */
    public void updateStatus(boolean bool) {
        flag.set(bool);
    }

    @Override
    public String toString() {
        return "RemoteConnect{" +
                "flag=" + flag +
                ", instance=" + instance +
                '}';
    }
}
