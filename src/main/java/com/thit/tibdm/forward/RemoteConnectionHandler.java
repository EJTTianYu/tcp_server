package com.thit.tibdm.forward;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author: dongzhiquan  Date: 2017/9/3 Time: 12:11
 */
public class RemoteConnectionHandler extends ChannelInboundHandlerAdapter {

    /**
     * 日志类
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteConnectionHandler.class);

    /**
     * 远程连接类
     */
    private RemoteConnect client;

    /**
     * 远程连接构造类
     * @param client
     */
    public RemoteConnectionHandler(RemoteConnect client) {
        this.client = client;
    }

    /**
     * 远程连接连接监控
     * @param ctx
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        LOGGER.info("已经连接上服务器。。。");
    }

    /**
     * 数据读取方法
     * @param ctx
     * @param msg
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;
        try {
            byte[] array = new byte[buf.readableBytes()];
            buf.readBytes(array);
            LOGGER.debug("收到回应的长度：" + array.length);
        } finally {
            buf.release();
        }

    }


    /**
     * 监控连接异常
     * @param ctx
     * @param cause
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
        LOGGER.error("发生异常，断开：{}", cause);
    }

    /**
     * 异常断开后的处理逻辑
     * 当连接断开了之后就每十秒进行一次重连
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.error("远程连接断开，准备重连");
        super.channelInactive(ctx);
        final EventLoop eventLoop = ctx.channel().eventLoop();
        client.updateStatus(false);
        eventLoop.schedule(()->{
            client.createBootstrap(new Bootstrap(),eventLoop);
        },10, TimeUnit.SECONDS);
    }
}
