package com.thit.tibdm.parse;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author: dongzhiquan  Date: 2019/1/15 Time: 16:32
 */
public class ClientHandler extends ChannelInboundHandlerAdapter{

    private static final Logger LOGGER= LoggerFactory.getLogger(ClientHandler.class);

    private ClientConnect client;

    public ClientHandler(ClientConnect client) {
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

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        final EventLoop eventloop = ctx.channel().eventLoop();
        client.updateStatus(false);
        eventloop.schedule(()->{
            client.createBootstrap(new Bootstrap(),eventloop);
        },10, TimeUnit.SECONDS);
    }
}
