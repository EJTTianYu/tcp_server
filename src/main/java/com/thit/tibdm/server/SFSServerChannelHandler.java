package com.thit.tibdm.server;

import com.thit.tibdm.MetricMonitor;
import com.thit.tibdm.ThreadPoolManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.thit.tibdm.server.SFSResponse.getHbResponse;
import static com.thit.tibdm.server.SFSResponse.getStatusResponse;

/**
 * 数据处理类
 * <p>
 * 主要负责接收数据发送数据
 */
@ChannelHandler.Sharable
public class SFSServerChannelHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(SFSServerChannelHandler.class);

    private static final String STATUS_REQUEST = "status.request";

    /**
     * 收到数据的处理
     *
     * @param ctx
     * @param msg
     * @throws Exception
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        SFSPacket packet = (SFSPacket) msg;
        switch (packet.getCmd()) {
            case 1:
                MetricMonitor.I.registryMeter(STATUS_REQUEST).mark();
                /**
                 * 处理状态数据
                 */
                ThreadPoolManager.I.getStatusThread().execute(() -> {
                    StatusHandler handler = new StatusHandler();
                    handler.handler(packet);
                });
                /**
                 * 状态数据包
                 * 1.发送数据包到kafka中方便解析
                 * 2.返回数据给客户端
                 */
                ByteBuf status = getStatusResponse(packet);
                LOGGER.debug("回应状态包，状态包长度：{}", status.readableBytes());
                ctx.writeAndFlush(status);
                break;
            case 3:
                /**
                 * 心跳数据包
                 * 收到什么包就返回什么数据包
                 */
                ByteBuf hb = getHbResponse(packet);
                LOGGER.debug("回应心跳包，心跳包长度：{}", hb.readableBytes());
                ctx.writeAndFlush(hb);
                break;
            default:
                LOGGER.info("没有此种数据类型！");
        }
    }

    /**
     * 异常情况的处理
     *
     * @param ctx
     * @param cause
     * @throws Exception
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
        LOGGER.error("发生异常情况，ip为：{}，错误信息{}", ctx.channel().remoteAddress(), cause);
    }

    /**
     * 第一次连接上的时候的处理
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        LOGGER.info("客户端连接上，ip为：{}", ctx.channel().remoteAddress());
    }

    /**
     * 断开的时候的处理
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        LOGGER.info("客户端断开，ip为：{}", ctx.channel().remoteAddress());
    }
}
