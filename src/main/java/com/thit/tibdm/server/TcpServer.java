package com.thit.tibdm.server;


import com.thit.tibdm.Config;
import com.thit.tibdm.KafkaProducer;
import com.thit.tibdm.MetricMonitor;
import com.thit.tibdm.parse.ContentDecode;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 服务端程序
 */
public class TcpServer {
    /**
     * 日志类
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(TcpServer.class);
    /**
     * 获取端口号
     */
    private static final int PORT = Config.I.getTcpPort();

    /**
     * 主启动程序
     *
     * @param args
     */
    public static void main(String[] args) throws InterruptedException {
        ContentDecode i = ContentDecode.I;
        MetricMonitor monitor = MetricMonitor.I;
        //生成连接
        KafkaProducer kafka = KafkaProducer.I;
        LOGGER.info("服务监控端口:{}", PORT);
        LOGGER.info("kafka生产者池数量:{}", kafka.getSizeConnKafka());
        TcpServer tcpServer = new TcpServer();
        tcpServer.createServer(tcpServer);
    }

    /**
     * 创建服务端程序
     * @param tcpServer
     * @throws InterruptedException
     */
    private void createServer(TcpServer tcpServer) throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup);
        tcpServer.setChannel(b);
        tcpServer.setOptios(b);
        b.childHandler(new ChannelInitializer<Channel>() {
            @Override
            public void initChannel(Channel ch) {
                initPipeline(ch.pipeline());
            }
        });

        // 绑定端口，同步等待成功
        ChannelFuture f = b.bind(PORT).sync();

        // 等待服务端监听端口关闭
        f.channel().closeFuture().sync();
    }

    /**
     * 设置通道nio 或者 epoll
     *
     * @param bootstrap
     */
    private void setChannel(ServerBootstrap bootstrap) {
        bootstrap.channel(NioServerSocketChannel.class);
    }

    /**
     * 设置各种参数
     *
     * @param bootstrap
     */
    private void setOptios(ServerBootstrap bootstrap) {
    }

    /**
     * 设置解码，处理类等
     *
     * @param pipeline
     */
    private void initPipeline(ChannelPipeline pipeline) {
        ByteBuf buf = Unpooled.buffer();
        DelimiterBasedFrameDecoder endSign = new DelimiterBasedFrameDecoder(65535,
                false, buf.writeBytes(SFSResponse.getEND()));
        pipeline.addLast("split_end", endSign);
        pipeline.addLast("decoder", new SFSDecoder());
        pipeline.addLast("handler", new SFSServerChannelHandler());
    }
}
