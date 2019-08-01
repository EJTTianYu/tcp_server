package com.thit.tibdm.net.SFSNet;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.thit.tibdm.BootstrapJob;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * 网络服务实现
 *
 * @author wanghaoqiang
 * @date 2018/4/12
 * @time 13:58
 */
public class SFSNetServiceImpl implements BootstrapJob, SFSNetRecive {

    private final Injector guice;

    private ServerBootstrap bootstrap;

    private final SFSConfiguration conf;

    @Inject
    public SFSNetServiceImpl(Injector guice,
                             SFSConfiguration conf) {
        this.guice = guice;
        this.conf = conf;
    }


    @Override
    public void start() {
//        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
//        EventLoopGroup workerGroup = new NioEventLoopGroup();
//        bootstrap = new ServerBootstrap();
//        bootstrap.group(bossGroup, workerGroup);
//        setOptions(bootstrap);
//        setChannel(bootstrap);
//        bootstrap.childHandler(new ChannelInitializer<Channel>() {
//            @Override
//            protected void initChannel(Channel ch) {
//                initPipeline(ch.pipeline());
//            }
//        });
//        // 绑定端口，同步等待成功
//        ChannelFuture f;
//        try {
//            f = bootstrap.bind(conf.getTcpPort()).sync();
//            // 等待服务端监听端口关闭
//            f.channel().closeFuture().sync();
//        } catch (InterruptedException e) {
//            LOGGER
//        }

    }

    @Override
    public void stop() {

    }


    @Override
    public void setOptions(ServerBootstrap bootstrap) {
        bootstrap.option(ChannelOption.SO_BACKLOG, 1024);
        bootstrap.option(ChannelOption.SO_SNDBUF, 32 * 1000);
        bootstrap.option(ChannelOption.SO_RCVBUF, 32 * 1000);
        bootstrap.option(ChannelOption.TCP_NODELAY, false);
    }

    @Override
    public void initPipeline(ChannelPipeline pipeline) {

    }

    @Override
    public void setChannel(ServerBootstrap bootstrap) {
        if (isLinux()) {
            bootstrap.channel(EpollServerSocketChannel.class);
        } else {
            bootstrap.channel(NioServerSocketChannel.class);
        }
    }

    /**
     * 判断是否是linux系统是的话则使用epoll
     *
     * @return
     */
    private static boolean isLinux() {
        return System.getProperty("os.name").toLowerCase().contains("linux");
    }
}
