package com.thit.tibdm.net.SFSNet;

import com.thit.tibdm.server.TcpServer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelPipeline;

/**
 * @author wanghaoqiang
 * @date 2018/4/12
 * @time 14:16
 */
public interface SFSNetRecive {

    void setOptions(ServerBootstrap bootstrap);

    void initPipeline(ChannelPipeline pipeline);

    void setChannel(ServerBootstrap bootstrap);
}
