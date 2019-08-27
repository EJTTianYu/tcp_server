package com.thit.tibdm.net.SFSNet;


import com.google.inject.Inject;
import com.google.inject.name.Named;


/**
 * @author wanghaoqiang
 * @date 2018/4/12
 * @time 13:32
 */
public class SFSConfiguration {
    private static final String TCP_PORT = "tcp.port";

    @Inject(optional = true)
    @Named(TCP_PORT)
    private int tcpPort = 8099;

    public int getTcpPort() {
        return tcpPort;
    }
}
