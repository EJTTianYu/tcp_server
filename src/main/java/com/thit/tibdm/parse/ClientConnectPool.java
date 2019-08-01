package com.thit.tibdm.parse;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: dongzhiquan  Date: 2019/1/17 Time: 16:10
 */
public enum ClientConnectPool {

    I;

    private Map<String ,ClientConnect> clientConnectPool;

    ClientConnectPool(){
        clientConnectPool=new ConcurrentHashMap<>();
    }

    public ClientConnect getConnect(String ch){
        if (clientConnectPool.containsKey(ch)){
            return clientConnectPool.get(ch);
        }else {
            ClientConnect connect=new ClientConnect();
            clientConnectPool.put(ch,connect);
            return connect;
        }
    }
}
