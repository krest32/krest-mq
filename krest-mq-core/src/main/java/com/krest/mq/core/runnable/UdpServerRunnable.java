package com.krest.mq.core.runnable;

import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.server.MQUDPServer;

import java.util.concurrent.Callable;

/**
 * 生成 udp Server
 */
public class UdpServerRunnable implements Runnable {
    Integer port;

    public UdpServerRunnable(Integer port) {
        this.port = port;
    }


    @Override
    public void run() {
        MQUDPServer mqudpServer = new MQUDPServer(this.port);
        mqudpServer.start();
        AdminServerCache.mqudpServer = mqudpServer;
    }
}
