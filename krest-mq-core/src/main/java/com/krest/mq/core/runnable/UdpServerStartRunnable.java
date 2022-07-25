package com.krest.mq.core.runnable;

import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.server.MQUDPServer;

import java.util.concurrent.Callable;

/**
 * 生成 udp Server
 */
public class UdpServerStartRunnable implements Runnable {
    Integer port;

    public UdpServerStartRunnable(Integer port) {
        this.port = port;
    }

    @Override
    public void run() {
        MQUDPServer mqudpServer = new MQUDPServer(this.port);
        AdminServerCache.mqudpServer = mqudpServer;
        AdminServerCache.mqudpServer.start();
    }
}
