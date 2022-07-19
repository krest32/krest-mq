package com.krest.mq.core.runnable;

import com.krest.mq.core.server.MQUDPServer;

import java.util.concurrent.Callable;

public class UdpServerRunnable implements Callable {
    Integer port;

    public UdpServerRunnable(Integer port) {
        this.port = port;
    }

    @Override
    public MQUDPServer call() throws Exception {
        MQUDPServer mqudpServer = new MQUDPServer(this.port);
        mqudpServer.start();
        return mqudpServer;
    }
}
