package com.krest.mq.admin.thread;

import com.krest.mq.core.server.MQTCPServer;
import org.springframework.stereotype.Component;

public class TCPServerRunnable implements Runnable {

    Integer port;

    public TCPServerRunnable(Integer port) {
        this.port = port;
    }


    @Override
    public void run() {
        MQTCPServer mqtcpServer = new MQTCPServer(this.port);
        mqtcpServer.start();
    }
}
