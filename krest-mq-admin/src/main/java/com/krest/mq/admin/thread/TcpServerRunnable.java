package com.krest.mq.admin.thread;

import com.krest.mq.core.server.MQTCPServer;


/**
 * @author Administrator
 */
public class TcpServerRunnable implements Runnable {

    Integer port;

    public TcpServerRunnable(Integer port) {
        this.port = port;
    }


    @Override
    public void run() {
        MQTCPServer mqtcpServer = new MQTCPServer(this.port);
        mqtcpServer.start();
    }
}
