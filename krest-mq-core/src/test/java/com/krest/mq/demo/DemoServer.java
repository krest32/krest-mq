package com.krest.mq.demo;

import com.krest.mq.core.server.MqServer;

public class DemoServer {
    public static void main(String[] args) {
        MqServer mqServer = new MqServer(8001, true);
        mqServer.start();
    }
}
