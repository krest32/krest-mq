package com.krest.mq.admin;

import com.krest.mq.core.server.MQTCPServer;

public class MqStart {
    public static void main(String[] args) {
        MQTCPServer mqServer = new MQTCPServer(9001,true);
        mqServer.start();
    }
}
