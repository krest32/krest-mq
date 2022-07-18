package com.krest.mq.admin;

import com.krest.mq.core.server.MQServer;
import com.krest.mq.core.server.MQServerFactory;

public class TCPServer2 {
    public static void main(String[] args) {
        MQServerFactory serverFactory = new MQServerFactory();
        MQServer mqServer = serverFactory.getMqServer();
        mqServer.start();
    }
}
