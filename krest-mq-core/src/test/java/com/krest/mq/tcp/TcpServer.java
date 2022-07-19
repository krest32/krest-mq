package com.krest.mq.tcp;

import com.krest.mq.core.entity.ConnType;
import com.krest.mq.core.server.MQServer;
import com.krest.mq.core.server.MQServerFactory;

public class TcpServer {
    public static void main(String[] args) {
        MQServerFactory serverFactory = new MQServerFactory(ConnType.TCP, 9001);
        MQServer mqServer = serverFactory.getMqServer();
        mqServer.start();
    }
}
